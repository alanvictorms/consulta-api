import csv
import hashlib
import io
import json
import os
import queue
import secrets
import sqlite3
import threading
import time
import uuid
from datetime import datetime, date
from functools import wraps

import requests as http_requests
from flask import (
    Flask, render_template, request, jsonify, send_file, Response,
    session, redirect, url_for, flash,
)
from flask_sqlalchemy import SQLAlchemy
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
app.secret_key = os.environ.get("SECRET_KEY", "fallback-dev-key-change-in-prod")

db_path = os.environ.get("DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "instance", "dataflow.db"))
os.makedirs(os.path.dirname(db_path), exist_ok=True)
app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{db_path}"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["UPLOAD_FOLDER"] = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads")
os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)

db = SQLAlchemy(app)

ESTADOS = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG",
    "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO",
]


@app.after_request
def add_cors_headers(response):
    if request.path.startswith("/api/v1/") or request.path.startswith("/api/keys/"):
        response.headers["Access-Control-Allow-Origin"] = request.headers.get("Origin", "*")
        response.headers["Access-Control-Allow-Headers"] = "Content-Type, X-API-Key, Authorization, Accept"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    return response


# ── Models ──────────────────────────────────────────────────────────

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(200), default="")
    password_hash = db.Column(db.String(128), nullable=False)
    role = db.Column(db.String(20), nullable=False, default="user")  # admin, user, api_user, consulta_api
    plan_id = db.Column(db.Integer, db.ForeignKey("plan.id"), nullable=True)
    credits_used_today = db.Column(db.Integer, default=0)
    credits_used_total = db.Column(db.Integer, default=0)
    last_credit_reset = db.Column(db.Date, default=date.today)
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    plan = db.relationship("Plan", backref="users")


class Plan(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    plan_type = db.Column(db.String(30), default="consulta")  # consulta, api, consulta_api
    daily_limit = db.Column(db.Integer, default=100)
    monthly_limit = db.Column(db.Integer, default=3000)
    modules = db.Column(db.Text, default="[]")  # JSON list of allowed module ids
    price = db.Column(db.Float, default=0)
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def get_modules(self):
        try:
            mods = json.loads(self.modules or "[]")
            if ("df_cnpj" in mods or "cb_cnpj" in mods) and "cnpj_titulares" not in mods:
                mods.append("cnpj_titulares")
            return mods
        except Exception:
            return []


class UserApiKey(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    key = db.Column(db.String(64), unique=True, nullable=False)
    label = db.Column(db.String(100), default="")
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_used = db.Column(db.DateTime)
    total_requests = db.Column(db.Integer, default=0)

    user = db.relationship("User", backref="api_keys")


class Provider(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    slug = db.Column(db.String(50), unique=True, nullable=False)  # dataflow, cpfbusca
    base_url = db.Column(db.String(300), nullable=False)
    auth_type = db.Column(db.String(20), default="header")
    auth_header = db.Column(db.String(50), default="x-api-key")
    auth_value = db.Column(db.String(300), default="")
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_used = db.Column(db.DateTime)
    total_queries = db.Column(db.Integer, default=0)


class QueryLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    endpoint = db.Column(db.String(50))
    module = db.Column(db.String(50))
    query_param = db.Column(db.String(300))
    provider_slug = db.Column(db.String(50))
    result_count = db.Column(db.Integer, default=0)
    status_code = db.Column(db.Integer)
    response_time_ms = db.Column(db.Integer)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class QueryResponse(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    log_id = db.Column(db.Integer, db.ForeignKey("query_log.id"), unique=True, index=True)
    payload = db.Column(db.Text, nullable=False)


class BatchJob(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    batch_type = db.Column(db.String(30), default="nome4_cpf")
    file_path = db.Column(db.String(500))
    started_at = db.Column(db.DateTime, default=datetime.utcnow)
    finished_at = db.Column(db.DateTime)
    total = db.Column(db.Integer, default=0)
    done = db.Column(db.Integer, default=0)
    found = db.Column(db.Integer, default=0)
    errors = db.Column(db.Integer, default=0)
    threads = db.Column(db.Integer, default=3)
    rpm = db.Column(db.Integer, default=20)
    start_from = db.Column(db.Integer, default=1)
    end_at = db.Column(db.Integer, default=0)
    items_json = db.Column(db.Text, default="[]")
    status = db.Column(db.String(20), default="running")  # running, paused, stopped, done


class BatchResult(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    job_id = db.Column(db.Integer, db.ForeignKey("batch_job.id"), index=True)
    row_index = db.Column(db.Integer, default=0)
    registro = db.Column(db.String(100))
    nome_busca = db.Column(db.String(300))
    cidade_busca = db.Column(db.String(120), default="")
    uf_busca = db.Column(db.String(5))
    qtd_resultados = db.Column(db.Integer, default=0)
    cpf = db.Column(db.String(20))
    nome_encontrado = db.Column(db.String(300))
    data_nascimento = db.Column(db.String(20))
    nome_mae = db.Column(db.String(300))
    sexo = db.Column(db.String(20))
    cidade = db.Column(db.String(100))
    uf_result = db.Column(db.String(5))
    cep = db.Column(db.String(15))
    telefones = db.Column(db.Text, default="")
    status = db.Column(db.String(200))
    tempo_ms = db.Column(db.Integer, default=0)
    raw_data = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


# ── Unified Module Registry ─────────────────────────────────────────
# Removidos df_cnpj e cb_cnpj conforme solicitado; mantido CNPJ + Titular (cnpj_titulares).

PROVIDER_LABELS = {
    "cpfbusca": "CPF Busca",
    "dataflow": "DataFlow+",
    "minhareceita": "Receita Federal + CPF Busca (Compliance)",
}

MODULES = [
    # Receita Federal + CPFBusca Compliance (CNPJ + Titular)
    {"id": "cnpj_titulares", "label": "CNPJ + Titular", "category": "Empresas", "provider": "minhareceita",
     "provider_label": "Receita Federal + CPF Busca", "icon": "building", "verified": True,
     "path": "/cnpj-titulares", "method": "POST", "params": [{"name": "cnpj", "required": True}],
     "description": "Consulta cadastral de CNPJ na Receita Federal e enriquece automaticamente os titulares/sócios (QSA) via Nome + CPF parcial (nome3)."},

    # CPFBusca modules — Consultas básicas
    {"id": "cb_cpf", "label": "Consulta CPF", "category": "Documentos", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "file", "verified": True,
     "path": "/cpf", "method": "GET", "params": [{"name": "cpf", "required": True}],
     "description": "Consulta completa por CPF. Retorna cadastro, endereços, telefones, emails, score, renda, veículos e vínculos."},
    {"id": "cb_nome", "label": "Busca por Nome", "category": "Busca", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "search", "verified": True,
     "path": "/nome", "method": "GET", "params": [{"name": "nome", "required": True}, {"name": "uf", "required": False}],
     "description": "Busca pessoas pelo nome completo ou parcial com filtro opcional de UF."},
    {"id": "cb_telefone", "label": "Consulta Telefone", "category": "Contato", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "phone", "verified": True,
     "path": "/telefone", "method": "GET", "params": [{"name": "telefone", "required": True}],
     "description": "Busca dados cadastrais do titular e vínculos por número de telefone com DDD."},
    # Documentos
    {"id": "cb_rg", "label": "Consulta RG", "category": "Documentos", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "id", "verified": True,
     "path": "/rg", "method": "GET", "params": [{"name": "rg", "required": True}],
     "description": "Busca pessoa por número de documento RG."},
    {"id": "cb_titulo", "label": "Título de Eleitor", "category": "Documentos", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "file", "verified": True,
     "path": "/titulo", "method": "GET", "params": [{"name": "titulo", "required": True}],
     "description": "Busca pessoa por número do título de eleitor."},
    {"id": "cb_pis", "label": "Consulta PIS", "category": "Documentos", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "file", "verified": True,
     "path": "/pis", "method": "GET", "params": [{"name": "pis", "required": True}],
     "description": "Busca pessoa por número PIS/PASEP."},
    {"id": "cb_nis", "label": "Consulta NIS", "category": "Documentos", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "file", "verified": True,
     "path": "/nis", "method": "GET", "params": [{"name": "nis", "required": True}],
     "description": "Busca pessoa por número NIS."},
    {"id": "cb_cpf_nascimento", "label": "CPF + Nascimento", "category": "Documentos", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "calendar", "verified": True,
     "path": "/cpf-data-nascimento", "method": "GET", "params": [{"name": "cpf", "required": True}, {"name": "data_nascimento", "required": True}],
     "description": "Valida e consulta CPF combinado com data de nascimento."},
    # Relações
    {"id": "cb_parentes", "label": "Parentes", "category": "Relações", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "users", "verified": True,
     "path": "/parentes", "method": "GET", "params": [{"name": "cpf", "required": True}],
     "description": "Lista parentes e familiares vinculados ao CPF."},
    {"id": "cb_vizinhos", "label": "Vizinhos", "category": "Relações", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "users", "verified": True,
     "path": "/vizinhos", "method": "GET", "params": [{"name": "cpf", "required": True}],
     "description": "Lista vizinhos associados ao endereço/CEP do CPF."},
    # Localização
    {"id": "cb_cep", "label": "Consulta CEP", "category": "Localização", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "pin", "verified": True,
     "path": "/cep", "method": "GET", "params": [{"name": "cep", "required": True}],
     "description": "Busca endereço completo e registros por CEP."},
    {"id": "cb_cep2", "label": "CEP + Emails", "category": "Localização", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "mail", "verified": True,
     "path": "/cep2", "method": "GET", "params": [{"name": "cep", "required": True}],
     "description": "Busca moradores e e-mails vinculados ao CEP."},
    {"id": "cb_cep3", "label": "CEP + Telefones", "category": "Localização", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "phone", "verified": True,
     "path": "/cep3", "method": "GET", "params": [{"name": "cep", "required": True}],
     "description": "Busca moradores e telefones vinculados ao CEP."},
    {"id": "cb_cep4", "label": "CEP + Nome", "category": "Localização", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "search", "verified": True,
     "path": "/cep4", "method": "GET", "params": [{"name": "cep", "required": True}, {"name": "nome", "required": False}, {"name": "limit", "required": False}],
     "description": "Busca moradores por CEP com filtro de nome."},
    {"id": "cb_endereco", "label": "Busca por Endereço", "category": "Localização", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "pin", "verified": True,
     "path": "/endereco", "method": "GET", "params": [{"name": "uf", "required": True}, {"name": "logradouro", "required": True}, {"name": "cidade", "required": False}, {"name": "limit", "required": False}],
     "description": "Busca pessoas por endereço (UF + logradouro + cidade opcional)."},
    {"id": "cb_estado", "label": "Consulta por Estado", "category": "Localização", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "pin", "verified": True,
     "path": "/estado", "method": "GET", "params": [{"name": "uf", "required": True}],
     "description": "Consulta estatísticas e dados por estado (UF)."},
    {"id": "cb_geo", "label": "Geolocalização", "category": "Localização", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "pin", "verified": True,
     "path": "/geo", "method": "GET", "params": [{"name": "cep", "required": True}, {"name": "limit", "required": False}],
     "description": "Busca geolocalização e moradores por CEP."},
    # Busca por nome (variantes)
    {"id": "cb_nome2", "label": "Nome Base 2", "category": "Busca", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "search", "verified": True,
     "path": "/nome2", "method": "GET", "params": [{"name": "nome", "required": True}],
     "description": "Busca por nome em base alternativa."},
    {"id": "cb_nome3", "label": "Nome + CPF Parcial", "category": "Busca", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "search", "verified": True,
     "path": "/nome3", "method": "GET", "params": [{"name": "nome", "required": True}, {"name": "cpf_meio", "required": False}, {"name": "cpf", "required": False}, {"name": "limit", "required": False}],
     "description": "Busca por nome com CPF parcial (6 dígitos do meio em cpf_meio) e retorna dados pessoais e empresas vinculadas."},
    {"id": "cb_nome4", "label": "Nome + Cidade", "category": "Busca", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "search", "verified": True,
     "path": "/nome4", "method": "GET", "params": [{"name": "nome", "required": True}, {"name": "cidade", "required": True}, {"name": "limit", "required": False}],
     "description": "Busca por nome completo filtrado por cidade."},
    {"id": "cb_nome5", "label": "Nome + Ano", "category": "Busca", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "search", "verified": True,
     "path": "/nome5", "method": "GET", "params": [{"name": "nome", "required": True}, {"name": "ano", "required": True}, {"name": "limit", "required": False}],
     "description": "Busca por nome filtrado por ano de nascimento."},
    # Contato
    {"id": "cb_email", "label": "Busca por Email", "category": "Contato", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "mail", "verified": True,
     "path": "/email", "method": "GET", "params": [{"name": "email", "required": True}],
     "description": "Busca pessoa vinculada a um endereço de e-mail."},
    {"id": "cb_telefone_ddd", "label": "Telefones por DDD", "category": "Contato", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "phone", "verified": True,
     "path": "/telefone_ddd", "method": "GET", "params": [{"name": "ddd", "required": True}],
     "description": "Lista telefones por código DDD."},
    {"id": "cb_telefone_full", "label": "Telefone Completo", "category": "Contato", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "phone", "verified": True,
     "path": "/telefone_full", "method": "GET", "params": [{"name": "phone", "required": True}],
     "description": "Consulta detalhada de telefone com operadora e titular."},
    {"id": "cb_telefone_cpf", "label": "Telefones por CPF", "category": "Contato", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "phone", "verified": True,
     "path": "/telefone_cpf", "method": "GET", "params": [{"name": "cpf", "required": True}],
     "description": "Lista todos os telefones vinculados a um CPF."},
    {"id": "cb_operadora", "label": "Operadora", "category": "Contato", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "phone", "verified": True,
     "path": "/operadora", "method": "GET", "params": [{"name": "telefone", "required": True}],
     "description": "Identifica a operadora de um número de telefone."},
    # Financeiro
    {"id": "cb_score", "label": "Score", "category": "Financeiro", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "wallet", "verified": True,
     "path": "/score", "method": "GET", "params": [{"name": "cpf", "required": True}],
     "description": "Score de crédito e classificação de risco por CPF."},
    {"id": "cb_renda", "label": "Renda", "category": "Financeiro", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "wallet", "verified": True,
     "path": "/renda", "method": "GET", "params": [{"name": "min", "required": False}, {"name": "max", "required": False}, {"name": "limit", "required": False}],
     "description": "Consulta renda estimada com filtros de faixa."},
    {"id": "cb_cbo", "label": "Consulta CBO", "category": "Financeiro", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "wallet", "verified": True,
     "path": "/cbo", "method": "GET", "params": [{"name": "cbo", "required": True}],
     "description": "Busca profissionais por código CBO."},
    {"id": "cb_profissao", "label": "Profissão", "category": "Financeiro", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "wallet", "verified": True,
     "path": "/profissao", "method": "GET", "params": [{"name": "profissao", "required": True}],
     "description": "Busca pessoas por profissão."},
    {"id": "cb_bin", "label": "Consulta BIN", "category": "Financeiro", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "wallet", "verified": True,
     "path": "/bin", "method": "GET", "params": [{"name": "bin", "required": True}],
     "description": "Consulta dados de cartão por BIN (6 primeiros dígitos)."},
    {"id": "cb_banco", "label": "Consulta Banco", "category": "Financeiro", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "building", "verified": True,
     "path": "/banco", "method": "GET", "params": [{"name": "codigo", "required": True}],
     "description": "Consulta dados de instituição bancária por código."},
    {"id": "cb_irpf", "label": "Situação IRPF", "category": "Financeiro", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "wallet", "verified": True,
     "path": "/irpf", "method": "GET", "params": [{"name": "cpf", "required": True}],
     "description": "Consulta situação do CPF na Receita Federal / IRPF."},
    # Fotos
    {"id": "cb_foto", "label": "Foto Geral", "category": "Fotos", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "user", "verified": True,
     "path": "/foto", "method": "GET", "params": [{"name": "cpf", "required": True}],
     "description": "Retorna foto vinculada ao CPF."},
    {"id": "cb_foto_sp", "label": "Foto SP", "category": "Fotos", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "user", "verified": True,
     "path": "/foto_sp", "method": "GET", "params": [{"name": "cpf", "required": True}],
     "description": "Foto da base de São Paulo."},
    {"id": "cb_foto_ma", "label": "Foto MA", "category": "Fotos", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "user", "verified": True,
     "path": "/foto_ma", "method": "GET", "params": [{"name": "cpf", "required": True}],
     "description": "Foto da base do Maranhão."},
    {"id": "cb_foto_ro", "label": "Foto RO", "category": "Fotos", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "user", "verified": True,
     "path": "/foto_ro", "method": "GET", "params": [{"name": "cpf", "required": True}],
     "description": "Foto da base de Rondônia."},
    {"id": "cb_foto_all", "label": "Foto Todas Bases", "category": "Fotos", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "user", "verified": True,
     "path": "/foto_all", "method": "GET", "params": [{"name": "cpf", "required": True}],
     "description": "Busca foto em todas as bases disponíveis."},
    # Veículos e outros
    {"id": "cb_placa", "label": "Consulta Placa", "category": "Veículos", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "car", "verified": True,
     "path": "/placa", "method": "GET", "params": [{"name": "placa", "required": True}],
     "description": "Consulta veículo por placa. Retorna marca, modelo, ano e proprietário."},
    {"id": "cb_veiculos_jbr", "label": "Veículos JBR", "category": "Veículos", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "car", "verified": True,
     "path": "/veiculos_jbr", "method": "GET", "params": [{"name": "placa", "required": False}, {"name": "chassi", "required": False}],
     "description": "Consulta veículo por placa ou chassi (base JBR)."},
    {"id": "cb_random", "label": "Pessoa Aleatória", "category": "Outros", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "spark", "verified": True,
     "path": "/random", "method": "GET", "params": [{"name": "uf", "required": False}],
     "description": "Retorna dados de uma pessoa aleatória."},
    {"id": "cb_profissionais", "label": "Profissionais", "category": "Profissional", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "user", "verified": True,
     "path": "/profissionais", "method": "GET", "params": [{"name": "nome", "required": False}, {"name": "email", "required": False}, {"name": "profissao", "required": False}, {"name": "limit", "required": False}],
     "description": "Busca profissionais por nome, e-mail ou profissão."},
    {"id": "cb_socios", "label": "Sócios por CPF", "category": "Empresas", "provider": "cpfbusca",
     "provider_label": "CPF Busca", "icon": "building", "verified": True,
     "path": "/socios", "method": "GET", "params": [{"name": "cpf", "required": True}],
     "description": "Lista empresas e participações societárias vinculadas ao CPF."},

    # DataFlow+ modules
    {"id": "df_nome", "label": "Busca por Nome (DF)", "category": "Busca", "provider": "dataflow",
     "provider_label": "DataFlow+", "icon": "search", "verified": True,
     "path": "/nome", "method": "GET", "params": [{"name": "nome", "required": True}, {"name": "estado", "required": False}],
     "description": "Busca pessoas pelo nome completo ou parcial na base DataFlow+."},
    {"id": "df_cpf", "label": "Consulta CPF (DF)", "category": "Documentos", "provider": "dataflow",
     "provider_label": "DataFlow+", "icon": "file", "verified": True,
     "path": "/cpf", "method": "GET", "params": [{"name": "cpf", "required": True}],
     "description": "Consulta completa por CPF na base DataFlow+."},
    {"id": "df_documentos", "label": "Documentos Multi (DF)", "category": "Documentos", "provider": "dataflow",
     "provider_label": "DataFlow+", "icon": "id", "verified": True,
     "path": "/documentos", "method": "GET", "params": [{"name": "cpf", "required": False}, {"name": "telefone", "required": False}, {"name": "email", "required": False}],
     "description": "Busca por documento (CPF, telefone ou email) com detecção automática."},
    {"id": "df_telefone", "label": "Busca por Telefone (DF)", "category": "Contato", "provider": "dataflow",
     "provider_label": "DataFlow+", "icon": "phone", "verified": True,
     "path": "/telefone", "method": "GET", "params": [{"name": "telefone", "required": True}],
     "description": "Busca proprietário de número de telefone na base DataFlow+."},
]

for idx, m in enumerate(MODULES):
    m["idx"] = idx

MODULES_BY_ID = {m["id"]: m for m in MODULES}
MODULE_CATEGORIES = sorted(set(m["category"] for m in MODULES))


# ── DB Init & Safe Migrations ───────────────────────────────────────

def _run_safe_migrations():
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(batch_job)")
        bj_cols = {row[1] for row in cur.fetchall()}
        if "start_from" not in bj_cols:
            cur.execute("ALTER TABLE batch_job ADD COLUMN start_from INTEGER DEFAULT 1")
        if "end_at" not in bj_cols:
            cur.execute("ALTER TABLE batch_job ADD COLUMN end_at INTEGER DEFAULT 0")
        if "items_json" not in bj_cols:
            cur.execute("ALTER TABLE batch_job ADD COLUMN items_json TEXT DEFAULT '[]'")

        cur.execute("PRAGMA table_info(batch_result)")
        br_cols = {row[1] for row in cur.fetchall()}
        if "telefones" not in br_cols:
            cur.execute("ALTER TABLE batch_result ADD COLUMN telefones TEXT DEFAULT ''")
        if "cidade_busca" not in br_cols:
            cur.execute("ALTER TABLE batch_result ADD COLUMN cidade_busca VARCHAR(120) DEFAULT ''")
        conn.commit()
        conn.close()
    except Exception:
        pass


with app.app_context():
    db.create_all()
    _run_safe_migrations()
    all_mod_ids = [m["id"] for m in MODULES]
    if not User.query.filter_by(username="admin").first():
        admin_plan = Plan(
            name="Admin Ilimitado", plan_type="consulta_api",
            daily_limit=999999, monthly_limit=999999,
            modules=json.dumps(all_mod_ids),
        )
        db.session.add(admin_plan)
        db.session.flush()
        db.session.add(User(
            username="admin",
            email="admin@sistema.local",
            password_hash=hashlib.sha256("admin".encode()).hexdigest(),
            role="admin",
            plan_id=admin_plan.id,
        ))
        db.session.commit()
    else:
        for p in Plan.query.all():
            mods = p.get_modules()
            changed = False
            if p.plan_type == "admin" or p.name == "Admin Ilimitado":
                for mid in all_mod_ids:
                    if mid not in mods:
                        mods.append(mid)
                        changed = True
            else:
                if "cnpj_titulares" not in mods:
                    mods.append("cnpj_titulares")
                    changed = True
                for old_id in ("df_cnpj", "cb_cnpj"):
                    if old_id in mods:
                        mods.remove(old_id)
                        changed = True
            if changed:
                p.modules = json.dumps(mods)
        db.session.commit()


# ── Helpers ─────────────────────────────────────────────────────────

def hash_pw(pw):
    return hashlib.sha256(pw.encode()).hexdigest()


def get_user():
    uid = session.get("user_id")
    if not uid:
        return None
    u = db.session.get(User, uid)
    if u and u.last_credit_reset != date.today():
        u.credits_used_today = 0
        u.last_credit_reset = date.today()
        db.session.commit()
    return u


def user_has_consulta(user):
    if not user:
        return False
    if user.role in ("admin", "user", "consulta_api"):
        return True
    if user.plan and user.plan.plan_type in ("consulta", "consulta_api", "ambos", "admin"):
        return True
    return True


def user_has_api(user):
    if not user:
        return False
    if user.role in ("admin", "api_user", "consulta_api"):
        return True
    if user.plan and user.plan.plan_type in ("api", "consulta_api", "ambos", "admin"):
        return True
    return False


@app.context_processor
def inject_access_flags():
    u = get_user()
    return {
        "has_consulta": user_has_consulta(u),
        "has_api": user_has_api(u),
        "provider_labels": PROVIDER_LABELS,
    }


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        u = get_user()
        if not u:
            if request.is_json or request.path.startswith("/api/"):
                return jsonify({"error": "Não autenticado"}), 401
            return redirect(url_for("login_page"))
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        u = get_user()
        orig_id = session.get("admin_user_id")
        orig = db.session.get(User, orig_id) if orig_id else None
        is_admin = (u and u.role == "admin") or (orig and orig.role == "admin" and orig.is_active)
        if not is_admin:
            if request.is_json or request.path.startswith("/api/"):
                return jsonify({"error": "Acesso negado — exclusivo para administrador"}), 403
            return redirect(url_for("dashboard"))
        return f(*args, **kwargs)
    return decorated


def debit_credit(user):
    if user.role == "admin":
        return True
    if not user.plan:
        return False
    if user.credits_used_today >= user.plan.daily_limit:
        return False
    user.credits_used_today += 1
    user.credits_used_total += 1
    db.session.commit()
    return True


def get_active_provider_slugs():
    return {"cpfbusca", "dataflow", "minhareceita"}


def get_user_modules(user):
    if not user or user.role == "admin":
        return list(MODULES)
    allowed = set(user.plan.get_modules() if user.plan else [])
    if not allowed:
        return list(MODULES)
    allowed.add("cnpj_titulares")
    matched = [m for m in MODULES if m["id"] in allowed]
    return matched if matched else list(MODULES)


def get_provider(slug):
    p = Provider.query.filter_by(slug=slug, is_active=True).first()
    if p:
        return p
    all_provs = Provider.query.filter_by(is_active=True).all() or Provider.query.all()
    if slug == "cpfbusca":
        for cand in all_provs:
            s_low = (cand.slug or "").lower()
            n_low = (cand.name or "").lower()
            b_low = (cand.base_url or "").lower()
            a_low = (cand.auth_value or "").strip().lower()
            if "cpf" in s_low or "busca" in s_low or "cpf" in n_low or "busca" in n_low or "cpfbusca" in b_low or a_low.startswith("mb_") or "mb_live_" in a_low:
                return cand
    if slug == "dataflow":
        for cand in all_provs:
            s_low = (cand.slug or "").lower()
            n_low = (cand.name or "").lower()
            b_low = (cand.base_url or "").lower()
            if "data" in s_low or "flow" in s_low or s_low == "df" or "data" in n_low or "flow" in n_low or "dataflow" in b_low:
                return cand
    return all_provs[0] if all_provs else None


def build_provider_request_config(provider, path):
    base = (provider.base_url or "").strip().rstrip("/")
    s_low = (provider.slug or "").lower()
    n_low = (provider.name or "").lower()
    a_low = (provider.auth_value or "").strip().lower()
    is_cb = (
        s_low == "cpfbusca"
        or "cpfbusca" in base.lower()
        or "cpf" in s_low
        or "busca" in s_low
        or "cpf" in n_low
        or "busca" in n_low
        or a_low.startswith("mb_")
        or "mb_live_" in a_low
    )
    if is_cb and "cpfbuscas.com" in base.lower():
        if base.lower().endswith("/api/v1"):
            base = base + "/query"
        elif "/api/v1/query" not in base.lower():
            base = base.split("/api/")[0] + "/api/v1/query"
    url = f"{base}{path}"
    auth_val = (provider.auth_value or "").strip()
    if is_cb:
        if auth_val.lower().startswith("bearer "):
            bearer = auth_val
        else:
            bearer = f"Bearer {auth_val}"
        headers = {"Authorization": bearer, "Accept": "application/json"}
    else:
        h_name = (provider.auth_header or "x-api-key").strip()
        if h_name.lower() == "authorization" and not auth_val.lower().startswith("bearer "):
            auth_val = f"Bearer {auth_val}"
        headers = {h_name: auth_val, "Accept": "application/json"}
    return url, headers


def provider_request(provider, path, params):
    url, headers = build_provider_request_config(provider, path)
    t0 = time.monotonic()
    resp = http_requests.get(url, params=params, headers=headers, timeout=25)
    ms = int((time.monotonic() - t0) * 1000)
    try:
        provider.last_used = datetime.utcnow()
        provider.total_queries = (provider.total_queries or 0) + 1
        db.session.commit()
    except Exception:
        db.session.rollback()
    return resp, ms


def flatten_single_record(data):
    flat = {}
    lists = {}
    if not isinstance(data, dict):
        return flat, lists
    for key, val in data.items():
        if isinstance(val, dict):
            for k2, v2 in val.items():
                if v2 is not None and not isinstance(v2, (dict, list)):
                    flat[f"{key}_{k2}" if k2 in flat else k2] = v2
        elif isinstance(val, list):
            lists[key] = val
        elif val is not None:
            flat[key] = val
    return flat, lists


def detect_doc_param(valor):
    v = valor.strip().replace(".", "").replace("-", "").replace("/", "")
    if v.isdigit():
        if len(v) == 11:
            return "cpf", v
        if len(v) == 14:
            return "cnpj", v
        if 10 <= len(v) <= 11:
            return "telefone", v
    if "@" in valor:
        return "email", valor.strip()
    return "cpf", v


def api_key_auth():
    key = (
        request.headers.get("X-API-Key")
        or request.headers.get("x-api-key")
        or request.headers.get("Authorization", "").replace("Bearer ", "").replace("bearer ", "")
    ).strip()
    if not key:
        data = request.get_json(silent=True) or {}
        key = (data.get("api_key") or data.get("x_api_key") or "").strip()
    if not key:
        return None
    ak = UserApiKey.query.filter_by(key=key, is_active=True).first()
    if ak and ak.user and ak.user.is_active:
        ak.last_used = datetime.utcnow()
        ak.total_requests = (ak.total_requests or 0) + 1
        db.session.commit()
        return ak.user
    return None


def extract_phones_from_cpf_body(body):
    """Extract and deduplicate phone numbers from CPFBusca /cpf response body."""
    if not isinstance(body, dict):
        return []
    seen = set()
    phones = []

    def add_phone(p):
        if p is None:
            return
        s = str(p).strip()
        if not s or s.lower() in ("null", "none", "[]"):
            return
        digits = "".join(c for c in s if c.isdigit())
        if len(digits) < 8:
            return
        if digits not in seen:
            seen.add(digits)
            phones.append(digits)

    for p in (body.get("phones") or []):
        if isinstance(p, dict):
            add_phone(p.get("telefone") or p.get("phone") or p.get("numero"))
        else:
            add_phone(p)

    for p in (body.get("telefones") or []):
        if isinstance(p, dict):
            ddd = str(p.get("ddd") or "").strip()
            num = str(p.get("numero") or p.get("telefone") or p.get("TELEFONE") or "").strip()
            add_phone(f"{ddd}{num}" if ddd and not num.startswith(ddd) else num)
        else:
            add_phone(p)

    for key in ("telefones_assecc", "telefones_operadoras", "historico_telefones"):
        for item in (body.get(key) or []):
            if isinstance(item, dict):
                ddd = str(item.get("ddd") or "").strip()
                num = str(item.get("telefone") or item.get("numero") or "").strip()
                add_phone(f"{ddd}{num}" if ddd and not num.startswith(ddd) else num)
            else:
                add_phone(item)

    serasa_tels = (body.get("serasa_completo") or {}).get("telefones") or []
    for item in serasa_tels:
        if isinstance(item, dict):
            ddd = str(item.get("ddd") or "").strip()
            num = str(item.get("TELEFONE") or item.get("telefone") or "").strip()
            add_phone(f"{ddd}{num}" if ddd and not num.startswith(ddd) else num)

    for p in (body.get("datasus_phones") or []):
        add_phone(p)

    return phones


def execute_cnpj_titulares_lookup(cnpj_raw):
    """Query https://minhareceita.org/<CNPJ>, enrich each QSA partner via CPFBusca /nome3,
    and then enrich further via CPFBusca /cpf using the full CPF obtained in the 1st enrichment.
    """
    cnpj_val = "".join(c for c in str(cnpj_raw or "") if c.isdigit())
    if len(cnpj_val) != 14:
        return None, ("CNPJ inválido. Informe 14 dígitos.", 400)

    t0 = time.monotonic()
    try:
        resp = http_requests.get(
            f"https://minhareceita.org/{cnpj_val}",
            headers={"Accept": "application/json", "User-Agent": "DataConsult/1.0"},
            timeout=20,
        )
        if resp.status_code == 404:
            return None, ("CNPJ não encontrado na base da Receita.", 404)
        resp.raise_for_status()
        cnpj_data = resp.json()
    except Exception as e:
        return None, (f"Erro ao consultar CNPJ em minhareceita.org: {e}", 502)

    qsa = cnpj_data.get("qsa") or []
    socios_enriched = []
    cb_provider = get_provider("cpfbusca") or get_provider("dataflow")

    for socio in qsa:
        enriched = dict(socio)
        nome_socio = (socio.get("nome_socio") or "").strip()
        cpf_socio = (socio.get("cnpj_cpf_do_socio") or "").strip()
        digits = "".join(c for c in cpf_socio if c.isdigit())
        cpf_meio = ""
        cpf_full_digits = ""
        if len(digits) == 6:
            cpf_meio = digits
        elif len(digits) == 11:
            cpf_meio = digits[3:9]
            cpf_full_digits = digits
        elif len(digits) > 6:
            cpf_meio = digits[:6]

        enriched["cpf_parcial_extraido"] = cpf_meio
        person_nome3 = {}
        empresas_vinc = []

        # 1º Enriquecimento: GET /nome3 (Nome + CPF parcial)
        if cb_provider and nome_socio and len(cpf_meio) == 6:
            try:
                url1, headers1 = build_provider_request_config(cb_provider, "/nome3")
                params1 = {"nome": nome_socio, "cpf_meio": cpf_meio}
                r1 = http_requests.get(url1, params=params1, headers=headers1, timeout=20)
                if r1.ok:
                    nome3_json = r1.json()
                    inner = nome3_json.get("data", {}) if isinstance(nome3_json, dict) else {}
                    person_list = []
                    if isinstance(inner, dict):
                        person_list = inner.get("data") if isinstance(inner.get("data"), list) else inner.get("results") or []
                        empresas_vinc = inner.get("empresas_vinculadas") or []
                    elif isinstance(inner, list):
                        person_list = inner

                    enriched["consulta_nome3"] = nome3_json
                    if person_list and isinstance(person_list[0], dict):
                        person_nome3 = dict(person_list[0])
                        if empresas_vinc and not person_nome3.get("empresas_vinculadas"):
                            person_nome3["empresas_vinculadas"] = empresas_vinc
                        cpf_from_nome3 = "".join(c for c in str(person_nome3.get("cpf") or "") if c.isdigit())
                        if len(cpf_from_nome3) == 11:
                            cpf_full_digits = cpf_from_nome3
            except Exception:
                pass

        # 2º Enriquecimento: GET /cpf?cpf=<CPF_OBTIDO> (Consulta completa de CPF)
        cpf_body = {}
        if cb_provider and len(cpf_full_digits) == 11:
            try:
                url2, headers2 = build_provider_request_config(cb_provider, "/cpf")
                r2 = http_requests.get(url2, params={"cpf": cpf_full_digits}, headers=headers2, timeout=25)
                if r2.ok:
                    cpf_json = r2.json()
                    enriched["consulta_cpf"] = cpf_json
                    d2 = cpf_json.get("data", cpf_json) if isinstance(cpf_json, dict) else {}
                    cpf_body = d2.get("body", d2) if isinstance(d2, dict) else {}
                    if not isinstance(cpf_body, dict):
                        cpf_body = {}
            except Exception:
                pass

        # Consolida dados do 1º e 2º enriquecimento priorizando os dados completos do 2º (/cpf)
        merged_person = dict(person_nome3)
        if cpf_body:
            merged_person.update(cpf_body)
            if not merged_person.get("nome") and cpf_body.get("name"):
                merged_person["nome"] = cpf_body["name"]
            if not merged_person.get("cpf"):
                merged_person["cpf"] = cpf_body.get("cpf_masked") or cpf_body.get("cpf") or cpf_full_digits
            if not merged_person.get("nascimento") and cpf_body.get("birth_date"):
                merged_person["nascimento"] = cpf_body["birth_date"]
            if not merged_person.get("nome_mae") and cpf_body.get("mother_name"):
                merged_person["nome_mae"] = cpf_body["mother_name"]
            if not merged_person.get("nome_pai") and cpf_body.get("father_name"):
                merged_person["nome_pai"] = cpf_body["father_name"]
            if not merged_person.get("profissao") and (cpf_body.get("occupation") or cpf_body.get("occupation_name")):
                merged_person["profissao"] = cpf_body.get("occupation") or cpf_body.get("occupation_name")
            if not merged_person.get("renda") and cpf_body.get("income") is not None:
                merged_person["renda"] = cpf_body.get("income")
            if not merged_person.get("titulo_eleitor") and cpf_body.get("voter_id"):
                merged_person["titulo_eleitor"] = cpf_body.get("voter_id")
            phones_extracted = extract_phones_from_cpf_body(cpf_body)
            merged_person["phones_consolidated"] = phones_extracted
            enriched["telefones"] = phones_extracted

        # Consolida empresas vinculadas de ambas as consultas
        emp_seen = set()
        merged_empresas = []
        for emp_source in (
            person_nome3.get("empresas_vinculadas") or [],
            empresas_vinc or [],
            cpf_body.get("empresas_vinculadas") or [],
            cpf_body.get("empresas") or [],
        ):
            if isinstance(emp_source, list):
                for emp in emp_source:
                    if isinstance(emp, dict):
                        ecnpj = "".join(c for c in str(emp.get("cnpj") or "") if c.isdigit()) or id(emp)
                        if ecnpj not in emp_seen:
                            emp_seen.add(ecnpj)
                            merged_empresas.append(emp)

        if merged_empresas:
            merged_person["empresas_vinculadas"] = merged_empresas

        enriched["cpf_completo"] = cpf_full_digits
        enriched["dados_cpf"] = cpf_body if cpf_body else None
        enriched["dados_pessoais"] = merged_person if merged_person else None
        enriched["empresas_vinculadas"] = merged_empresas
        socios_enriched.append(enriched)

    ms = int((time.monotonic() - t0) * 1000)
    enriched_cnpj = dict(cnpj_data)
    enriched_cnpj["qsa"] = socios_enriched
    return (enriched_cnpj, socios_enriched, ms), None


# ── Auth Routes ─────────────────────────────────────────────────────

@app.route("/login")
def login_page():
    if get_user():
        return redirect(url_for("dashboard"))
    return render_template("login.html")


@app.route("/api/auth/login", methods=["POST"])
def login_action():
    data = request.json or {}
    u = (data.get("username") or "").strip()
    p = data.get("password", "")
    user = User.query.filter_by(username=u).first()
    if not user or user.password_hash != hash_pw(p) or not user.is_active:
        return jsonify({"error": "Credenciais inválidas"}), 401
    session.clear()
    session["user_id"] = user.id
    session["user_role"] = user.role
    return jsonify({"ok": True, "role": user.role})


@app.route("/api/auth/logout", methods=["POST"])
def logout_action():
    session.clear()
    return jsonify({"ok": True})


@app.route("/api/auth/password", methods=["POST"])
@login_required
def change_password():
    data = request.json or {}
    current = data.get("current", "")
    new_pw = data.get("new", "")
    if len(new_pw) < 4:
        return jsonify({"error": "Senha deve ter pelo menos 4 caracteres"}), 400
    user = get_user()
    if user.password_hash != hash_pw(current):
        return jsonify({"error": "Senha atual incorreta"}), 400
    user.password_hash = hash_pw(new_pw)
    db.session.commit()
    return jsonify({"ok": True})


# ── Dashboard Routes ────────────────────────────────────────────────

@app.route("/")
@login_required
def dashboard():
    user = get_user()
    if user.role == "admin":
        return redirect(url_for("admin_dashboard"))
    elif user_has_api(user) and not user_has_consulta(user):
        return redirect(url_for("api_dashboard"))
    return redirect(url_for("user_dashboard"))


@app.route("/admin")
@admin_required
def admin_dashboard():
    user = get_user()
    total_users = User.query.count()
    total_queries = QueryLog.query.count()
    today_queries = QueryLog.query.filter(QueryLog.created_at >= datetime.utcnow().replace(hour=0, minute=0, second=0)).count()
    providers = Provider.query.all()
    users = User.query.order_by(User.id.desc()).all()
    plans = Plan.query.order_by(Plan.id.desc()).all()
    return render_template("admin_dashboard.html", user=user, total_users=total_users,
                           total_queries=total_queries, today_queries=today_queries,
                           providers=providers, users=users, plans=plans, modules=MODULES)


@app.route("/admin/batch")
@admin_required
def admin_batch_page():
    user = get_user()
    providers = Provider.query.all()
    jobs = BatchJob.query.order_by(BatchJob.id.desc()).limit(50).all()
    return render_template("consulta_nome.html", user=user, providers=providers, jobs=jobs, estados=ESTADOS)


@app.route("/painel")
@login_required
def user_dashboard():
    user = get_user()
    if not user_has_consulta(user) and user_has_api(user):
        return redirect(url_for("api_dashboard"))
    user_modules = get_user_modules(user)
    categories = sorted(set(m["category"] for m in user_modules))
    return render_template("user_dashboard.html", user=user, modules=user_modules,
                           categories=categories, estados=ESTADOS)


@app.route("/api-painel")
@login_required
def api_dashboard():
    user = get_user()
    if not user_has_api(user):
        return redirect(url_for("user_dashboard"))
    keys = UserApiKey.query.filter_by(user_id=user.id).order_by(UserApiKey.id.desc()).all()
    user_modules = get_user_modules(user)
    return render_template("api_dashboard.html", user=user, keys=keys, modules=user_modules,
                           all_modules=MODULES, categories=MODULE_CATEGORIES)


@app.route("/perfil")
@login_required
def profile_page():
    user = get_user()
    return render_template("profile.html", user=user)


# ── Admin API ───────────────────────────────────────────────────────

@app.route("/api/admin/users", methods=["GET"])
@admin_required
def admin_list_users():
    users = User.query.order_by(User.id.desc()).all()
    plans = {p.id: p.name for p in Plan.query.all()}
    return jsonify([{
        "id": u.id, "username": u.username, "email": u.email, "role": u.role,
        "plan": plans.get(u.plan_id, "Nenhum"), "plan_id": u.plan_id,
        "credits_today": u.credits_used_today, "credits_total": u.credits_used_total,
        "is_active": u.is_active,
        "created_at": u.created_at.isoformat() if u.created_at else "",
    } for u in users])


@app.route("/api/admin/users", methods=["POST"])
@admin_required
def admin_create_user():
    data = request.json or {}
    username = (data.get("username") or "").strip()
    password = data.get("password", "")
    role = data.get("role", "user")
    plan_id = data.get("plan_id")
    email = (data.get("email") or "").strip()
    if not username or not password:
        return jsonify({"error": "Username e senha obrigatórios"}), 400
    if User.query.filter_by(username=username).first():
        return jsonify({"error": "Username já existe"}), 400
    if role not in ("user", "api_user", "consulta_api", "admin"):
        return jsonify({"error": "Role inválida"}), 400
    u = User(username=username, email=email, password_hash=hash_pw(password),
             role=role, plan_id=int(plan_id) if plan_id else None)
    db.session.add(u)
    db.session.commit()
    return jsonify({"ok": True, "id": u.id})


@app.route("/api/admin/users/<int:uid>", methods=["PUT"])
@admin_required
def admin_update_user(uid):
    u = db.session.get(User, uid)
    if not u:
        return jsonify({"error": "Usuário não encontrado"}), 404
    data = request.json or {}
    if "email" in data:
        u.email = data["email"]
    if "role" in data and data["role"] in ("user", "api_user", "consulta_api", "admin"):
        u.role = data["role"]
    if "plan_id" in data:
        u.plan_id = int(data["plan_id"]) if data["plan_id"] else None
    if "is_active" in data:
        u.is_active = bool(data["is_active"])
    if "password" in data and data["password"]:
        u.password_hash = hash_pw(data["password"])
    if "credits_used_today" in data:
        u.credits_used_today = int(data["credits_used_today"])
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/api/admin/users/<int:uid>", methods=["DELETE"])
@admin_required
def admin_delete_user(uid):
    u = db.session.get(User, uid)
    if not u:
        return jsonify({"error": "Usuário não encontrado"}), 404
    if u.username == "admin":
        return jsonify({"error": "Não é possível excluir o admin"}), 400
    db.session.delete(u)
    db.session.commit()
    return jsonify({"ok": True})


# ── Admin User View & Impersonate ──────────────────────────────────

@app.route("/admin/user/<int:uid>")
@admin_required
def admin_view_user(uid):
    u = db.session.get(User, uid)
    if not u:
        return redirect("/admin")
    user = get_user()
    plans = Plan.query.order_by(Plan.name).all()
    logs = QueryLog.query.filter_by(user_id=uid).order_by(QueryLog.created_at.desc()).limit(50).all()
    api_keys = UserApiKey.query.filter_by(user_id=uid).all()
    return render_template("admin_user_view.html", user=user, target=u, plans=plans, logs=logs, api_keys=api_keys)


@app.route("/api/admin/impersonate/<int:uid>", methods=["POST"])
@admin_required
def admin_impersonate(uid):
    u = db.session.get(User, uid)
    if not u:
        return jsonify({"error": "Usuário não encontrado"}), 404
    if u.id == get_user().id:
        return jsonify({"error": "Selecione outra conta de usuário"}), 400
    session.setdefault("admin_user_id", get_user().id)
    session["user_id"] = u.id
    session["user_role"] = u.role
    return jsonify({"ok": True, "username": u.username})


@app.route("/api/admin/return", methods=["POST"])
def admin_return():
    admin_id = session.get("admin_user_id")
    admin = db.session.get(User, admin_id) if admin_id else None
    if not admin or admin.role != "admin" or not admin.is_active:
        return jsonify({"error": "Sessão administrativa inválida. Entre novamente."}), 403
    session.clear()
    session["user_id"] = admin.id
    session["user_role"] = admin.role
    return jsonify({"ok": True})


# ── Plans API ───────────────────────────────────────────────────────

@app.route("/api/admin/plans", methods=["GET"])
@admin_required
def admin_list_plans():
    plans = Plan.query.order_by(Plan.id.desc()).all()
    return jsonify([{
        "id": p.id, "name": p.name, "plan_type": p.plan_type,
        "daily_limit": p.daily_limit, "monthly_limit": p.monthly_limit,
        "modules": p.get_modules(), "price": p.price, "is_active": p.is_active,
        "user_count": User.query.filter_by(plan_id=p.id).count(),
    } for p in plans])


@app.route("/api/admin/plans", methods=["POST"])
@admin_required
def admin_create_plan():
    data = request.json or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "Nome obrigatório"}), 400
    plan_type = data.get("plan_type", "consulta")
    if plan_type not in ("consulta", "api", "consulta_api", "ambos"):
        plan_type = "consulta"
    p = Plan(
        name=name,
        plan_type=plan_type,
        daily_limit=int(data.get("daily_limit", 100)),
        monthly_limit=int(data.get("monthly_limit", 3000)),
        modules=json.dumps(data.get("modules", [])),
        price=float(data.get("price", 0)),
    )
    db.session.add(p)
    db.session.commit()
    return jsonify({"ok": True, "id": p.id})


@app.route("/api/admin/plans/<int:pid>", methods=["PUT"])
@admin_required
def admin_update_plan(pid):
    p = db.session.get(Plan, pid)
    if not p:
        return jsonify({"error": "Plano não encontrado"}), 404
    data = request.json or {}
    if "name" in data:
        p.name = data["name"]
    if "plan_type" in data and data["plan_type"] in ("consulta", "api", "consulta_api", "ambos"):
        p.plan_type = data["plan_type"]
    if "daily_limit" in data:
        p.daily_limit = int(data["daily_limit"])
    if "monthly_limit" in data:
        p.monthly_limit = int(data["monthly_limit"])
    if "modules" in data:
        p.modules = json.dumps(data["modules"])
    if "price" in data:
        p.price = float(data["price"])
    if "is_active" in data:
        p.is_active = bool(data["is_active"])
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/api/admin/plans/<int:pid>", methods=["DELETE"])
@admin_required
def admin_delete_plan(pid):
    p = db.session.get(Plan, pid)
    if not p:
        return jsonify({"error": "Plano não encontrado"}), 404
    db.session.delete(p)
    db.session.commit()
    return jsonify({"ok": True})


# ── Providers API ───────────────────────────────────────────────────

@app.route("/api/admin/providers", methods=["GET"])
@admin_required
def admin_list_providers():
    provs = Provider.query.order_by(Provider.id.desc()).all()
    return jsonify([{
        "id": p.id, "name": p.name, "slug": p.slug, "base_url": p.base_url,
        "auth_type": p.auth_type, "auth_header": p.auth_header,
        "auth_value_masked": p.auth_value[:8] + "..." if len(p.auth_value) > 8 else "***",
        "is_active": p.is_active, "total_queries": p.total_queries,
        "last_used": p.last_used.isoformat() if p.last_used else None,
    } for p in provs])


@app.route("/api/admin/providers", methods=["POST"])
@admin_required
def admin_create_provider():
    data = request.json or {}
    name = (data.get("name") or "").strip()
    slug = (data.get("slug") or "").strip().lower()
    base_url = (data.get("base_url") or "").strip().rstrip("/")
    auth_value = (data.get("auth_value") or "").strip()
    if not name or not slug or not base_url or not auth_value:
        return jsonify({"error": "Todos os campos são obrigatórios"}), 400
    if Provider.query.filter_by(slug=slug).first():
        return jsonify({"error": "Slug já existe"}), 400
    p = Provider(name=name, slug=slug, base_url=base_url,
                 auth_header=data.get("auth_header", "x-api-key"),
                 auth_value=auth_value, is_active=True)
    db.session.add(p)
    db.session.commit()
    return jsonify({"ok": True, "id": p.id})


@app.route("/api/admin/providers/<int:pid>", methods=["PUT"])
@admin_required
def admin_update_provider(pid):
    p = db.session.get(Provider, pid)
    if not p:
        return jsonify({"error": "Provider não encontrado"}), 404
    data = request.json or {}
    for field in ("name", "base_url", "auth_header", "auth_value"):
        if field in data and data[field]:
            setattr(p, field, data[field].strip() if isinstance(data[field], str) else data[field])
    if "is_active" in data:
        p.is_active = bool(data["is_active"])
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/api/admin/providers/<int:pid>", methods=["DELETE"])
@admin_required
def admin_delete_provider(pid):
    p = db.session.get(Provider, pid)
    if not p:
        return jsonify({"error": "Provider não encontrado"}), 404
    db.session.delete(p)
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/api/admin/providers/<int:pid>/test", methods=["POST"])
@admin_required
def admin_test_provider(pid):
    p = db.session.get(Provider, pid)
    if not p:
        return jsonify({"error": "Provider não encontrado"}), 404
    try:
        test_path = "/nome" if p.slug == "dataflow" else "/cpf"
        test_params = {"nome": "TESTE"} if p.slug == "dataflow" else {"cpf": "00000000000"}
        resp, ms = provider_request(p, test_path, test_params)
        return jsonify({
            "ok": resp.status_code < 500,
            "status_code": resp.status_code,
            "response_time_ms": ms,
        })
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)})


# ── Modules info ────────────────────────────────────────────────────

@app.route("/api/modules")
@login_required
def list_modules():
    user = get_user()
    return jsonify(get_user_modules(user))


# ── Unified Query ───────────────────────────────────────────────────

@app.route("/api/consulta", methods=["POST"])
@login_required
def consulta():
    user = get_user()
    data = request.json or {}

    if data.get("history_id") is not None:
        log = db.session.get(QueryLog, int(data["history_id"]))
        if not log:
            return jsonify({"error": "Consulta não encontrada"}), 404
        saved = QueryResponse.query.filter_by(log_id=log.id).first()
        if not saved:
            return jsonify({"error": "Resposta não disponível"}), 410
        payload = json.loads(saved.payload)
        payload.update({"history_id": log.id, "from_history": True})
        return jsonify(payload)

    module_id = data.get("module", "cb_cpf")
    if module_id in ("df_cnpj", "cb_cnpj"):
        module_id = "cnpj_titulares"
    module = MODULES_BY_ID.get(module_id)
    if not module:
        return jsonify({"error": "Módulo não encontrado"}), 400

    allowed = set(user.plan.get_modules() if user.plan else [])
    allowed.add("cnpj_titulares")
    if user.role != "admin" and module_id not in allowed:
        return jsonify({"error": "Módulo não disponível no seu plano"}), 403

    if not debit_credit(user):
        return jsonify({"error": "Limite diário atingido"}), 429

    # Special handling for cnpj_titulares (uses minhareceita.org + cpfbusca /nome3)
    if module_id == "cnpj_titulares":
        cnpj_raw = data.get("cnpj") or data.get("valor") or ""
        res_tuple, err = execute_cnpj_titulares_lookup(cnpj_raw)
        if err:
            return jsonify({"error": err[0]}), err[1]
        enriched_cnpj, socios_enriched, ms = res_tuple
        cnpj_val = enriched_cnpj.get("cnpj", "")
        rows = [{
            "razao_social": enriched_cnpj.get("razao_social", ""),
            "cnpj": cnpj_val,
            "nome_fantasia": enriched_cnpj.get("nome_fantasia", ""),
            "situacao": enriched_cnpj.get("descricao_situacao_cadastral", ""),
            "porte": enriched_cnpj.get("porte") or enriched_cnpj.get("descricao_porte", ""),
            "capital_social": enriched_cnpj.get("capital_social", 0),
            "cnae_principal": enriched_cnpj.get("cnae_fiscal_descricao", ""),
            "municipio": enriched_cnpj.get("municipio", ""),
            "uf": enriched_cnpj.get("uf", ""),
        }]
        lists = {"qsa_enriquecido": socios_enriched}
        payload = {
            "success": True,
            "rows": rows,
            "lists": lists,
            "total": 1 + len(socios_enriched),
            "time_ms": ms,
            "meta": {"time_ms": ms, "module": module_id},
            "module": module_id,
            "data": enriched_cnpj,
            "raw_response": {
                "success": True,
                "consulta_cnpj": enriched_cnpj,
                "qsa": socios_enriched,
                "data": enriched_cnpj,
                "meta": {"time_ms": ms, "module": module_id},
            },
        }
        log = QueryLog(user_id=user.id, endpoint="/cnpj-titulares", module=module_id, query_param=cnpj_val,
                       provider_slug="minhareceita", result_count=len(socios_enriched) + 1, status_code=200, response_time_ms=ms)
        db.session.add(log)
        db.session.flush()
        db.session.add(QueryResponse(log_id=log.id, payload=json.dumps(payload, ensure_ascii=False)))
        db.session.commit()
        payload["history_id"] = log.id
        payload["from_history"] = False
        return jsonify(payload)

    provider = get_provider(module["provider"])
    if not provider:
        return jsonify({"error": f"Provedor '{module.get('provider_label', module['provider'])}' não configurado ou inativo no painel Admin."}), 400

    params = {}
    valor = (data.get("valor") or "").strip()
    if module_id == "df_documentos" and valor:
        pname, pval = detect_doc_param(valor)
        params[pname] = pval
    elif valor:
        first_required = next((p["name"] for p in module["params"] if p["required"]), None)
        if first_required:
            params[first_required] = valor
    for p in module["params"]:
        if p["name"] in data and str(data[p["name"]]).strip():
            params[p["name"]] = str(data[p["name"]]).strip()
    if "estado" in data and data["estado"]:
        params["estado"] = data["estado"]
    if "uf" in data and data["uf"] and "uf" not in params:
        params["uf"] = data["uf"]

    # Clean document digits for CPF/CEP/CNPJ/Phone modules when appropriate
    for doc_field in ("cpf", "cep", "cnpj", "telefone", "phone", "pis", "nis", "titulo"):
        if doc_field in params:
            cleaned = "".join(c for c in str(params[doc_field]) if c.isdigit())
            if cleaned:
                params[doc_field] = cleaned

    try:
        resp, ms = provider_request(provider, module["path"], params)
        result = resp.json()

        rows = []
        lists = {}
        meta = result.get("meta", {}) if isinstance(result, dict) else {}
        if not isinstance(meta, dict):
            meta = {}
        meta.setdefault("time_ms", ms)
        meta.setdefault("module", module_id)

        raw_data = result.get("data", result) if isinstance(result, dict) else result
        if isinstance(raw_data, dict):
            if isinstance(raw_data.get("body"), list):
                rows = raw_data["body"]
            elif isinstance(raw_data.get("body"), dict):
                rows = [raw_data["body"]]
                _, lists = flatten_single_record(raw_data["body"])
            elif isinstance(raw_data.get("data"), list):
                rows = raw_data["data"]
                if raw_data.get("empresas_vinculadas"):
                    lists["empresas_vinculadas"] = raw_data["empresas_vinculadas"]
            elif isinstance(raw_data.get("data"), dict):
                rows = [raw_data["data"]]
            elif isinstance(raw_data.get("results"), list):
                rows = raw_data["results"]
            elif raw_data:
                flat, lists = flatten_single_record(raw_data)
                rows = [raw_data] if raw_data else ([flat] if flat else [])
        elif isinstance(raw_data, list):
            rows = raw_data

        payload = {
            "success": resp.status_code < 400,
            "rows": rows,
            "lists": lists,
            "total": len(rows),
            "time_ms": ms,
            "meta": meta,
            "data": raw_data,
            "raw_response": result,
            "module": module_id,
        }

        q_str = valor or next(iter(params.values()), "") or json.dumps(params, ensure_ascii=False)
        log = QueryLog(user_id=user.id, endpoint=module["path"], module=module_id,
                       query_param=str(q_str)[:290], provider_slug=module["provider"],
                       result_count=len(rows), status_code=resp.status_code, response_time_ms=ms)
        db.session.add(log)
        db.session.flush()
        db.session.add(QueryResponse(log_id=log.id, payload=json.dumps(payload, ensure_ascii=False)))
        db.session.commit()
        payload["history_id"] = log.id
        payload["from_history"] = False
        return jsonify(payload)

    except http_requests.exceptions.Timeout:
        return jsonify({"error": "Timeout — o provedor demorou mais de 25s para responder"}), 504
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Logs ────────────────────────────────────────────────────────────

@app.route("/api/logs")
@login_required
def query_logs():
    user = get_user()
    q = QueryLog.query
    if user.role != "admin":
        q = q.filter_by(user_id=user.id)
    logs = q.order_by(QueryLog.id.desc()).limit(200).all()
    return jsonify([{
        "id": l.id, "module": l.module, "endpoint": l.endpoint,
        "query": l.query_param, "provider": l.provider_slug,
        "results": l.result_count, "status": l.status_code,
        "time_ms": l.response_time_ms,
        "date": l.created_at.isoformat() if l.created_at else "",
    } for l in logs])


@app.route("/api/logs/stats")
@login_required
def log_stats():
    user = get_user()
    q = QueryLog.query
    if user.role != "admin":
        q = q.filter_by(user_id=user.id)
    total = q.count()
    today = q.filter(QueryLog.created_at >= datetime.utcnow().replace(hour=0, minute=0, second=0)).count()
    return jsonify({
        "total_queries": total, "today": today,
        "credits_today": user.credits_used_today,
        "daily_limit": user.plan.daily_limit if user.plan else 0,
        "credits_total": user.credits_used_total,
    })


# ── API Keys (for API users) ───────────────────────────────────────

@app.route("/api/keys", methods=["GET"])
@login_required
def list_api_keys():
    user = get_user()
    keys = UserApiKey.query.filter_by(user_id=user.id).order_by(UserApiKey.id.desc()).all()
    return jsonify([{
        "id": k.id,
        "key": k.key[:max(1, len(k.key) // 5)] + "•" * (len(k.key) - max(1, len(k.key) // 5)),
        "label": k.label, "is_active": k.is_active,
        "created_at": k.created_at.isoformat() if k.created_at else "",
        "last_used": k.last_used.isoformat() if k.last_used else None,
        "total_requests": k.total_requests,
    } for k in keys])


@app.route("/api/keys", methods=["POST"])
@login_required
def create_api_key():
    user = get_user()
    data = request.json or {}
    label = (data.get("label") or "").strip() or "API Key"
    key = f"sk_{secrets.token_hex(24)}"
    ak = UserApiKey(user_id=user.id, key=key, label=label)
    db.session.add(ak)
    db.session.commit()
    return jsonify({"ok": True, "id": ak.id})


@app.route("/api/keys/<int:kid>/reveal", methods=["POST"])
@login_required
def reveal_api_key(kid):
    user = get_user()
    key = UserApiKey.query.filter_by(id=kid, user_id=user.id).first()
    if not key and user.role == "admin":
        key = db.session.get(UserApiKey, kid)
    if not key:
        return jsonify({"error": "Chave não encontrada"}), 404
    original = db.session.get(User, session.get("admin_user_id")) if session.get("admin_user_id") else None
    is_admin = user.role == "admin" or (original and original.role == "admin" and original.is_active)
    if not is_admin:
        now = time.time()
        if session.get("key_reveal_blocked_until", 0) > now:
            return jsonify({"error": "Muitas tentativas. Aguarde 5 minutos."}), 429
        password = (request.get_json(silent=True) or {}).get("password", "")
        if not password or not secrets.compare_digest(user.password_hash, hash_pw(password)):
            attempts = session.get("key_reveal_attempts", 0) + 1
            session["key_reveal_attempts"] = attempts
            if attempts >= 5:
                session["key_reveal_blocked_until"] = now + 300
            return jsonify({"error": "Senha incorreta"}), 403
        session.pop("key_reveal_attempts", None)
        session.pop("key_reveal_blocked_until", None)
    response = jsonify({"key": key.key})
    response.headers["Cache-Control"] = "no-store"
    return response


@app.route("/api/keys/<int:kid>/playground", methods=["POST", "OPTIONS"])
@login_required
def key_playground(kid):
    if request.method == "OPTIONS":
        return "", 204
    user = get_user()
    key = UserApiKey.query.filter_by(id=kid, user_id=user.id, is_active=True).first()
    if not key:
        return jsonify({"error": "Chave inválida ou inativa", "code": "UNAUTHORIZED"}), 403
    key.last_used = datetime.utcnow()
    key.total_requests = (key.total_requests or 0) + 1
    db.session.commit()
    return external_api_query(user)


@app.route("/api/keys/<int:kid>", methods=["DELETE"])
@login_required
def revoke_api_key(kid):
    user = get_user()
    ak = UserApiKey.query.filter_by(id=kid, user_id=user.id).first()
    if not ak:
        return jsonify({"error": "Key não encontrada"}), 404
    db.session.delete(ak)
    db.session.commit()
    return jsonify({"ok": True})


# ── External API (for API plan users & Playground) ──────────────────

@app.route("/api/v1/query", methods=["POST", "OPTIONS"])
def external_api_query(user_override=None):
    if request.method == "OPTIONS":
        return "", 204
    user = user_override or api_key_auth()
    if not user:
        # Also allow logged-in session user if they pass "SESSION_KEY" from Playground
        sess_user = get_user()
        req_key = (request.headers.get("X-API-Key") or "").strip()
        if sess_user and req_key == "SESSION":
            user = sess_user
        else:
            return jsonify({"error": "API key inválida ou inativa", "code": "UNAUTHORIZED"}), 401

    data = request.get_json(silent=True) or {}
    module_id = data.get("module")
    if not module_id:
        return jsonify({"error": "Campo 'module' obrigatório", "code": "MISSING_MODULE"}), 400
    if module_id in ("df_cnpj", "cb_cnpj"):
        module_id = "cnpj_titulares"

    module = MODULES_BY_ID.get(module_id)
    if not module:
        return jsonify({"error": f"Módulo '{module_id}' não existe", "code": "INVALID_MODULE"}), 400

    allowed = set(user.plan.get_modules() if user.plan else [])
    allowed.add("cnpj_titulares")
    if user.role != "admin" and module_id not in allowed:
        return jsonify({"error": "Módulo não disponível no seu plano", "code": "FORBIDDEN"}), 403

    if not debit_credit(user):
        return jsonify({"error": "Limite diário atingido", "code": "RATE_LIMITED"}), 429

    # Special: cnpj_titulares uses minhareceita.org + cpfbusca /nome3
    if module_id == "cnpj_titulares":
        cnpj_raw = data.get("cnpj") or data.get("valor") or ""
        res_tuple, err = execute_cnpj_titulares_lookup(cnpj_raw)
        if err:
            code = "MISSING_PARAMS" if err[1] == 400 else "SERVICE_UNAVAILABLE"
            return jsonify({"error": err[0], "code": code}), err[1]
        enriched_cnpj, socios_enriched, ms = res_tuple
        log = QueryLog(user_id=user.id, endpoint="/cnpj-titulares", module=module_id,
                       query_param=enriched_cnpj.get("cnpj", ""), provider_slug="minhareceita",
                       result_count=len(socios_enriched), status_code=200, response_time_ms=ms)
        db.session.add(log)
        db.session.commit()
        return jsonify({"success": True, "data": enriched_cnpj, "meta": {"time_ms": ms, "module": module_id}})

    provider = get_provider(module["provider"])
    if not provider:
        return jsonify({"error": f"Provedor '{module['provider']}' temporariamente indisponível ou não configurado", "code": "SERVICE_UNAVAILABLE"}), 503

    params = {p["name"]: str(data.get(p["name"], "")).strip() for p in module["params"] if data.get(p["name"]) is not None and str(data.get(p["name"], "")).strip() != ""}
    missing = [p["name"] for p in module["params"] if p["required"] and not params.get(p["name"])]
    if missing:
        return jsonify({"error": f"Parâmetros obrigatórios: {', '.join(missing)}", "code": "MISSING_PARAMS"}), 400

    for doc_field in ("cpf", "cep", "cnpj", "telefone", "phone", "pis", "nis", "titulo"):
        if doc_field in params:
            cleaned = "".join(c for c in str(params[doc_field]) if c.isdigit())
            if cleaned:
                params[doc_field] = cleaned

    try:
        resp, ms = provider_request(provider, module["path"], params)
        result = resp.json()
        log = QueryLog(user_id=user.id, endpoint=module["path"], module=module_id,
                       query_param=json.dumps(params, ensure_ascii=False)[:290], provider_slug=module["provider"],
                       result_count=1 if resp.ok else 0, status_code=resp.status_code, response_time_ms=ms)
        db.session.add(log)
        db.session.commit()
        return jsonify({
            "success": resp.status_code < 400,
            "data": result.get("data", result) if isinstance(result, dict) else result,
            "meta": {"time_ms": ms, "module": module_id},
        }), (200 if resp.status_code < 400 else resp.status_code)
    except Exception as e:
        return jsonify({"error": str(e), "code": "INTERNAL_ERROR"}), 500


@app.route("/api/v1/modules", methods=["GET", "OPTIONS"])
def external_api_modules():
    if request.method == "OPTIONS":
        return "", 204
    user = api_key_auth() or get_user()
    if not user:
        return jsonify({"error": "API key inválida", "code": "UNAUTHORIZED"}), 401
    user_mods = get_user_modules(user)
    mods = [{
        "id": m["id"], "label": m["label"], "category": m["category"],
        "provider": m["provider"], "provider_label": m.get("provider_label", m["provider"]),
        "params": m["params"], "description": m["description"],
    } for m in user_mods]
    return jsonify({"modules": mods})


# ── Export & Upload ─────────────────────────────────────────────────

@app.route("/api/export_csv", methods=["POST"])
@login_required
def export_csv():
    data = request.json or {}
    rows = data.get("rows", [])
    if not rows:
        return jsonify({"error": "Sem dados"}), 400
    all_keys = list(dict.fromkeys(k for row in rows for k in row))
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=all_keys, delimiter=";", extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    mem = io.BytesIO()
    mem.write(b"\xef\xbb\xbf")
    mem.write(buf.getvalue().encode("utf-8"))
    mem.seek(0)
    return send_file(mem, mimetype="text/csv", as_attachment=True,
                     download_name=f"consulta_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")


@app.route("/api/export_json", methods=["POST"])
@login_required
def export_json():
    data = request.json or {}
    rows = data.get("rows", [])
    if not rows:
        return jsonify({"error": "Sem dados"}), 400
    mem = io.BytesIO()
    mem.write(json.dumps(rows, ensure_ascii=False, indent=2).encode("utf-8"))
    mem.seek(0)
    return send_file(mem, mimetype="application/json", as_attachment=True,
                     download_name=f"consulta_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")


def _pdf_escape(text):
    s = str(text if text is not None else "")
    s = s.replace("\r", " ").replace("\n", " ")
    # Map common Unicode chars to WinAnsi (Latin-1)
    replacements = {
        "•": "-", "·": "-", "—": "-", "–": "-", "“": '"', "”": '"',
        "‘": "'", "’": "'", "…": "...", "✓": "OK", "✅": "OK", "❌": "X",
    }
    for k, v in replacements.items():
        s = s.replace(k, v)
    b = s.encode("latin-1", errors="replace").decode("latin-1")
    return b.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _wrap_pdf_text(text, max_chars=85):
    s = str(text if text is not None else "").strip()
    if not s:
        return ["Não informado"]
    words = s.split()
    lines = []
    cur = ""
    for w in words:
        if len(w) > max_chars:
            if cur:
                lines.append(cur)
                cur = ""
            for i in range(0, len(w), max_chars):
                lines.append(w[i:i + max_chars])
        elif not cur:
            cur = w
        elif len(cur) + 1 + len(w) <= max_chars:
            cur += " " + w
        else:
            lines.append(cur)
            cur = w
    if cur:
        lines.append(cur)
    return lines or ["Não informado"]


def build_formatted_pdf_bytes(title, subtitle, sections):
    """Build a multi-page formatted A4 PDF (595 x 842 pt) using pure Python PDF 1.4."""
    PAGE_W, PAGE_H = 595, 842
    MARGIN_X = 36
    CONTENT_W = PAGE_W - 2 * MARGIN_X
    pages_ops = []
    cur_ops = []
    y = PAGE_H - 40

    def start_page(is_first=False):
        nonlocal cur_ops, y
        cur_ops = []
        if is_first:
            # Header banner (dark indigo)
            cur_ops.append("0.22 0.25 0.58 rg")
            cur_ops.append(f"{MARGIN_X} {PAGE_H - 92} {CONTENT_W} 58 re f")
            cur_ops.append("1 1 1 rg")
            cur_ops.append(f"BT /F2 13 Tf {MARGIN_X + 14} {PAGE_H - 56} Td ({_pdf_escape(title[:75])}) Tj ET")
            cur_ops.append("0.85 0.88 0.98 rg")
            cur_ops.append(f"BT /F1 8.5 Tf {MARGIN_X + 14} {PAGE_H - 74} Td ({_pdf_escape(subtitle[:105])}) Tj ET")
            y = PAGE_H - 112
        else:
            cur_ops.append("0.22 0.25 0.58 rg")
            cur_ops.append(f"{MARGIN_X} {PAGE_H - 52} {CONTENT_W} 22 re f")
            cur_ops.append("1 1 1 rg")
            cur_ops.append(f"BT /F2 8.5 Tf {MARGIN_X + 10} {PAGE_H - 44} Td ({_pdf_escape(title[:85])}) Tj ET")
            y = PAGE_H - 68

    def flush_page():
        if cur_ops:
            pages_ops.append(list(cur_ops))

    def ensure_space(needed_h):
        nonlocal y
        if y - needed_h < 48:
            flush_page()
            start_page(is_first=False)

    start_page(is_first=True)

    for sec in (sections or []):
        sec_title = str(sec.get("title") or "Seção").strip()
        rows = sec.get("rows") or []
        if not rows:
            continue
        ensure_space(42)
        # Section Header Bar
        cur_ops.append("0.93 0.94 0.98 rg")
        cur_ops.append(f"{MARGIN_X} {y - 20} {CONTENT_W} 20 re f")
        cur_ops.append("0.22 0.25 0.58 rg")
        cur_ops.append(f"{MARGIN_X} {y - 20} 4 20 re f")
        cur_ops.append("0.12 0.15 0.30 rg")
        cur_ops.append(f"BT /F2 9.5 Tf {MARGIN_X + 11} {y - 14} Td ({_pdf_escape(sec_title[:85])}) Tj ET")
        y -= 26

        for r_idx, row in enumerate(rows):
            if isinstance(row, (list, tuple)) and len(row) >= 2:
                k_str = str(row[0] or "").strip()
                v_str = str(row[1] if row[1] is not None and row[1] != "" else "Não informado").strip()
            elif isinstance(row, dict):
                k_str = str(row.get("label") or row.get("key") or "").strip()
                v_str = str(row.get("value") if row.get("value") is not None and row.get("value") != "" else "Não informado").strip()
            else:
                k_str = ""
                v_str = str(row or "").strip()

            v_lines = _wrap_pdf_text(v_str, max_chars=66 if k_str else 90)
            row_h = max(16, len(v_lines) * 12 + 4)
            ensure_space(row_h + 4)

            if r_idx % 2 == 1:
                cur_ops.append("0.975 0.98 0.99 rg")
                cur_ops.append(f"{MARGIN_X} {y - row_h + 2} {CONTENT_W} {row_h} re f")

            if k_str:
                cur_ops.append("0.38 0.42 0.52 rg")
                cur_ops.append(f"BT /F2 8 Tf {MARGIN_X + 8} {y - 10} Td ({_pdf_escape(k_str[:38])}) Tj ET")
                cur_ops.append("0.10 0.12 0.18 rg")
                for l_i, line in enumerate(v_lines):
                    cur_ops.append(f"BT /F1 8.5 Tf {MARGIN_X + 165} {y - 10 - l_i * 12} Td ({_pdf_escape(line)}) Tj ET")
            else:
                cur_ops.append("0.10 0.12 0.18 rg")
                for l_i, line in enumerate(v_lines):
                    cur_ops.append(f"BT /F1 8.5 Tf {MARGIN_X + 8} {y - 10 - l_i * 12} Td ({_pdf_escape(line)}) Tj ET")

            # Subtle divider line
            cur_ops.append("0.90 0.91 0.94 RG 0.4 w")
            cur_ops.append(f"{MARGIN_X} {y - row_h + 2} m {MARGIN_X + CONTENT_W} {y - row_h + 2} l S")
            y -= row_h

        y -= 10

    flush_page()
    total_pages = len(pages_ops) or 1

    # Add footer to each page
    now_str = datetime.now().strftime("%d/%m/%Y %H:%M")
    for idx, ops in enumerate(pages_ops, 1):
        ops.append("0.85 0.87 0.92 RG 0.5 w")
        ops.append(f"{MARGIN_X} 34 m {MARGIN_X + CONTENT_W} 34 l S")
        ops.append("0.45 0.48 0.55 rg")
        ops.append(f"BT /F1 7.5 Tf {MARGIN_X} 22 Td (DataConsult - Relatorio gerado em {_pdf_escape(now_str)}) Tj ET")
        page_lbl = f"Pagina {idx} de {total_pages}"
        ops.append(f"BT /F1 7.5 Tf {PAGE_W - MARGIN_X - 65} 22 Td ({page_lbl}) Tj ET")

    # Assemble PDF 1.4 structure
    # Object 1: Catalog, Object 2: Pages, Object 3: Font F1 (Helvetica), Object 4: Font F2 (Helvetica-Bold)
    objects = []
    kids_refs = []
    next_obj_id = 5
    page_entries = []
    for ops in pages_ops:
        page_obj_id = next_obj_id
        content_obj_id = next_obj_id + 1
        next_obj_id += 2
        kids_refs.append(f"{page_obj_id} 0 R")
        stream_bytes = "\n".join(ops).encode("latin-1", errors="replace")
        page_entries.append((page_obj_id, content_obj_id, stream_bytes))

    objects.append((1, b"<< /Type /Catalog /Pages 2 0 R >>"))
    objects.append((2, f"<< /Type /Pages /Kids [{' '.join(kids_refs)}] /Count {len(kids_refs)} >>".encode("latin-1")))
    objects.append((3, b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>"))
    objects.append((4, b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold /Encoding /WinAnsiEncoding >>"))

    for page_obj_id, content_obj_id, stream_bytes in page_entries:
        p_dict = (
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {PAGE_W} {PAGE_H}] "
            f"/Resources << /Font << /F1 3 0 R /F2 4 0 R >> >> /Contents {content_obj_id} 0 R >>"
        ).encode("latin-1")
        c_dict = f"<< /Length {len(stream_bytes)} >>\nstream\n".encode("latin-1") + stream_bytes + b"\nendstream"
        objects.append((page_obj_id, p_dict))
        objects.append((content_obj_id, c_dict))

    objects.sort(key=lambda x: x[0])
    buf = io.BytesIO()
    buf.write(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = {}
    for obj_id, obj_body in objects:
        offsets[obj_id] = buf.tell()
        buf.write(f"{obj_id} 0 obj\n".encode("latin-1"))
        buf.write(obj_body)
        buf.write(b"\nendobj\n")

    xref_pos = buf.tell()
    max_id = objects[-1][0]
    buf.write(f"xref\n0 {max_id + 1}\n".encode("latin-1"))
    buf.write(b"0000000000 65535 f \n")
    for i in range(1, max_id + 1):
        off = offsets.get(i, 0)
        buf.write(f"{off:010d} 00000 n \n".encode("latin-1"))
    buf.write(f"trailer\n<< /Size {max_id + 1} /Root 1 0 R >>\nstartxref\n{xref_pos}\n%%EOF\n".encode("latin-1"))
    buf.seek(0)
    return buf


@app.route("/api/export_pdf", methods=["POST"])
@login_required
def export_pdf():
    data = request.json or {}
    title = (data.get("title") or "Relatório de Consulta").strip()
    subtitle = (data.get("subtitle") or f"Gerado em {datetime.now().strftime('%d/%m/%Y %H:%M')}").strip()
    sections = data.get("sections") or []
    filename = secure_filename(data.get("filename") or f"relatorio_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf")
    if not filename.lower().endswith(".pdf"):
        filename += ".pdf"
    pdf_buf = build_formatted_pdf_bytes(title, subtitle, sections)
    return send_file(pdf_buf, mimetype="application/pdf", as_attachment=True, download_name=filename)


@app.route("/api/upload", methods=["POST"])
@admin_required
def upload_file():
    if "file" not in request.files:
        return jsonify({"error": "Nenhum arquivo enviado"}), 400
    f = request.files["file"]
    if not f.filename:
        return jsonify({"error": "Arquivo vazio"}), 400
    fname = secure_filename(f.filename)
    path = os.path.join(app.config["UPLOAD_FOLDER"], f"{int(time.time())}_{fname}")
    f.save(path)
    return jsonify({"ok": True, "path": path, "filename": fname})


# ── Batch Engine (Admin-Only: Nome+Cidade nome4 → CPF Telefones) ───

batch_state = {
    "running": False,
    "paused": False,
    "stop_flag": False,
    "queue": queue.Queue(),
    "total": 0,
    "done": 0,
    "found": 0,
    "errors": 0,
    "skipped": 0,
    "threads": 3,
    "rpm": 20,
    "items": [],
    "results": [],
    "job_id": None,
    "batch_type": "nome4_cpf",
}
batch_lock = threading.Lock()


class RateLimiter:
    def __init__(self, max_per_minute):
        self.max_per_minute = max(1, int(max_per_minute))
        self.lock = threading.Lock()
        self.timestamps = []

    def set_rpm(self, rpm):
        with self.lock:
            self.max_per_minute = max(1, int(rpm))

    def acquire(self):
        while True:
            if batch_state["stop_flag"]:
                return
            while batch_state["paused"] and not batch_state["stop_flag"]:
                time.sleep(0.25)
            with self.lock:
                now = time.monotonic()
                self.timestamps = [t for t in self.timestamps if now - t < 60.0]
                if len(self.timestamps) < self.max_per_minute:
                    self.timestamps.append(now)
                    return
                wait = 60.0 - (now - self.timestamps[0]) + 0.05
            time.sleep(min(max(wait, 0.1), 1.0))


rate_limiter = RateLimiter(20)


def _api_get_with_retry(url, params, headers):
    for attempt in range(3):
        if batch_state["stop_flag"]:
            break
        while batch_state["paused"] and not batch_state["stop_flag"]:
            time.sleep(0.25)
        resp = http_requests.get(url, params=params, headers=headers, timeout=25)
        try:
            j = resp.json()
            rl = j.get("rate_limit") if isinstance(j, dict) else None
            if isinstance(rl, dict) and rl.get("remaining") == 0 and rl.get("retry_after"):
                wait_s = min(max(int(rl.get("retry_after", 2)), 1), 60)
                time.sleep(wait_s)
        except Exception:
            pass
        if resp.status_code == 429:
            retry_s = 5
            try:
                rj = resp.json()
                retry_s = int((rj.get("rate_limit") or {}).get("retry_after") or 5)
            except Exception:
                pass
            time.sleep(min(max(retry_s, 2), 60))
            continue
        return resp
    return resp


def _emit_progress(row_out, job_id, found_inc=False, error_inc=False):
    with batch_lock:
        batch_state["results"].append(row_out)
        batch_state["done"] += 1
        if found_inc:
            batch_state["found"] += 1
        if error_inc:
            batch_state["errors"] += 1
        done_now = batch_state["done"]
        found_now = batch_state["found"]
        errors_now = batch_state["errors"]
        total_now = batch_state["total"]
        evt = {
            "type": "progress",
            "job_id": job_id,
            "done": done_now,
            "total": total_now,
            "found": found_now,
            "errors": errors_now,
            "paused": batch_state["paused"],
            "row": row_out,
        }
    batch_state["queue"].put(evt)


def parse_batch_items(file_path="", text_input="", items_raw=None):
    """Parse batch list from CSV/TXT/JSON file, pasted text, or JSON array.
    Supports format:
      03026T;FRANCISCO FABULO ALMEIDA MENDES;FORTALEZA-CE;Ativo;2a - CE
    """
    raw_lines = []
    if file_path and os.path.isfile(file_path):
        with open(file_path, "r", encoding="utf-8-sig", errors="replace") as f:
            content = f.read().strip()
        if file_path.lower().endswith(".json") or content.startswith("["):
            try:
                arr = json.loads(content)
                if isinstance(arr, list):
                    items_raw = arr
            except Exception:
                raw_lines = content.splitlines()
        else:
            raw_lines = content.splitlines()
    elif text_input:
        content = text_input.strip()
        if content.startswith("["):
            try:
                arr = json.loads(content)
                if isinstance(arr, list):
                    items_raw = arr
            except Exception:
                raw_lines = content.splitlines()
        else:
            raw_lines = content.splitlines()

    parsed = []
    if items_raw and isinstance(items_raw, list):
        for idx, it in enumerate(items_raw, 1):
            if isinstance(it, dict):
                reg = str(it.get("registro") or it.get("id") or idx).strip()
                nome = str(it.get("nome") or it.get("name") or "").strip()
                cid_uf = str(it.get("cidade_uf") or it.get("cidade") or it.get("municipio") or "").strip()
                uf = str(it.get("uf") or it.get("estado") or "").strip().upper()
                cidade = cid_uf
                if "-" in cid_uf:
                    c_p, u_p = cid_uf.rsplit("-", 1)
                    cidade = c_p.strip()
                    if len(u_p.strip()) == 2:
                        uf = u_p.strip().upper()
                elif "/" in cid_uf:
                    c_p, u_p = cid_uf.rsplit("/", 1)
                    cidade = c_p.strip()
                    if len(u_p.strip()) == 2:
                        uf = u_p.strip().upper()
                if nome:
                    parsed.append({
                        "_row_index": int(it.get("_row_index") or idx),
                        "registro": reg,
                        "nome": nome,
                        "cidade": cidade,
                        "uf": uf,
                        "cidade_uf": f"{cidade}-{uf}" if uf and cidade else (cidade or uf),
                        "situacao_lista": str(it.get("situacao_lista") or it.get("situacao") or ""),
                        "regiao_lista": str(it.get("regiao_lista") or it.get("regiao") or ""),
                    })
        return parsed

    for idx, line in enumerate(raw_lines, 1):
        line_str = line.strip()
        if not line_str:
            continue
        delim = ";" if ";" in line_str else ("\t" if "\t" in line_str else ",")
        parts = [p.strip().strip('"') for p in line_str.split(delim)]
        # Skip header row if detected
        if idx == 1 and parts and parts[0].lower() in ("registro", "id", "codigo", "inscricao", "matricula", "nome"):
            if len(parts) > 1 and parts[1].lower() in ("nome", "nome_completo", "cidade", "municipio", "uf"):
                continue
        if len(parts) >= 3:
            registro = parts[0]
            nome = parts[1]
            cidade_uf = parts[2]
            situacao_lista = parts[3] if len(parts) > 3 else ""
            regiao_lista = parts[4] if len(parts) > 4 else ""
        elif len(parts) == 2:
            registro = f"#{idx}"
            nome = parts[0]
            cidade_uf = parts[1]
            situacao_lista = ""
            regiao_lista = ""
        else:
            continue

        if not nome:
            continue

        cidade = cidade_uf
        uf = ""
        if "-" in cidade_uf:
            c_p, u_p = cidade_uf.rsplit("-", 1)
            if len(u_p.strip()) == 2:
                cidade = c_p.strip()
                uf = u_p.strip().upper()
        elif "/" in cidade_uf:
            c_p, u_p = cidade_uf.rsplit("/", 1)
            if len(u_p.strip()) == 2:
                cidade = c_p.strip()
                uf = u_p.strip().upper()

        parsed.append({
            "_row_index": len(parsed) + 1,
            "registro": registro,
            "nome": nome,
            "cidade": cidade,
            "uf": uf,
            "cidade_uf": cidade_uf,
            "situacao_lista": situacao_lista,
            "regiao_lista": regiao_lista,
        })
    return parsed


def batch_nome4_cpf_worker(items_chunk, provider, job_id):
    """Two-step batch per record using CPFBusca:
    Step 1: GET /api/v1/query/nome4?nome=<NOME>&cidade=<CIDADE> -> store all returned data
    Step 2: GET /api/v1/query/cpf?cpf=<CPF> -> extract all phone numbers (and store full response)
    Saves each record immediately to SQLite (BatchResult & BatchJob).
    """
    for item in items_chunk:
        if batch_state["stop_flag"]:
            break
        while batch_state["paused"] and not batch_state["stop_flag"]:
            time.sleep(0.25)
        if batch_state["stop_flag"]:
            break

        nome = item.get("nome", "").strip()
        cidade = item.get("cidade", "").strip()
        uf = item.get("uf", "").strip().upper()
        cidade_uf = item.get("cidade_uf", "") or (f"{cidade}-{uf}" if uf else cidade)
        registro = item.get("registro", "")
        row_index = item.get("_row_index", 0)

        if not cidade and "-" in cidade_uf:
            parts = cidade_uf.rsplit("-", 1)
            cidade = parts[0].strip()
            uf = parts[1].strip().upper()

        rate_limiter.acquire()
        if batch_state["stop_flag"]:
            break

        t0 = time.monotonic()
        try:
            # 1ª Consulta: GET /nome4 (Nome + Cidade)
            url1, headers1 = build_provider_request_config(provider, "/nome4")
            params1 = {"nome": nome, "cidade": cidade}
            resp1 = _api_get_with_retry(url1, params1, headers1)
            ms1 = int((time.monotonic() - t0) * 1000)
            res1_json = resp1.json() if resp1.content else {}
            resp1.raise_for_status()

            inner1 = res1_json.get("data", res1_json) if isinstance(res1_json, dict) else {}
            if isinstance(inner1, dict):
                results_list = (
                    inner1.get("data")
                    if isinstance(inner1.get("data"), list)
                    else inner1.get("results")
                    if isinstance(inner1.get("results"), list)
                    else inner1.get("body")
                    if isinstance(inner1.get("body"), list)
                    else []
                )
            elif isinstance(inner1, list):
                results_list = inner1
            else:
                results_list = []

            if not results_list:
                raw_payload = {
                    "item_lista": item,
                    "primeira_consulta_nome4": res1_json,
                    "segunda_consulta_cpf": None,
                    "telefones": [],
                }
                row_out = {
                    "row_index": row_index,
                    "registro": registro,
                    "nome_busca": nome,
                    "cidade_busca": cidade,
                    "uf_busca": uf,
                    "cidade_uf": cidade_uf,
                    "qtd_resultados": 0,
                    "cpf": "",
                    "nome_encontrado": "",
                    "data_nascimento": "",
                    "nome_mae": "",
                    "sexo": "",
                    "cidade": cidade,
                    "uf_result": uf,
                    "cep": "",
                    "rg": "",
                    "titulo_eleitor": "",
                    "renda": "",
                    "profissao": "",
                    "telefones": "",
                    "status": "SEM_RESULTADOS",
                    "tempo_ms": ms1,
                }
                with app.app_context():
                    br = BatchResult(
                        job_id=job_id, row_index=row_index, registro=registro,
                        nome_busca=nome, cidade_busca=cidade, uf_busca=uf,
                        qtd_resultados=0, status="SEM_RESULTADOS", tempo_ms=ms1,
                        telefones="",
                        raw_data=json.dumps(raw_payload, ensure_ascii=False),
                    )
                    db.session.add(br)
                    job_row = db.session.get(BatchJob, job_id)
                    if job_row:
                        job_row.done = (job_row.done or 0) + 1
                    db.session.commit()
                    row_out["id"] = br.id
                _emit_progress(row_out, job_id, found_inc=False, error_inc=False)
                continue

            best = results_list[0] if isinstance(results_list[0], dict) else {}
            cpf_found = str(best.get("cpf") or "").strip()
            cpf_digits = "".join(c for c in cpf_found if c.isdigit())
            endereco_obj = best.get("endereco") if isinstance(best.get("endereco"), dict) else {}

            # 2ª Consulta: GET /cpf?cpf=<cpf_digits> para capturar os telefones
            res2_json = None
            body2 = {}
            phones_list = []
            if cpf_digits:
                rate_limiter.acquire()
                if batch_state["stop_flag"]:
                    break
                try:
                    url2, headers2 = build_provider_request_config(provider, "/cpf")
                    resp2 = _api_get_with_retry(url2, {"cpf": cpf_digits}, headers2)
                    if resp2.ok:
                        res2_json = resp2.json()
                        d2 = res2_json.get("data", res2_json) if isinstance(res2_json, dict) else {}
                        body2 = d2.get("body", d2) if isinstance(d2, dict) else {}
                        phones_list = extract_phones_from_cpf_body(body2)
                except Exception:
                    pass

            ms_total = int((time.monotonic() - t0) * 1000)
            tel_str = ", ".join(phones_list)

            data_nasc = best.get("dataNascimento") or best.get("nascimento") or best.get("data_nascimento") or body2.get("birth_date") or ""
            if " " in str(data_nasc):
                data_nasc = str(data_nasc).split(" ")[0]
            nome_mae = best.get("nomeMae") or best.get("nome_mae") or body2.get("mother_name") or ""
            sexo = best.get("sexo") or body2.get("gender") or ""
            cid_res = endereco_obj.get("municipio") or best.get("cidade") or cidade
            uf_res = endereco_obj.get("uf") or best.get("uf") or uf
            cep_res = endereco_obj.get("cep") or best.get("cep") or ""
            rg_res = best.get("rg") or body2.get("rg") or ""
            titulo_res = best.get("titulo_eleitor") or body2.get("voter_id") or ""
            renda_res = str(best.get("renda") or body2.get("income") or "")
            profissao_res = best.get("profissao") or body2.get("occupation") or ""

            raw_payload = {
                "item_lista": item,
                "primeira_consulta_nome4": res1_json,
                "registro_nome4": best,
                "segunda_consulta_cpf": res2_json,
                "telefones": phones_list,
                "telefones_formatados": tel_str,
            }

            row_out = {
                "row_index": row_index,
                "registro": registro,
                "nome_busca": nome,
                "cidade_busca": cidade,
                "uf_busca": uf,
                "cidade_uf": cidade_uf,
                "qtd_resultados": len(results_list),
                "cpf": cpf_found,
                "nome_encontrado": best.get("nome") or body2.get("name") or nome,
                "data_nascimento": data_nasc,
                "nome_mae": nome_mae,
                "sexo": sexo,
                "cidade": cid_res,
                "uf_result": uf_res,
                "cep": cep_res,
                "rg": rg_res,
                "titulo_eleitor": titulo_res,
                "renda": renda_res,
                "profissao": profissao_res,
                "telefones": tel_str,
                "total_telefones": len(phones_list),
                "status": "OK",
                "tempo_ms": ms_total,
            }

            with app.app_context():
                br = BatchResult(
                    job_id=job_id,
                    row_index=row_index,
                    registro=registro,
                    nome_busca=nome,
                    cidade_busca=cidade,
                    uf_busca=uf,
                    qtd_resultados=len(results_list),
                    cpf=cpf_found,
                    nome_encontrado=row_out["nome_encontrado"],
                    data_nascimento=data_nasc,
                    nome_mae=nome_mae,
                    sexo=str(sexo)[:20],
                    cidade=cid_res,
                    uf_result=str(uf_res)[:5],
                    cep=str(cep_res)[:15],
                    telefones=tel_str,
                    status="OK",
                    tempo_ms=ms_total,
                    raw_data=json.dumps(raw_payload, ensure_ascii=False),
                )
                db.session.add(br)
                job_row = db.session.get(BatchJob, job_id)
                if job_row:
                    job_row.done = (job_row.done or 0) + 1
                    job_row.found = (job_row.found or 0) + 1
                db.session.commit()
                row_out["id"] = br.id

            _emit_progress(row_out, job_id, found_inc=True, error_inc=False)

        except Exception as e:
            ms = int((time.monotonic() - t0) * 1000)
            err_msg = f"ERRO: {str(e)[:160]}"
            row_out = {
                "row_index": row_index,
                "registro": registro,
                "nome_busca": nome,
                "cidade_busca": cidade,
                "uf_busca": uf,
                "cidade_uf": cidade_uf,
                "qtd_resultados": 0,
                "cpf": "",
                "nome_encontrado": "",
                "telefones": "",
                "status": err_msg,
                "tempo_ms": ms,
            }
            with app.app_context():
                br = BatchResult(
                    job_id=job_id, row_index=row_index, registro=registro,
                    nome_busca=nome, cidade_busca=cidade, uf_busca=uf,
                    qtd_resultados=0, status=err_msg, tempo_ms=ms,
                    raw_data=json.dumps({"item_lista": item, "error": str(e)}, ensure_ascii=False),
                )
                db.session.add(br)
                job_row = db.session.get(BatchJob, job_id)
                if job_row:
                    job_row.done = (job_row.done or 0) + 1
                    job_row.errors = (job_row.errors or 0) + 1
                db.session.commit()
                row_out["id"] = br.id
            _emit_progress(row_out, job_id, found_inc=False, error_inc=True)


def batch_orchestrator(items, provider_id, num_threads, job_id):
    with app.app_context():
        provider = db.session.get(Provider, provider_id)
    if not provider:
        batch_state["running"] = False
        return

    chunks = [[] for _ in range(num_threads)]
    for i, item in enumerate(items):
        chunks[i % num_threads].append(item)

    threads = []
    for chunk in chunks:
        if chunk:
            t = threading.Thread(target=batch_nome4_cpf_worker, args=(chunk, provider, job_id), daemon=True)
            threads.append(t)
            t.start()
    for t in threads:
        t.join()

    with app.app_context():
        job = db.session.get(BatchJob, job_id)
        if job:
            job.finished_at = datetime.utcnow()
            job.done = BatchResult.query.filter_by(job_id=job_id).count()
            job.found = BatchResult.query.filter(BatchResult.job_id == job_id, BatchResult.qtd_resultados > 0).count()
            job.errors = BatchResult.query.filter(BatchResult.job_id == job_id, BatchResult.status.like("ERRO%")).count()
            job.status = "stopped" if batch_state["stop_flag"] else "done"
            db.session.commit()

    batch_state["queue"].put({
        "type": "done",
        "job_id": job_id,
        "status": "stopped" if batch_state["stop_flag"] else "done",
        "total": batch_state["total"],
        "done": batch_state["done"],
        "found": batch_state["found"],
        "errors": batch_state["errors"],
    })
    batch_state["running"] = False
    batch_state["paused"] = False


@app.route("/api/batch/preview", methods=["POST"])
@admin_required
def batch_preview():
    data = request.json or {}
    items = parse_batch_items(
        file_path=data.get("file_path", ""),
        text_input=data.get("text", ""),
        items_raw=data.get("items"),
    )
    start_from = max(1, int(data.get("start_from") or 1))
    end_at = int(data.get("end_at") or data.get("limit") or 0)
    sliced = [it for it in items if it["_row_index"] >= start_from and (end_at <= 0 or it["_row_index"] <= end_at)]
    return jsonify({
        "ok": True,
        "total_in_list": len(items),
        "total_selected": len(sliced),
        "preview": sliced[:25],
    })


@app.route("/api/batch/start", methods=["POST"])
@admin_required
def batch_start():
    if batch_state["running"]:
        return jsonify({"error": "Já existe uma consulta em massa em execução. Pause ou aguarde terminar."}), 400
    user = get_user()
    provider = get_provider("cpfbusca") or get_provider("dataflow")
    if not provider:
        return jsonify({"error": "Provedor CPF Busca não está configurado ou ativo no painel Admin."}), 400

    data = request.json or {}
    file_path = data.get("file_path", "")
    text_input = data.get("text", "")
    items_raw = data.get("items")

    all_items = parse_batch_items(file_path=file_path, text_input=text_input, items_raw=items_raw)
    if not all_items:
        return jsonify({"error": "Nenhum registro válido encontrado na lista enviada."}), 400

    start_from = max(1, int(data.get("start_from") or 1))
    end_at = int(data.get("end_at") or 0)
    limit = int(data.get("limit") or 0)

    if end_at > 0:
        items = [it for it in all_items if start_from <= it["_row_index"] <= end_at]
    else:
        items = [it for it in all_items if it["_row_index"] >= start_from]
        if limit > 0:
            items = items[:limit]

    if not items:
        return jsonify({"error": "Nenhum item no intervalo selecionado (verifique 'A partir de' e 'Até')."}), 400

    num_threads = max(1, min(int(data.get("threads") or 2), 10))
    rpm = max(1, min(int(data.get("rpm") or 20), 300))
    rate_limiter.set_rpm(rpm)

    job = BatchJob(
        user_id=user.id,
        file_path=file_path or "lista_colada",
        total=len(items),
        done=0,
        found=0,
        errors=0,
        threads=num_threads,
        rpm=rpm,
        start_from=start_from,
        end_at=end_at or (items[-1]["_row_index"] if items else 0),
        items_json=json.dumps(items, ensure_ascii=False),
        status="running",
        batch_type="nome4_cpf",
    )
    db.session.add(job)
    db.session.commit()

    with batch_lock:
        batch_state.update({
            "running": True,
            "paused": False,
            "stop_flag": False,
            "total": len(items),
            "done": 0,
            "found": 0,
            "errors": 0,
            "skipped": 0,
            "threads": num_threads,
            "rpm": rpm,
            "items": items,
            "results": [],
            "job_id": job.id,
            "batch_type": "nome4_cpf",
        })
        while not batch_state["queue"].empty():
            try:
                batch_state["queue"].get_nowait()
            except queue.Empty:
                break

    t = threading.Thread(
        target=batch_orchestrator,
        args=(items, provider.id, num_threads, job.id),
        daemon=True,
    )
    t.start()
    return jsonify({"ok": True, "total": len(items), "job_id": job.id})


@app.route("/api/batch/pause", methods=["POST"])
@admin_required
def batch_pause():
    if not batch_state["running"]:
        return jsonify({"error": "Nenhum lote em execução"}), 400
    batch_state["paused"] = True
    if batch_state["job_id"]:
        job = db.session.get(BatchJob, batch_state["job_id"])
        if job:
            job.status = "paused"
            db.session.commit()
    return jsonify({"ok": True, "paused": True, "job_id": batch_state["job_id"]})


@app.route("/api/batch/resume", methods=["POST"])
@admin_required
def batch_resume_active():
    if batch_state["running"] and batch_state["paused"]:
        batch_state["paused"] = False
        if batch_state["job_id"]:
            job = db.session.get(BatchJob, batch_state["job_id"])
            if job:
                job.status = "running"
                db.session.commit()
        return jsonify({"ok": True, "paused": False, "job_id": batch_state["job_id"]})
    data = request.get_json(silent=True) or {}
    jid = data.get("job_id") or batch_state.get("job_id")
    if jid:
        return batch_job_resume(int(jid))
    return jsonify({"error": "Nenhum lote pausado para continuar"}), 400


@app.route("/api/batch/stop", methods=["POST"])
@admin_required
def batch_stop():
    batch_state["stop_flag"] = True
    batch_state["paused"] = False
    if batch_state["job_id"]:
        job = db.session.get(BatchJob, batch_state["job_id"])
        if job:
            job.status = "stopped"
            db.session.commit()
    return jsonify({"ok": True})


@app.route("/api/batch/status")
@admin_required
def batch_status():
    return jsonify({
        "running": batch_state["running"],
        "paused": batch_state["paused"],
        "total": batch_state["total"],
        "done": batch_state["done"],
        "found": batch_state["found"],
        "errors": batch_state["errors"],
        "job_id": batch_state["job_id"],
        "rpm": batch_state["rpm"],
        "threads": batch_state["threads"],
    })


@app.route("/api/batch/stream")
@admin_required
def batch_stream():
    def generate():
        while batch_state["running"] or not batch_state["queue"].empty():
            try:
                msg = batch_state["queue"].get(timeout=0.5)
                yield f"data: {json.dumps(msg, ensure_ascii=False)}\n\n"
                if msg.get("type") == "done":
                    return
            except queue.Empty:
                yield f"data: {json.dumps({'type': 'heartbeat', 'paused': batch_state['paused']})}\n\n"
        yield f"data: {json.dumps({'type': 'done', 'total': batch_state['done'], 'found': batch_state['found'], 'errors': batch_state['errors']})}\n\n"
    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/batch/export")
@admin_required
def batch_export():
    jid = request.args.get("job_id") or batch_state.get("job_id")
    if jid:
        return batch_job_export(int(jid))
    results = batch_state["results"]
    if not results:
        return jsonify({"error": "Sem resultados"}), 400
    cols = [
        "row_index", "registro", "nome_busca", "cidade_busca", "uf_busca",
        "qtd_resultados", "cpf", "nome_encontrado", "data_nascimento",
        "nome_mae", "sexo", "cidade", "uf_result", "cep", "rg",
        "titulo_eleitor", "renda", "profissao", "telefones", "status", "tempo_ms",
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=cols, delimiter=";", extrasaction="ignore")
    writer.writeheader()
    for r in results:
        writer.writerow(r)
    mem = io.BytesIO()
    mem.write(b"\xef\xbb\xbf")
    mem.write(buf.getvalue().encode("utf-8"))
    mem.seek(0)
    return send_file(mem, mimetype="text/csv", as_attachment=True,
                     download_name=f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")


@app.route("/api/batch/jobs")
@admin_required
def batch_jobs():
    jobs = BatchJob.query.order_by(BatchJob.id.desc()).limit(50).all()
    return jsonify([{
        "id": j.id,
        "batch_type": j.batch_type or "nome4_cpf",
        "started_at": j.started_at.isoformat() if j.started_at else "",
        "finished_at": j.finished_at.isoformat() if j.finished_at else None,
        "total": j.total,
        "done": j.done,
        "found": j.found,
        "errors": j.errors,
        "threads": j.threads,
        "rpm": j.rpm,
        "start_from": getattr(j, "start_from", 1) or 1,
        "end_at": getattr(j, "end_at", 0) or 0,
        "status": "paused" if (batch_state["running"] and batch_state["paused"] and batch_state["job_id"] == j.id) else ("running" if (batch_state["running"] and batch_state["job_id"] == j.id) else j.status),
    } for j in jobs])


@app.route("/api/batch/jobs/<int:job_id>/resume", methods=["POST"])
@admin_required
def batch_job_resume(job_id):
    if batch_state["running"]:
        if batch_state["job_id"] == job_id and batch_state["paused"]:
            batch_state["paused"] = False
            job = db.session.get(BatchJob, job_id)
            if job:
                job.status = "running"
                db.session.commit()
            return jsonify({"ok": True, "resumed_in_place": True, "job_id": job_id})
        return jsonify({"error": "Já existe outro lote em execução."}), 400

    job = db.session.get(BatchJob, job_id)
    if not job:
        return jsonify({"error": "Lote não encontrado"}), 404

    provider = get_provider("cpfbusca") or get_provider("dataflow")
    if not provider:
        return jsonify({"error": "Provedor CPF Busca não configurado ou inativo."}), 400

    try:
        all_job_items = json.loads(job.items_json or "[]")
    except Exception:
        all_job_items = []

    if not all_job_items and job.file_path and os.path.isfile(job.file_path):
        all_job_items = parse_batch_items(file_path=job.file_path)

    processed_indices = {
        r.row_index for r in BatchResult.query.filter_by(job_id=job_id).all()
        if not (r.status or "").startswith("ERRO")
    }
    # Remove old error rows that will be retried
    BatchResult.query.filter(BatchResult.job_id == job_id, BatchResult.status.like("ERRO%")).delete()
    db.session.commit()

    remaining = [it for it in all_job_items if it.get("_row_index") not in processed_indices]
    if not remaining:
        job.status = "done"
        db.session.commit()
        return jsonify({"error": "Todos os itens deste lote já foram processados."}), 400

    data = request.get_json(silent=True) or {}
    rpm = int(data.get("rpm") or job.rpm or 20)
    num_threads = int(data.get("threads") or job.threads or 2)
    rate_limiter.set_rpm(rpm)

    done_count = BatchResult.query.filter_by(job_id=job_id).count()
    found_count = BatchResult.query.filter(BatchResult.job_id == job_id, BatchResult.qtd_resultados > 0).count()

    job.status = "running"
    job.rpm = rpm
    job.threads = num_threads
    job.done = done_count
    job.found = found_count
    job.errors = 0
    db.session.commit()

    with batch_lock:
        batch_state.update({
            "running": True,
            "paused": False,
            "stop_flag": False,
            "total": job.total,
            "done": done_count,
            "found": found_count,
            "errors": 0,
            "skipped": 0,
            "threads": num_threads,
            "rpm": rpm,
            "items": remaining,
            "results": [],
            "job_id": job.id,
            "batch_type": "nome4_cpf",
        })
        while not batch_state["queue"].empty():
            try:
                batch_state["queue"].get_nowait()
            except queue.Empty:
                break

    t = threading.Thread(
        target=batch_orchestrator,
        args=(remaining, provider.id, num_threads, job.id),
        daemon=True,
    )
    t.start()
    return jsonify({"ok": True, "job_id": job.id, "remaining": len(remaining), "total": job.total})


@app.route("/api/batch/jobs/<int:job_id>/results")
@admin_required
def batch_job_results(job_id):
    job = db.session.get(BatchJob, job_id)
    if not job:
        return jsonify({"error": "Job não encontrado"}), 404
    filt = request.args.get("filter", "all")
    search = (request.args.get("search") or "").strip()
    page = max(1, int(request.args.get("page", 1)))
    per_page = max(10, min(int(request.args.get("per_page", 50)), 500))

    q = BatchResult.query.filter_by(job_id=job_id)
    if filt == "with_phone":
        q = q.filter(BatchResult.telefones.isnot(None), BatchResult.telefones != "")
    elif filt == "ok":
        q = q.filter(BatchResult.qtd_resultados > 0)
    elif filt == "empty":
        q = q.filter(BatchResult.status == "SEM_RESULTADOS")
    elif filt == "error":
        q = q.filter(BatchResult.status.like("ERRO%"))

    if search:
        like_q = f"%{search}%"
        q = q.filter(
            (BatchResult.nome_busca.ilike(like_q))
            | (BatchResult.cpf.ilike(like_q))
            | (BatchResult.registro.ilike(like_q))
            | (BatchResult.cidade.ilike(like_q))
            | (BatchResult.telefones.ilike(like_q))
        )

    total_filtered = q.count()
    total_pages = max(1, (total_filtered + per_page - 1) // per_page)
    if page > total_pages:
        page = total_pages
    results = q.order_by(BatchResult.row_index.asc()).offset((page - 1) * per_page).limit(per_page).all()

    with_phone_count = BatchResult.query.filter(
        BatchResult.job_id == job_id,
        BatchResult.telefones.isnot(None),
        BatchResult.telefones != "",
    ).count()

    out_rows = []
    for r in results:
        tels = getattr(r, "telefones", "") or ""
        rec1 = {}
        if r.raw_data:
            try:
                rd = json.loads(r.raw_data)
                rec1 = rd.get("registro_nome4") or {}
                if not tels:
                    tels = rd.get("telefones_formatados") or (", ".join(rd.get("telefones", [])) if isinstance(rd.get("telefones"), list) else str(rd.get("telefones") or ""))
            except Exception:
                pass
        out_rows.append({
            "id": r.id,
            "row_index": r.row_index,
            "registro": r.registro or "",
            "nome_busca": r.nome_busca or "",
            "cidade_busca": getattr(r, "cidade_busca", "") or r.cidade or "",
            "uf_busca": r.uf_busca or "",
            "cpf": r.cpf or "",
            "nome_encontrado": r.nome_encontrado or "",
            "data_nascimento": r.data_nascimento or "",
            "nome_mae": r.nome_mae or "",
            "sexo": r.sexo or "",
            "cidade": r.cidade or "",
            "uf_result": r.uf_result or "",
            "cep": r.cep or "",
            "rg": rec1.get("rg") or "",
            "renda": rec1.get("renda") or "",
            "profissao": rec1.get("profissao") or "",
            "telefones": tels,
            "status": r.status or "",
            "tempo_ms": r.tempo_ms or 0,
        })

    return jsonify({
        "job": {
            "id": job.id,
            "total": job.total,
            "done": job.done,
            "found": job.found,
            "with_phone": with_phone_count,
            "errors": job.errors,
            "status": job.status,
            "rpm": job.rpm,
            "threads": job.threads,
            "start_from": getattr(job, "start_from", 1) or 1,
            "end_at": getattr(job, "end_at", 0) or 0,
        },
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total": total_filtered,
            "total_pages": total_pages,
        },
        "results": out_rows,
    })


@app.route("/api/batch/results/<int:result_id>/detail")
@admin_required
def batch_result_detail(result_id):
    r = db.session.get(BatchResult, result_id)
    if not r:
        return jsonify({"error": "Resultado não encontrado"}), 404
    raw = {}
    if r.raw_data:
        try:
            raw = json.loads(r.raw_data)
        except Exception:
            raw = {"raw_text": r.raw_data}
    return jsonify({
        "id": r.id,
        "job_id": r.job_id,
        "row_index": r.row_index,
        "registro": r.registro,
        "nome_busca": r.nome_busca,
        "cidade_busca": getattr(r, "cidade_busca", "") or "",
        "uf_busca": r.uf_busca,
        "cpf": r.cpf,
        "nome_encontrado": r.nome_encontrado,
        "data_nascimento": r.data_nascimento,
        "nome_mae": r.nome_mae,
        "sexo": r.sexo,
        "cidade": r.cidade,
        "uf_result": r.uf_result,
        "cep": r.cep,
        "telefones": getattr(r, "telefones", "") or "",
        "status": r.status,
        "tempo_ms": r.tempo_ms,
        "raw_data": raw,
    })


@app.route("/api/batch/jobs/<int:job_id>", methods=["DELETE"])
@admin_required
def batch_job_delete(job_id):
    job = db.session.get(BatchJob, job_id)
    if not job:
        return jsonify({"error": "Job não encontrado"}), 404
    BatchResult.query.filter_by(job_id=job_id).delete()
    db.session.delete(job)
    db.session.commit()
    return jsonify({"ok": True})


ALL_BATCH_EXPORT_COLUMNS = [
    "item", "registro", "nome_busca", "cidade_busca", "uf_busca",
    "status", "cpf", "nome_encontrado", "telefones", "data_nascimento",
    "sexo", "nome_mae", "rg", "titulo_eleitor", "renda", "cbo",
    "profissao", "logradouro", "numero", "complemento", "bairro",
    "cidade", "uf", "cep", "tempo_ms",
]


@app.route("/api/batch/jobs/<int:job_id>/export")
@admin_required
def batch_job_export(job_id):
    job = db.session.get(BatchJob, job_id)
    if not job:
        return jsonify({"error": "Job não encontrado"}), 404
    filt = request.args.get("filter", "all")
    fmt = request.args.get("format", "csv").lower()
    cols_param = (request.args.get("columns") or "").strip()

    q = BatchResult.query.filter_by(job_id=job_id)
    if filt == "with_phone":
        q = q.filter(BatchResult.telefones.isnot(None), BatchResult.telefones != "")
    elif filt == "ok":
        q = q.filter(BatchResult.qtd_resultados > 0)
    elif filt == "empty":
        q = q.filter(BatchResult.status == "SEM_RESULTADOS")
    elif filt == "error":
        q = q.filter(BatchResult.status.like("ERRO%"))

    results = q.order_by(BatchResult.row_index.asc()).all()
    if not results:
        return jsonify({"error": "Sem resultados para exportar com o filtro selecionado"}), 400

    if cols_param:
        selected_cols = [c.strip() for c in cols_param.split(",") if c.strip() in ALL_BATCH_EXPORT_COLUMNS]
        if not selected_cols:
            selected_cols = list(ALL_BATCH_EXPORT_COLUMNS)
    else:
        selected_cols = list(ALL_BATCH_EXPORT_COLUMNS)

    rows_export = []
    for r in results:
        raw = {}
        if r.raw_data:
            try:
                raw = json.loads(r.raw_data)
            except Exception:
                pass
        rec1 = raw.get("registro_nome4") or {}
        end1 = rec1.get("endereco") if isinstance(rec1.get("endereco"), dict) else {}
        tels = getattr(r, "telefones", "") or raw.get("telefones_formatados") or ""
        if not tels and isinstance(raw.get("telefones"), list):
            tels = ", ".join(raw["telefones"])

        if filt == "with_phone" and not str(tels).strip():
            continue

        full_row = {
            "item": r.row_index,
            "registro": r.registro or "",
            "nome_busca": r.nome_busca or "",
            "cidade_busca": getattr(r, "cidade_busca", "") or "",
            "uf_busca": r.uf_busca or "",
            "status": r.status or "",
            "cpf": r.cpf or "",
            "nome_encontrado": r.nome_encontrado or "",
            "telefones": tels,
            "data_nascimento": r.data_nascimento or "",
            "sexo": r.sexo or "",
            "nome_mae": r.nome_mae or "",
            "rg": rec1.get("rg") or "",
            "titulo_eleitor": rec1.get("titulo_eleitor") or "",
            "renda": rec1.get("renda") or "",
            "cbo": rec1.get("cbo") or "",
            "profissao": rec1.get("profissao") or "",
            "logradouro": end1.get("logradouro") or "",
            "numero": end1.get("logradouroNumero") or "",
            "complemento": end1.get("complemento") or "",
            "bairro": end1.get("bairro") or "",
            "cidade": r.cidade or end1.get("municipio") or "",
            "uf": r.uf_result or end1.get("uf") or "",
            "cep": r.cep or end1.get("cep") or "",
            "tempo_ms": r.tempo_ms or 0,
        }
        filtered_row = {c: full_row[c] for c in selected_cols}
        rows_export.append(filtered_row)

    if not rows_export:
        return jsonify({"error": "Nenhum registro encontrado para os critérios de exportação"}), 400

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if fmt == "json":
        mem = io.BytesIO()
        mem.write(json.dumps(rows_export, ensure_ascii=False, indent=2).encode("utf-8"))
        mem.seek(0)
        return send_file(mem, mimetype="application/json", as_attachment=True,
                         download_name=f"lote_{job_id}_{ts}.json")

    if fmt == "txt":
        lines = []
        if len(selected_cols) > 1:
            lines.append(";".join(selected_cols))
        for row in rows_export:
            lines.append(";".join(str(row.get(c, "") or "").replace("\n", " ").replace("\r", " ") for c in selected_cols))
        mem = io.BytesIO()
        mem.write("\r\n".join(lines).encode("utf-8"))
        mem.seek(0)
        return send_file(mem, mimetype="text/plain; charset=utf-8", as_attachment=True,
                         download_name=f"lote_{job_id}_{ts}.txt")

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=selected_cols, delimiter=";", extrasaction="ignore")
    writer.writeheader()
    for row in rows_export:
        writer.writerow(row)
    mem = io.BytesIO()
    mem.write(b"\xef\xbb\xbf")
    mem.write(buf.getvalue().encode("utf-8"))
    mem.seek(0)
    return send_file(mem, mimetype="text/csv", as_attachment=True,
                     download_name=f"lote_{job_id}_{ts}.csv")


# ── API Documentation, Playground, and Modules Pages ───────────────

@app.route("/docs")
@login_required
def api_docs_page():
    user = get_user()
    user_modules = get_user_modules(user)
    return render_template("api_docs.html", user=user, modules=user_modules, categories=MODULE_CATEGORIES)


@app.route("/api-playground")
@login_required
def api_playground_page():
    user = get_user()
    if not user_has_api(user):
        return redirect(url_for("user_dashboard"))
    user_modules = get_user_modules(user)
    keys = UserApiKey.query.filter_by(user_id=user.id, is_active=True).order_by(UserApiKey.id.desc()).all()
    masked_keys = [{
        "id": k.id,
        "label": k.label or f"Key #{k.id}",
        "masked": k.key[:max(1, len(k.key) // 5)] + "•" * (len(k.key) - max(1, len(k.key) // 5)),
    } for k in keys]
    return render_template("api_playground.html", user=user, modules=user_modules, categories=MODULE_CATEGORIES, keys=masked_keys)


@app.route("/api-modulos")
@login_required
def api_modulos_page():
    user = get_user()
    if not user_has_api(user):
        return redirect(url_for("user_dashboard"))
    user_modules = get_user_modules(user)
    return render_template("api_modulos.html", user=user, modules=user_modules, categories=MODULE_CATEGORIES)


# ── Direct CNPJ + Titulares Routes ─────────────────────────────────

@app.route("/api/consulta/cnpj-titulares", methods=["POST"])
@login_required
def cnpj_titulares():
    user = get_user()
    if not debit_credit(user):
        return jsonify({"error": "Limite diário atingido"}), 429
    data = request.json or {}
    cnpj_raw = data.get("cnpj") or data.get("valor") or ""
    res_tuple, err = execute_cnpj_titulares_lookup(cnpj_raw)
    if err:
        return jsonify({"error": err[0]}), err[1]
    enriched_cnpj, socios_enriched, ms = res_tuple
    log = QueryLog(user_id=user.id, endpoint="/cnpj-titulares", module="cnpj_titulares",
                   query_param=enriched_cnpj.get("cnpj", ""), provider_slug="minhareceita",
                   result_count=len(socios_enriched), status_code=200, response_time_ms=ms)
    db.session.add(log)
    db.session.commit()
    return jsonify({
        "success": True,
        "data": enriched_cnpj,
        "meta": {"time_ms": ms, "module": "cnpj_titulares"},
    })


@app.route("/api/v1/query/cnpj-titulares", methods=["POST", "OPTIONS"])
def external_cnpj_titulares():
    if request.method == "OPTIONS":
        return "", 204
    user = api_key_auth()
    if not user:
        return jsonify({"error": "API key inválida", "code": "UNAUTHORIZED"}), 401
    if not debit_credit(user):
        return jsonify({"error": "Limite diário atingido", "code": "RATE_LIMITED"}), 429
    data = request.get_json(silent=True) or {}
    cnpj_raw = data.get("cnpj") or data.get("valor") or ""
    res_tuple, err = execute_cnpj_titulares_lookup(cnpj_raw)
    if err:
        return jsonify({"error": err[0], "code": "MISSING_PARAMS" if err[1] == 400 else "SERVICE_UNAVAILABLE"}), err[1]
    enriched_cnpj, socios_enriched, ms = res_tuple
    log = QueryLog(user_id=user.id, endpoint="/cnpj-titulares", module="cnpj_titulares",
                   query_param=enriched_cnpj.get("cnpj", ""), provider_slug="minhareceita",
                   result_count=len(socios_enriched), status_code=200, response_time_ms=ms)
    db.session.add(log)
    db.session.commit()
    return jsonify({
        "success": True,
        "data": enriched_cnpj,
        "meta": {"time_ms": ms, "module": "cnpj_titulares"},
    })


# ── Catch-all & Health ─────────────────────────────────────────────

@app.route("/health")
def health_check():
    return "ok", 200


@app.errorhandler(404)
def not_found(e):
    if request.is_json or request.path.startswith("/api/"):
        return jsonify({"error": "Não encontrado"}), 404
    return redirect(url_for("login_page"))


if __name__ == "__main__":
    print("\n  Sistema de Consultas: http://localhost:5001\n")
    print("  Login padrão: admin / admin\n")
    app.run(debug=False, port=5001, threaded=True)
