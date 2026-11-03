import threading
import queue
import json
import time
import csv
import io
from datetime import datetime

from flask import Flask, render_template, render_template_string, Response, request, jsonify, send_file
from flask_sqlalchemy import SQLAlchemy

from scraper import CRTRScraper, REGIOES

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///crtr.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)


class Registro(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    numero_registro = db.Column(db.String(20), index=True)
    nome_razao_social = db.Column(db.String(500))
    cidade = db.Column(db.String(200))
    situacao = db.Column(db.String(50))
    regiao = db.Column(db.String(5), index=True)
    consulta_id = db.Column(db.Integer, db.ForeignKey("consulta.id"))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    __table_args__ = (db.UniqueConstraint("numero_registro", "regiao", name="uq_reg_regiao"),)


class Consulta(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    regiao = db.Column(db.String(5))
    status = db.Column(db.String(20), default="rodando")
    total_encontrados = db.Column(db.Integer, default=0)
    total_novos = db.Column(db.Integer, default=0)
    prefixo_atual = db.Column(db.String(10), default="")
    started_at = db.Column(db.DateTime, default=datetime.utcnow)
    finished_at = db.Column(db.DateTime)


class ConsultaResultado(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    consulta_id = db.Column(db.Integer, db.ForeignKey("consulta.id"), nullable=False, index=True)
    numero_registro = db.Column(db.String(20))
    payload = db.Column(db.Text, nullable=False)
    __table_args__ = (db.UniqueConstraint("consulta_id", "numero_registro"),)


with app.app_context():
    db.create_all()

workers = {}


def worker_fn(regiao_id, consulta_id):
    w = workers[regiao_id]
    q = w["queue"]
    seen = set()
    total_novos = 0
    reg_info = REGIOES[regiao_id]
    label = reg_info["nome"]

    def emit(data):
        q.put(data)

    def save_rows(rows):
        nonlocal total_novos
        new_rows = []
        for r in rows:
            key = r["numero_registro"]
            if key in seen:
                continue
            seen.add(key)
            new_rows.append(r)
        if new_rows:
            with app.app_context():
                for r in new_rows:
                    snapshot = dict(r, regiao=regiao_id)
                    db.session.add(ConsultaResultado(consulta_id=consulta_id,
                        numero_registro=r["numero_registro"], payload=json.dumps(snapshot, ensure_ascii=False)))
                    if not Registro.query.filter_by(numero_registro=r["numero_registro"], regiao=regiao_id).first():
                        total_novos += 1
                        db.session.add(Registro(
                            numero_registro=r["numero_registro"],
                            nome_razao_social=r["nome_razao_social"],
                            cidade=r["cidade"],
                            situacao=r["situacao"],
                            regiao=regiao_id,
                            consulta_id=consulta_id,
                        ))
                db.session.commit()
                c = db.session.get(Consulta, consulta_id)
                if c:
                    c.total_novos = total_novos
                    c.total_encontrados = len(seen)
                    db.session.commit()
        return new_rows

    try:
        scraper = CRTRScraper(reg_info["url"])
        scraper.init_session()
        emit({"type": "log", "msg": f"[{label}] Sessao iniciada"})

        for base in range(0, 1000):
            if not w["running"]:
                break

            prefix = f"{base:02d}"
            search_term = prefix + " "
            emit({"type": "progress", "regiao": regiao_id, "prefix": prefix})

            with app.app_context():
                c = db.session.get(Consulta, consulta_id)
                if c:
                    c.prefixo_atual = prefix
                    db.session.commit()

            try:
                rows = scraper.search(numero_registro=search_term)
            except Exception as e:
                emit({"type": "log", "msg": f"[{label}] Erro em {prefix}: {e}"})
                time.sleep(1)
                continue

            new = save_rows(rows)
            emit({
                "type": "batch", "regiao": regiao_id, "query": prefix,
                "found": len(rows), "new": len(new),
                "total": len(seen), "rows": new,
            })

            if rows:
                for sub in range(1, 10000):
                    if not w["running"]:
                        break
                    sub_prefix = f"{prefix}{sub}"
                    if len(sub_prefix) > 6:
                        break
                    sub_search = sub_prefix + " "
                    emit({"type": "progress", "regiao": regiao_id, "prefix": sub_prefix})
                    try:
                        sub_rows = scraper.search(numero_registro=sub_search)
                    except Exception as e:
                        emit({"type": "log", "msg": f"[{label}] Erro em {sub_prefix}: {e}"})
                        time.sleep(1)
                        continue
                    new_sub = save_rows(sub_rows)
                    if sub_rows:
                        emit({
                            "type": "batch", "regiao": regiao_id, "query": sub_prefix,
                            "found": len(sub_rows), "new": len(new_sub),
                            "total": len(seen), "rows": new_sub,
                        })
                    else:
                        break
                    time.sleep(0.3)

            time.sleep(0.3)

        with app.app_context():
            c = db.session.get(Consulta, consulta_id)
            if c:
                c.status = "concluido" if w["running"] else "interrompido"
                c.finished_at = datetime.utcnow()
                c.total_encontrados = len(seen)
                c.total_novos = total_novos
                db.session.commit()
        emit({"type": "done", "regiao": regiao_id, "total": len(seen)})

    except Exception as e:
        emit({"type": "log", "msg": f"[{label}] Erro fatal: {e}"})
        with app.app_context():
            c = db.session.get(Consulta, consulta_id)
            if c:
                c.status = "erro"
                c.finished_at = datetime.utcnow()
                db.session.commit()
    finally:
        w["running"] = False
        q.put(None)


@app.route("/")
def index():
    return render_template("dashboard.html", regioes=REGIOES)


@app.route("/api/start", methods=["POST"])
def start_search():
    data = request.json or {}
    if data.get("consulta_id") is not None:
        try:
            return consulta_detail(int(data["consulta_id"]))
        except (ValueError, TypeError):
            return jsonify({"error": "consulta_id invalido"}), 400
    ids = data.get("regioes", [])
    started = []
    for rid in ids:
        rid = str(rid).strip()
        if rid not in REGIOES:
            continue
        if rid in workers and workers[rid]["running"]:
            continue
        with app.app_context():
            c = Consulta(regiao=rid, status="rodando")
            db.session.add(c)
            db.session.commit()
            cid = c.id
        w = {"thread": None, "queue": queue.Queue(), "running": True, "consulta_id": cid}
        workers[rid] = w
        t = threading.Thread(target=worker_fn, args=(rid, cid), daemon=True)
        w["thread"] = t
        t.start()
        started.append(rid)
    return jsonify({"started": started})


@app.route("/api/stop", methods=["POST"])
def stop_search():
    rid = request.json.get("regiao", "")
    if rid == "ALL":
        for u in workers:
            workers[u]["running"] = False
        return jsonify({"stopped": list(workers.keys())})
    if rid in workers:
        workers[rid]["running"] = False
    return jsonify({"ok": True})


@app.route("/api/stream")
def stream():
    def generate():
        active = {k: w["queue"] for k, w in workers.items() if w["running"]}
        if not active:
            yield f"data: {json.dumps({'type':'end'})}\n\n"
            return
        finished = set()
        while finished != set(active.keys()):
            for k, q in active.items():
                if k in finished:
                    continue
                try:
                    msg = q.get(timeout=0.1)
                    if msg is None:
                        finished.add(k)
                        continue
                    yield f"data: {json.dumps(msg, ensure_ascii=False)}\n\n"
                except queue.Empty:
                    pass
            if not any(workers.get(k, {}).get("running") for k in active):
                for k in active:
                    if k not in finished:
                        try:
                            while True:
                                msg = active[k].get_nowait()
                                if msg is None:
                                    break
                                yield f"data: {json.dumps(msg, ensure_ascii=False)}\n\n"
                        except queue.Empty:
                            pass
                        finished.add(k)
        yield f"data: {json.dumps({'type':'end'})}\n\n"

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/registros")
def api_registros():
    if request.args.get("consulta_id") is not None:
        cid = request.args.get("consulta_id", type=int)
        if cid is None:
            return jsonify({"error": "consulta_id invalido"}), 400
        if not db.session.get(Consulta, cid):
            return jsonify({"error": "Consulta nao encontrada"}), 404
        return jsonify([json.loads(r.payload) for r in ConsultaResultado.query.filter_by(consulta_id=cid).all()])
    rid = request.args.get("regiao", "")
    q = Registro.query
    if rid:
        q = q.filter_by(regiao=rid)
    rows = q.order_by(Registro.numero_registro).all()
    return jsonify([{"id": r.id, "numero_registro": r.numero_registro,
                     "nome_razao_social": r.nome_razao_social, "cidade": r.cidade,
                     "situacao": r.situacao, "regiao": r.regiao} for r in rows])


@app.route("/api/stats")
def api_stats():
    total = Registro.query.count()
    by_reg = db.session.query(Registro.regiao, db.func.count(Registro.id)).group_by(Registro.regiao).all()
    consultas = Consulta.query.order_by(Consulta.id.desc()).limit(50).all()
    return jsonify({
        "total": total,
        "by_regiao": {r: cnt for r, cnt in by_reg},
        "consultas": [{"id": c.id, "regiao": c.regiao, "status": c.status,
                        "total": c.total_encontrados, "novos": c.total_novos,
                        "prefixo": c.prefixo_atual,
                        "inicio": c.started_at.isoformat() if c.started_at else "",
                        "fim": c.finished_at.isoformat() if c.finished_at else ""} for c in consultas],
    })


@app.route("/api/export")
def export():
    rid = request.args.get("regiao", "")
    q = Registro.query
    if rid:
        q = q.filter_by(regiao=rid)
    rows = q.order_by(Registro.regiao, Registro.numero_registro).all()
    if not rows:
        return jsonify({"error": "Sem dados"}), 400
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";")
    w.writerow(["Numero Registro", "Nome/Razao Social", "Cidade", "Situacao", "Regiao"])
    for r in rows:
        nome = REGIOES.get(r.regiao, {}).get("nome", r.regiao)
        w.writerow([r.numero_registro, r.nome_razao_social, r.cidade, r.situacao, nome])
    mem = io.BytesIO()
    mem.write(b"\xef\xbb\xbf")
    mem.write(buf.getvalue().encode("utf-8"))
    mem.seek(0)
    fname = f"crtr_{rid}.csv" if rid else "crtr_todos.csv"
    return send_file(mem, mimetype="text/csv", as_attachment=True, download_name=fname)


@app.route("/api/delete", methods=["POST"])
def delete_registros():
    rid = request.json.get("regiao", "")
    if rid:
        Registro.query.filter_by(regiao=rid).delete()
    else:
        Registro.query.delete()
    db.session.commit()
    return jsonify({"ok": True})


HISTORY_HTML = '<!doctype html><html lang="pt-BR"><meta charset="utf-8"><title>Historico de consultas</title><style>body{font:16px system-ui;margin:32px;max-width:1100px}button{padding:12px;text-align:left;cursor:pointer;width:100%;margin:4px 0}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#f1f5f9;padding:20px}</style><a href="/">Voltar</a><h1>Historico de consultas</h1><p>Os resultados abaixo sao lidos do banco. Abrir uma consulta nao executa uma nova busca.</p>{% for e in entries %}<button data-url="{{e.url}}" onclick="openSaved(this.dataset.url)">#{{e.id}} · {{e.label}}</button>{% else %}<p>Nenhuma consulta salva.</p>{% endfor %}<pre id="result">Selecione uma consulta.</pre><script>async function openSaved(url){const box=document.getElementById(\'result\');box.textContent=\'Carregando...\';try{const r=await fetch(url);const d=await r.json();box.textContent=JSON.stringify(d,null,2)}catch(e){box.textContent=\'Nao foi possivel abrir o resultado salvo.\'}}</script></html>'

@app.route("/api/consultas/<int:consulta_id>")
def consulta_detail(consulta_id):
    c = db.session.get(Consulta, consulta_id)
    if not c:
        return jsonify({"error": "Consulta nao encontrada"}), 404
    rows = [json.loads(r.payload) for r in ConsultaResultado.query.filter_by(consulta_id=c.id).order_by(ConsultaResultado.id).all()]
    return jsonify({"id": c.id, "regiao": c.regiao, "status": c.status,
                    "total": len(rows), "rows": rows, "from_history": True,
                    "aviso": "Historicos anteriores a atualizacao podem nao ter resultados completos." if not rows and c.total_encontrados else None})

@app.route("/historico")
def history_page():
    entries = [{"id": c.id, "label": f"{c.started_at} | Regiao {c.regiao} | {c.status}",
                "url": f"/api/consultas/{c.id}"}
               for c in Consulta.query.order_by(Consulta.id.desc()).limit(200).all()]
    return render_template_string(HISTORY_HTML, entries=entries)


if __name__ == "__main__":
    print("\n  CRTR Dashboard: http://localhost:5000\n")
    app.run(debug=False, port=5000, threaded=True)
