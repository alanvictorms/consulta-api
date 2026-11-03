import requests
import re
import csv
import time
import urllib.parse
from bs4 import BeautifulSoup

REGIOES = {
    "1":  {"nome": "1a - DF",       "url": "https://studiosti.com.br/STI-Financeiro/Radiologia/CRTR1/Telas/Cadastro/Profissional_Empresa_Listar_TL.aspx"},
    "2":  {"nome": "2a - CE",       "url": "https://www.studiosti.com.br/STI-Financeiro/Radiologia/CRTR2/Telas/Cadastro/Profissional_Empresa_Listar_TL.aspx"},
    "3":  {"nome": "3a - MG",       "url": "https://www.studiosti.com.br/STI-Financeiro/Radiologia/CRTR3/Telas/Cadastro/Profissional_Empresa_Listar_TL.aspx"},
    "4":  {"nome": "4a - RJ",       "url": "https://www.studiosti.com.br/STI-Financeiro/Radiologia/CRTR4/Telas/Cadastro/Profissional_Empresa_Listar_TL.aspx"},
    "5":  {"nome": "5a - SP",       "url": "https://www.studiosti.com.br/STI-Financeiro/Radiologia/CRTR5/Telas/Cadastro/Profissional_Empresa_Listar_TL.aspx"},
    "6":  {"nome": "6a - RS",       "url": "https://www.studiosti.com.br/STI-Financeiro/Radiologia/CRTR6/Telas/Cadastro/Profissional_Empresa_Listar_TL.aspx"},
    "8":  {"nome": "8a - BA",       "url": "https://www.studiosti.com.br/STI-Financeiro/Radiologia/CRTR8/Telas/Cadastro/Profissional_Empresa_Listar_TL.aspx"},
    "9":  {"nome": "9a - GO/TO",    "url": "https://www.studiosti.com.br/STI-Financeiro/Radiologia/CRTR9/Telas/Cadastro/Profissional_Empresa_Listar_TL.aspx"},
    "10": {"nome": "10a - PR",      "url": "https://www.studiosti.com.br/STI-Financeiro/Radiologia/CRTR10/Telas/Cadastro/Profissional_Empresa_Listar_TL.aspx"},
    "11": {"nome": "11a - SC",      "url": "https://www.studiosti.com.br/STI-Financeiro/Radiologia/CRTR11/Telas/Cadastro/Profissional_Empresa_Listar_TL.aspx"},
    "12": {"nome": "12a - MT/MS",   "url": "https://www.studiosti.com.br/STI-Financeiro/Radiologia/CRTR12/Telas/Cadastro/Profissional_Empresa_Listar_TL.aspx"},
    "13": {"nome": "13a - ES",      "url": "https://www.studiosti.com.br/STI-Financeiro/Radiologia/CRTR13/Telas/Cadastro/Profissional_Empresa_Listar_TL.aspx"},
    "14": {"nome": "14a - PA/AP",   "url": "https://www.studiosti.com.br/STI-Financeiro/Radiologia/CRTR14/Telas/Cadastro/Profissional_Empresa_Listar_TL.aspx"},
    "15": {"nome": "15a - PE",      "url": "https://www.studiosti.com.br/STI-Financeiro/Radiologia/CRTR15/Telas/Cadastro/Profissional_Empresa_Listar_TL.aspx"},
    "16": {"nome": "16a - RN/PB",   "url": "https://www.studiosti.com.br/STI-Financeiro/Radiologia/CRTR16/Telas/Cadastro/Profissional_Empresa_Listar_TL.aspx"},
    "17": {"nome": "17a - MA/PI",   "url": "https://www.studiosti.com.br/STI-Financeiro/Radiologia/CRTR17/Telas/Cadastro/Profissional_Empresa_Listar_TL.aspx"},
    "19": {"nome": "19a - AM/RR",   "url": "https://www.studiosti.com.br/STI-Financeiro/Radiologia/CRTR19/Telas/Cadastro/Profissional_Empresa_Listar_TL.aspx"},
}

STATIC_FIELDS = {
    "ctl00$BootstrapMenu": '{"selectedItemIndexPath":"","checkedState":""}',
    "ctl00$Content_Corpo$pc_MensagemState": '{"windowsState":"0:0:-1:0:0:0:-10000:-10000:1:0:0:0"}',
    "ctl00$Content_Corpo$bfl_Filtro$txt_Numero_Registro$State": '{"validationState":""}',
    "ctl00$Content_Corpo$bfl_Filtro$cbo_UF_Endereco$L$State": '{"CustomCallback":""}',
    "ctl00$Content_Corpo$bfl_Filtro$cbo_Cidade_Endereco$L$State": '{"CustomCallback":""}',
    "ctl00$Content_Corpo$bfl_Filtro$txt_Nome_Razao_Social$State": '{"validationState":""}',
    "ctl00$Content_Corpo$bfl_Filtro$txt_Cpf_Cnpj$State": '{"validationState":""}',
    "ctl00$Content_Corpo$frl_Profissional_Empresa$grv_Profissional_Empresa$DXFREditorcol1": "",
    "ctl00$Content_Corpo$frl_Profissional_Empresa$grv_Profissional_Empresa$DXFREditorcol2": "",
    "ctl00$Content_Corpo$frl_Profissional_Empresa$grv_Profissional_Empresa$DXFREditorcol3": "",
    "ctl00$Content_Corpo$frl_Profissional_Empresa$grv_Profissional_Empresa$DXFREditorcol5": "",
    "ctl00$Content_Corpo$frl_Profissional_Empresa$grv_Profissional_Empresa$DXFilterRowMenu": '{"selectedItemIndexPath":"","checkedState":""}',
    "ctl00$SuperBootstrapThemeSwitcher$HiddenLink": '{"data":"12|#|themeLinkID|4|10|1themeLink#"}',
    "ctl00$SuperBootstrapThemeSwitcher$themesMenu": '{"selectedItemIndexPath":"0","checkedState":""}',
    "__ASYNCPOST": "true",
}


class CRTRScraper:
    def __init__(self, base_url):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Cache-Control": "no-cache",
            "X-MicrosoftAjax": "Delta=true",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": base_url,
        })
        self.viewstate = ""
        self.viewstate_generator = ""
        self.event_validation = ""
        self.grid_callback_state = ""
        self.dx_script = ""
        self.dx_css = ""

    def _extract_hidden_fields(self, html):
        soup = BeautifulSoup(html, "lxml")
        vs = soup.find("input", {"id": "__VIEWSTATE"})
        if vs:
            self.viewstate = vs.get("value", "")
        vsg = soup.find("input", {"id": "__VIEWSTATEGENERATOR"})
        if vsg:
            self.viewstate_generator = vsg.get("value", "")
        ev = soup.find("input", {"id": "__EVENTVALIDATION"})
        if ev:
            self.event_validation = ev.get("value", "")

    def _extract_dx_assets(self, html):
        m = re.search(r'DXScript=([^&"]+)', html)
        if m:
            self.dx_script = urllib.parse.unquote(m.group(1))
        m = re.search(r'DXCss=([^&"]+)', html)
        if m:
            self.dx_css = urllib.parse.unquote(m.group(1))

    def _extract_grid_callback_state(self, html):
        m = re.search(r'"callbackState"\s*:\s*"([^"]+)"', html)
        if m:
            self.grid_callback_state = m.group(1)

    def _update_from_response(self, text):
        vs_match = re.search(r'__VIEWSTATE\|([^|]+)', text)
        if vs_match:
            self.viewstate = vs_match.group(1)
        ev_match = re.search(r'__EVENTVALIDATION\|([^|]+)', text)
        if ev_match:
            self.event_validation = ev_match.group(1)
        vsg_match = re.search(r'__VIEWSTATEGENERATOR\|([^|]+)', text)
        if vsg_match:
            self.viewstate_generator = vsg_match.group(1)
        self._extract_grid_callback_state(text)

    def _parse_rows(self, html):
        soup = BeautifulSoup(html, "lxml")
        rows = soup.find_all("tr", id=re.compile(r"DXDataRow\d+"))
        results = []
        for row in rows:
            cells = row.find_all("td")
            if len(cells) >= 4:
                results.append({
                    "numero_registro": cells[0].get_text(strip=True),
                    "nome_razao_social": cells[1].get_text(strip=True),
                    "cidade": cells[2].get_text(strip=True),
                    "situacao": cells[3].get_text(strip=True),
                })
        return results

    def _has_next_page(self, html):
        return 'class="dxbs-pages"' in html or "DXPBT" in html

    def init_session(self):
        resp = self.session.get(self.base_url)
        resp.raise_for_status()
        self._extract_hidden_fields(resp.text)
        self._extract_dx_assets(resp.text)
        self._extract_grid_callback_state(resp.text)

    def search(self, numero_registro=""):
        data = {
            "ctl00$ScriptManager1": "ctl00$Content_Corpo$upp_Botoes_Acoes|ctl00$Content_Corpo$btn_Pesquisar",
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": self.viewstate,
            "__VIEWSTATEGENERATOR": self.viewstate_generator,
            "__EVENTVALIDATION": self.event_validation,
            "ctl00$Content_Corpo$bfl_Filtro$txt_Numero_Registro": numero_registro,
            "ctl00$Content_Corpo$bfl_Filtro$cbo_UF_Endereco": "",
            "ctl00$Content_Corpo$bfl_Filtro$cbo_UF_Endereco$L": "",
            "ctl00_Content_Corpo_bfl_Filtro_cbo_UF_Endereco_VI": "",
            "ctl00$Content_Corpo$bfl_Filtro$cbo_Cidade_Endereco": " ",
            "ctl00$Content_Corpo$bfl_Filtro$cbo_Cidade_Endereco$L": "",
            "ctl00_Content_Corpo_bfl_Filtro_cbo_Cidade_Endereco_VI": "",
            "ctl00$Content_Corpo$bfl_Filtro$txt_Nome_Razao_Social": "",
            "ctl00$Content_Corpo$bfl_Filtro$txt_Cpf_Cnpj": "",
            "ctl00$Content_Corpo$frl_Profissional_Empresa$grv_Profissional_Empresa": (
                f'{{"lastMultiSelectIndex":-1,"keys":[],"callbackState":"{self.grid_callback_state}","selection":""}}'
            ),
            "ctl00$Content_Corpo$btn_Pesquisar": "Pesquisar",
        }
        data.update(STATIC_FIELDS)
        if self.dx_script:
            data["DXScript"] = self.dx_script
        if self.dx_css:
            data["DXCss"] = self.dx_css

        resp = self.session.post(
            self.base_url,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"},
        )
        resp.raise_for_status()
        self._update_from_response(resp.text)
        rows = self._parse_rows(resp.text)
        all_rows = list(rows)

        page = 2
        while self._has_next_page(resp.text):
            time.sleep(0.3)
            resp = self._navigate_page(page)
            if resp is None:
                break
            new_rows = self._parse_rows(resp.text)
            if not new_rows:
                break
            all_rows.extend(new_rows)
            self._update_from_response(resp.text)
            page += 1

        return all_rows

    def _navigate_page(self, page_num):
        data = {
            "ctl00$ScriptManager1": "ctl00$Content_Corpo$frl_Profissional_Empresa$upp_Profissional_Empresa|ctl00$Content_Corpo$frl_Profissional_Empresa$grv_Profissional_Empresa",
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": self.viewstate,
            "__VIEWSTATEGENERATOR": self.viewstate_generator,
            "__EVENTVALIDATION": self.event_validation,
            "ctl00$Content_Corpo$frl_Profissional_Empresa$grv_Profissional_Empresa": (
                f'{{"lastMultiSelectIndex":-1,"keys":[],"callbackState":"{self.grid_callback_state}","selection":""}}'
            ),
        }
        data.update(STATIC_FIELDS)
        if self.dx_script:
            data["DXScript"] = self.dx_script
        if self.dx_css:
            data["DXCss"] = self.dx_css

        resp = self.session.post(
            self.base_url,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"},
        )
        resp.raise_for_status()
        return resp
