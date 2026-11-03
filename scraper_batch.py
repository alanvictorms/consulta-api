"""
Scraper CRTR em modo batch (sem interacao).

Uso:
  py scraper_batch.py --uf CE
  py scraper_batch.py --uf CE --numero 000
  py scraper_batch.py --uf SP --nome "COOPERATIVA"
  py scraper_batch.py --uf RJ --cpf 12345678901
  py scraper_batch.py --all-ufs              # todas as UFs
  py scraper_batch.py --all-ufs --output profissionais.csv
"""
import argparse
import sys
import time

from scraper import CRTRScraper, save_csv, UF_MAP


def run_single(scraper, args):
    rows = scraper.search(
        numero_registro=args.numero or "",
        uf=args.uf or "",
        cidade=args.cidade or "",
        nome=args.nome or "",
        cpf_cnpj=args.cpf or "",
    )
    return rows


def run_all_ufs(scraper, args):
    ufs = sorted(k for k in UF_MAP if k)
    all_rows = []
    for i, uf in enumerate(ufs, 1):
        print(f"\n[{i}/{len(ufs)}] === UF: {uf} ===")
        try:
            rows = scraper.search(
                numero_registro=args.numero or "",
                uf=uf,
                cidade="",
                nome=args.nome or "",
                cpf_cnpj=args.cpf or "",
            )
            all_rows.extend(rows)
        except Exception as e:
            print(f"[!] Erro na UF {uf}: {e}")
        time.sleep(1)
    return all_rows


def main():
    parser = argparse.ArgumentParser(description="Scraper CRTR - Consulta Profissional/Empresa")
    parser.add_argument("--uf", help="UF (ex: CE, SP, RJ)")
    parser.add_argument("--numero", help="Numero do CRTR")
    parser.add_argument("--cidade", help="Cidade")
    parser.add_argument("--nome", help="Nome/Razao Social")
    parser.add_argument("--cpf", help="CPF/CNPJ")
    parser.add_argument("--all-ufs", action="store_true", help="Consultar todas as UFs")
    parser.add_argument("--output", "-o", default="resultados_crtr.csv", help="Arquivo de saida CSV (default: resultados_crtr.csv)")
    args = parser.parse_args()

    if not args.uf and not args.all_ufs:
        print("Erro: informe --uf ou --all-ufs")
        parser.print_help()
        sys.exit(1)

    scraper = CRTRScraper()
    scraper.init_session()

    if args.all_ufs:
        rows = run_all_ufs(scraper, args)
    else:
        rows = run_single(scraper, args)

    print(f"\n[=] Total: {len(rows)} registros.")
    if rows:
        save_csv(rows, args.output)


if __name__ == "__main__":
    main()
