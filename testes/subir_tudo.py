"""Liga a API e todos os workers das Fases 4 e 5 num terminal só (atalho para testes).

Uso (a partir da raiz, com o .venv ativado):
    python testes/subir_tudo.py                 # API + assentos + 2 payment_service
    python testes/subir_tudo.py --pagamentos 3  # escolhe quantas réplicas do pagamento
Ctrl+C desliga tudo. Para o vídeo, prefira um terminal por serviço.
"""

import argparse
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ / "workers"))

import db  # noqa: E402

PORTA_API = 5000
NO_WINDOWS = os.name == "nt"


def api_ja_rodando():
    # Se alguém já subiu a API (outro terminal, VS Code), não sobe outra:
    # duas APIs na mesma porta fazem o navegador cair na errada.
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", PORTA_API)) == 0


def verificar_broker():
    # Reaproveita o antes_de_testar: 0 = livre, 1 = alguém testando, 2 = sobras.
    codigo = subprocess.run([sys.executable, str(RAIZ / "testes" / "antes_de_testar.py")], cwd=RAIZ).returncode
    if codigo == 1:
        sys.exit("\nNada foi ligado: tem outra pessoa usando o broker.")
    if codigo not in (0, 2):
        sys.exit("\nNada foi ligado: não consegui consultar o broker (confira a AMQP_URL no .env).")


def iniciar(nome, script):
    env = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUNBUFFERED": "1"}
    # Grupo de processos próprio: o Ctrl+C chega só aqui, e quem desliga os
    # serviços (na ordem certa) é o finally do main().
    extra = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP} if NO_WINDOWS else {"start_new_session": True}
    proc = subprocess.Popen(
        [sys.executable, script], cwd=RAIZ, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace", **extra,
    )
    threading.Thread(target=repassar_saida, args=(nome, proc), daemon=True).start()
    return proc


def repassar_saida(nome, proc):
    # Junta a saída de todos os serviços neste terminal. Os workers já imprimem
    # "[servico pid=...]"; só o GET do polling da página é filtrado (vira spam).
    for linha in proc.stdout:
        linha = linha.rstrip()
        if '"GET ' in linha:
            continue
        print(linha if linha.startswith("[") else f"[{nome}] {linha}", flush=True)
    print(f"[{nome}] processo terminou (código {proc.wait()})", flush=True)


def desligar(proc):
    if proc.poll() is not None:
        return
    if NO_WINDOWS:
        # /T derruba a árvore: o python.exe do .venv é um lançador que abre outro processo.
        subprocess.run(["taskkill", "/T", "/F", "/PID", str(proc.pid)], capture_output=True)
    else:
        os.killpg(proc.pid, signal.SIGTERM)
    proc.wait()


def _parar(*_):
    raise KeyboardInterrupt


def main():
    parser = argparse.ArgumentParser(description="Sobe a API e os workers das Fases 4 e 5.")
    parser.add_argument("--pagamentos", type=int, default=2, help="réplicas do payment_service (padrão: 2)")
    args = parser.parse_args()

    verificar_broker()
    if not db.DB_PATH.exists():
        print("\nBanco não encontrado: criando com python workers/db.py --reset")
        subprocess.run([sys.executable, "workers/db.py", "--reset"], cwd=RAIZ, check=True)

    servicos = []
    print()
    if api_ja_rodando():
        print(f"[subir_tudo] API já está rodando na porta {PORTA_API}: usando a existente.")
    else:
        servicos.append(iniciar("api", "api/app.py"))
    servicos.append(iniciar("assentos_service", "workers/assentos_service.py"))
    for _ in range(max(1, args.pagamentos)):
        servicos.append(iniciar("payment_service", "workers/payment_service.py"))

    print(f"[subir_tudo] {len(servicos)} processo(s) ligado(s). Abra http://localhost:{PORTA_API}  |  Ctrl+C desliga tudo.\n",
          flush=True)

    # Ctrl+Break (Windows) e SIGTERM também desligam tudo, não só o Ctrl+C.
    for nome_sinal in ("SIGBREAK", "SIGTERM"):
        if hasattr(signal, nome_sinal):
            signal.signal(getattr(signal, nome_sinal), _parar)

    try:
        # Laço com sleep em vez de proc.wait(): no Windows o wait() trava a thread
        # de um jeito que o Ctrl+C só é percebido quando o processo termina.
        while any(proc.poll() is None for proc in servicos):
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        print("\n[subir_tudo] desligando...", flush=True)
        # Workers primeiro: sem ack, a mensagem que estava em processamento volta para a fila.
        for proc in reversed(servicos):
            desligar(proc)
        print("[subir_tudo] tudo desligado.", flush=True)


if __name__ == "__main__":
    main()
