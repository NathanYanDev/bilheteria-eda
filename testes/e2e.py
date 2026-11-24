"""Verificação de ponta a ponta (Atividade 04 – Message Broker).

Sobe a API e os workers de verdade (subprocessos), fala com o broker do .env e
confere cada requisito com evidência: resposta HTTP, contagem de fila, linha de
log com pid e linhas do banco.

ATENÇÃO: este script PURGA todas as filas do broker configurado no .env e
RESETA o banco SQLite. O broker é compartilhado pelo grupo: rode
`python testes/antes_de_testar.py` e avise no grupo antes.

Uso (a partir da raiz, com o .venv ativado):
    python testes/e2e.py --sim               # roda tudo (--sim confirma o purge/reset)
    python testes/e2e.py --sim --idle 660    # + teste da API depois de N s ociosa (heartbeat)
    python testes/e2e.py --sim --manter-filas  # não purga as filas no final (para ver no painel)

Saídas:
    logs/<servico>[_N].log    saída de cada processo (o _N é a 2ª réplica)
    logs/backup_filas_*.json  cópia das mensagens que estavam nas filas antes do 1º purge
    logs/e2e_resultado.json   PASS/FAIL + evidências de cada teste

Só usa a biblioteca padrão + pika (que já está no requirements.txt).
A AMQP_URL nunca é impressa: ela contém a senha.
"""

import argparse
import base64
import json
import os
import re
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ / "workers"))

import pika  # noqa: E402

import common  # noqa: E402
import db  # noqa: E402
from setup_broker import DLQ, FILAS  # noqa: E402

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

API = "http://127.0.0.1:5000"
LOGS = RAIZ / "logs"
NO_WINDOWS = os.name == "nt"
FILAS_TODAS = [*FILAS, DLQ]
WORKERS = [
    "assentos_service",
    "payment_service",
    "emissao_service",
    "notificacao_service",
    "status_service",
]
FILA_DO_WORKER = {
    "assentos_service": "q.assentos",
    "payment_service": "q.pagamentos",
    "emissao_service": "q.emissao",
    "notificacao_service": "q.notificacoes",
    "status_service": "q.status",
}
FINAIS = {"CONFIRMADA", "ASSENTO_INDISPONIVEL", "PAGAMENTO_RECUSADO"}

CARTAO_OK = "4111111111111111"
CARTAO_SEM_LIMITE = "4111111111110000"
CARTAO_FRAUDE = "9999111111111111"

ESPERADO = {
    "q.assentos": {"reserva.criada", "pagamento.aprovado", "pagamento.recusado"},
    "q.pagamentos": {"assento.reservado"},
    "q.emissao": {"pagamento.aprovado"},
    "q.notificacoes": {
        "reserva.criada",
        "assento.indisponivel",
        "pagamento.recusado",
        "ingresso.emitido",
    },
    "q.status": {"#"},
}

RESULTADOS = []
EXTRA = {}  # dados guardados entre testes (ex.: propriedades lidas da DLQ)


# --------------------------------------------------------------------------- #
# Utilidades
# --------------------------------------------------------------------------- #
def agora():
    return datetime.now().strftime("%H:%M:%S")


def info(msg):
    print(f"  [{agora()}] {msg}", flush=True)


def esperar(condicao, timeout, intervalo=0.5):
    """Polling: chama condicao() até ela devolver algo verdadeiro ou estourar o timeout.

    Devolve o último valor (verdadeiro ou não). Nunca usar sleep fixo longo:
    o teste anda assim que a condição fica pronta.
    """
    limite = time.monotonic() + timeout
    valor = condicao()
    while not valor and time.monotonic() < limite:
        time.sleep(intervalo)
        valor = condicao()
    return valor


def registrar(cid, titulo, ok, evidencias):
    RESULTADOS.append(
        {"id": cid, "titulo": titulo, "ok": bool(ok), "evidencias": evidencias}
    )
    print(f"\n{'PASS' if ok else 'FAIL'}  {cid}  {titulo}", flush=True)
    for linha in evidencias:
        print(f"      {linha}", flush=True)


def http(metodo, caminho, corpo=None, timeout=15):
    """Requisição HTTP com urllib. Devolve (status, json_ou_texto, segundos)."""
    dados = json.dumps(corpo).encode("utf-8") if corpo is not None else None
    headers = {"Content-Type": "application/json"} if dados is not None else {}
    req = urllib.request.Request(
        API + caminho, data=dados, method=metodo, headers=headers
    )
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status, texto = resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        status, texto = exc.code, exc.read()
    segundos = time.perf_counter() - t0
    try:
        conteudo = json.loads(texto)
    except ValueError:
        conteudo = texto.decode("utf-8", "replace")
    return status, conteudo, segundos


def reservar(
    numero, cartao=CARTAO_OK, nome="Teste E2E", email="e2e@exemplo.com", evento_id=1
):
    return http(
        "POST",
        "/api/reservas",
        {
            "evento_id": evento_id,
            "numero": numero,
            "nome": nome,
            "email": email,
            "cartao": cartao,
        },
    )


def status_reserva(reserva_id):
    status, corpo, _ = http("GET", f"/api/reservas/{reserva_id}")
    return corpo.get("status") if status == 200 else None


def esperar_status(reserva_id, aceitos, timeout):
    """Espera o status entrar em `aceitos`. Devolve (status_final, historico_visto)."""
    historico = []

    def cond():
        s = status_reserva(reserva_id)
        if s and (not historico or historico[-1] != s):
            historico.append(s)
        return s in aceitos

    esperar(cond, timeout, intervalo=0.3)
    return (historico[-1] if historico else None), historico


def sql(consulta, *params):
    conn = db.connect()
    try:
        return [dict(r) for r in conn.execute(consulta, params).fetchall()]
    finally:
        conn.close()


def assento(numero, evento_id=1):
    rows = sql(
        "SELECT status, reserva_id FROM assentos WHERE evento_id = ? AND numero = ?",
        evento_id,
        numero,
    )
    return rows[0] if rows else None


def assentos_livres(evento_id=1):
    return [
        r["numero"]
        for r in sql(
            "SELECT numero FROM assentos WHERE evento_id = ? AND status = 'LIVRE' ORDER BY numero",
            evento_id,
        )
    ]


def linhas_log(padrao, contem):
    """Linhas dos arquivos logs/<padrao> que contêm o texto."""
    achadas = []
    for arquivo in sorted(LOGS.glob(padrao)):
        for linha in arquivo.read_text(encoding="utf-8", errors="replace").splitlines():
            if contem in linha:
                achadas.append(f"{arquivo.name}: {linha.strip()}")
    return achadas


def curto(reserva_id):
    return common.short_id(reserva_id)


# --------------------------------------------------------------------------- #
# Broker (AMQP): contagens, purge, inspeção da DLQ, publicação manual
# --------------------------------------------------------------------------- #
class Broker:
    """Uma conexão só, reaberta se cair (o canal ocioso pode cair por heartbeat)."""

    def __init__(self):
        self.conn = None

    def _conexao(self):
        if self.conn is None or self.conn.is_closed:
            self.conn = common.get_connection("e2e")
        return self.conn

    def _tentar(self, funcao):
        for tentativa in (1, 2):
            try:
                return funcao(self._conexao())
            except pika.exceptions.AMQPConnectionError:
                self.fechar()
                if tentativa == 2:
                    raise

    def fila(self, nome):
        """(Ready, consumidores) via queue_declare(passive=True): só consulta, não cria."""

        def consulta(conn):
            canal = conn.channel()
            try:
                metodo = canal.queue_declare(queue=nome, passive=True).method
                return metodo.message_count, metodo.consumer_count
            finally:
                if canal.is_open:
                    canal.close()

        return self._tentar(consulta)

    def contagens(self):
        return {fila: self.fila(fila)[0] for fila in FILAS_TODAS}

    def consumidores(self, fila):
        return self.fila(fila)[1]

    def purgar(self):
        def purge(conn):
            canal = conn.channel()
            for fila in FILAS_TODAS:
                canal.queue_purge(fila)
            canal.close()

        self._tentar(purge)

    def backup(self, destino):
        """Copia corpo + propriedades de todas as mensagens Ready para um JSON.

        basic_get SEM ack: ao fechar o canal, o broker devolve tudo para a fila.
        Só depois disso o purge apaga. Assim nada se perde se algo falhar no meio.
        """
        copia = []

        def ler(conn):
            for fila in FILAS_TODAS:
                canal = conn.channel()
                while True:
                    metodo, props, corpo = canal.basic_get(queue=fila, auto_ack=False)
                    if metodo is None:
                        break
                    try:
                        texto, codificacao = corpo.decode("utf-8"), "utf-8"
                    except UnicodeDecodeError:
                        texto, codificacao = base64.b64encode(corpo).decode(), "base64"
                    copia.append(
                        {
                            "fila": fila,
                            "exchange": metodo.exchange,
                            "routing_key": metodo.routing_key,
                            "redelivered": metodo.redelivered,
                            "properties": {
                                "message_id": props.message_id,
                                "content_type": props.content_type,
                                "delivery_mode": props.delivery_mode,
                                "type": props.type,
                                "headers": props.headers,
                            },
                            "body": texto,
                            "body_encoding": codificacao,
                        }
                    )
                canal.close()  # devolve (requeue) tudo o que foi lido sem ack

        self._tentar(ler)
        destino.write_text(
            json.dumps(copia, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        return len(copia)

    def espiar(self, fila, filtro):
        """basic_get de toda a fila, guarda as que passam no filtro e devolve TODAS
        com basic_nack(requeue=True): a fila fica como estava."""
        achadas = []

        def ler(conn):
            canal = conn.channel()
            ultimo = None
            while True:
                metodo, props, corpo = canal.basic_get(queue=fila, auto_ack=False)
                if metodo is None:
                    break
                ultimo = metodo.delivery_tag
                if filtro(corpo):
                    achadas.append(
                        {
                            "routing_key": metodo.routing_key,
                            "props": props,
                            "body": corpo,
                        }
                    )
            if ultimo is not None:
                canal.basic_nack(delivery_tag=ultimo, multiple=True, requeue=True)
            canal.close()

        self._tentar(ler)
        return achadas

    def publicar_bruto(self, routing_key, corpo, props):
        def pub(conn):
            canal = conn.channel()
            canal.confirm_delivery()
            canal.basic_publish(
                exchange=common.EXCHANGE,
                routing_key=routing_key,
                body=corpo,
                properties=props,
            )
            canal.close()

        self._tentar(pub)

    def fechar(self):
        if self.conn is not None and self.conn.is_open:
            try:
                self.conn.close()
            except pika.exceptions.AMQPError:
                pass
        self.conn = None


# --------------------------------------------------------------------------- #
# HTTP API de gerenciamento (RabbitMQ/LavinMQ): topologia e bindings
# --------------------------------------------------------------------------- #
class Gerenciamento:
    def __init__(self):
        url = urllib.parse.urlparse(common.AMQP_URL)
        self.base = f"https://{url.hostname}/api"
        self.vhost = urllib.parse.quote(
            urllib.parse.unquote(url.path[1:]) or "/", safe=""
        )
        credencial = f"{urllib.parse.unquote(url.username or '')}:{urllib.parse.unquote(url.password or '')}"
        self._auth = "Basic " + base64.b64encode(credencial.encode()).decode()

    def get(self, *partes):
        caminho = "/".join(
            urllib.parse.quote(p, safe="") if p != "{vh}" else self.vhost
            for p in partes
        )
        req = urllib.request.Request(f"{self.base}/{caminho}")
        req.add_header("Authorization", self._auth)
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.load(resp)

    def disponivel(self):
        try:
            self.get("overview")
            return True
        except Exception as exc:  # noqa: BLE001  (só para decidir o fallback)
            info(f"API de gerenciamento indisponível: {type(exc).__name__}")
            return False


# --------------------------------------------------------------------------- #
# Processos: sobe/derruba API e workers, cada um com seu log
# --------------------------------------------------------------------------- #
class Servicos:
    def __init__(self):
        self.procs = {}

    @staticmethod
    def arquivo(nome, n):
        return LOGS / (f"{nome}.log" if n == 1 else f"{nome}_{n}.log")

    def iniciar(self, nome, n=1, env_extra=None):
        script = "api/app.py" if nome == "api" else f"workers/{nome}.py"
        env = {
            **os.environ,
            "PYTHONIOENCODING": "utf-8",
            "PYTHONUNBUFFERED": "1",
            **(env_extra or {}),
        }
        log = open(self.arquivo(nome, n), "a", encoding="utf-8")
        log.write(
            f"\n===== e2e {agora()}: iniciando {nome} #{n} {env_extra or ''} =====\n"
        )
        log.flush()
        # Grupo de processos próprio para conseguir derrubar a árvore inteira
        # (no Windows o python.exe do .venv é um lançador que abre outro processo).
        extra = (
            {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
            if NO_WINDOWS
            else {"start_new_session": True}
        )
        proc = subprocess.Popen(
            [sys.executable, "-u", script],
            cwd=RAIZ,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            **extra,
        )
        self.procs[(nome, n)] = (proc, log)
        return proc

    def vivo(self, nome, n=1):
        par = self.procs.get((nome, n))
        return par is not None and par[0].poll() is None

    def derrubar(self, nome, n=1):
        """Kill sem aviso (TerminateProcess/SIGKILL): simula uma queda, não um Ctrl+C."""
        proc, log = self.procs.pop((nome, n))
        if proc.poll() is None:
            if NO_WINDOWS:
                subprocess.run(
                    ["taskkill", "/T", "/F", "/PID", str(proc.pid)], capture_output=True
                )
            else:
                os.killpg(proc.pid, signal.SIGKILL)
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
        log.write(f"===== e2e {agora()}: {nome} #{n} derrubado (kill) =====\n")
        log.close()

    def derrubar_todos(self):
        for nome, n in list(reversed(list(self.procs))):
            try:
                self.derrubar(nome, n)
            except Exception as exc:  # noqa: BLE001  (limpeza: segue para os outros)
                print(f"  aviso: não consegui derrubar {nome} #{n}: {exc!r}")

    def pid_real(self, nome, n=1):
        """pid impresso pelo próprio worker (o do lançador do .venv é outro)."""
        texto = self.arquivo(nome, n).read_text(encoding="utf-8", errors="replace")
        achados = re.findall(rf"\[{nome} pid=(\d+)\] aguardando", texto)
        return achados[-1] if achados else "?"


def subir_worker(svc, broker, nome, n=1, env_extra=None):
    """Sobe e espera o consumidor aparecer na fila (prova de que conectou)."""
    fila = FILA_DO_WORKER[nome]
    antes = broker.consumidores(fila)
    svc.iniciar(nome, n, env_extra)
    if not esperar(lambda: broker.consumidores(fila) > antes, 30):
        raise RuntimeError(
            f"{nome} #{n} não apareceu como consumidor de {fila} (veja {svc.arquivo(nome, n).name})"
        )
    esperar(lambda: svc.pid_real(nome, n) != "?", 5)
    info(
        f"{nome} #{n} conectado (pid={svc.pid_real(nome, n)}, consumidores em {fila}: {broker.consumidores(fila)})"
    )


def derrubar_worker(svc, broker, nome, n=1):
    fila = FILA_DO_WORKER[nome]
    antes = broker.consumidores(fila)
    svc.derrubar(nome, n)
    esperar(lambda: broker.consumidores(fila) < antes, 30)
    info(f"{nome} #{n} derrubado (consumidores em {fila}: {broker.consumidores(fila)})")


def subir_api(svc):
    svc.iniciar("api")
    if not esperar(lambda: _api_responde(), 20):
        raise RuntimeError("API não respondeu em 20 s (veja logs/api.log)")
    info("API respondendo em http://127.0.0.1:5000")


def _api_responde():
    try:
        return http("GET", "/api/eventos", timeout=2)[0] == 200
    except (urllib.error.URLError, OSError):
        return False


def porta_ocupada(porta=5000):
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", porta)) == 0


def fmt_filas(contagens):
    return ", ".join(f"{fila}={n}" for fila, n in contagens.items())


# --------------------------------------------------------------------------- #
# Parte A (parcial e automatizável): varredura do código
# --------------------------------------------------------------------------- #
def parte_a():
    arquivos = [p for p in [*RAIZ.glob("api/*.py"), *RAIZ.glob("workers/**/*.py")]]
    achados = {
        "auto_ack=True": [],
        "requeue=True": [],
        "basic_publish sem delivery_mode": [],
    }
    for arq in arquivos:
        texto = arq.read_text(encoding="utf-8")
        for i, linha in enumerate(texto.splitlines(), 1):
            if re.search(r"auto_ack\s*=\s*True", linha):
                achados["auto_ack=True"].append(f"{arq.relative_to(RAIZ)}:{i}")
            # Só chamadas de verdade: a docstring do common.py cita requeue=True para explicar o porquê de não usar.
            if re.search(r"basic_(nack|reject)\([^)]*requeue\s*=\s*True", linha):
                achados["requeue=True"].append(f"{arq.relative_to(RAIZ)}:{i}")
        for m in re.finditer(r"basic_publish\((.*?)\n\s*\)", texto, re.S):
            if "delivery_mode" not in m.group(1):
                linha = texto[: m.start()].count("\n") + 1
                achados["basic_publish sem delivery_mode"].append(
                    f"{arq.relative_to(RAIZ)}:{linha}"
                )
    ok = not any(achados.values())
    registrar(
        "A*",
        "Varredura automática do código (auto_ack, requeue, delivery_mode)",
        ok,
        [f"{k}: {v or 'nenhum'}" for k, v in achados.items()],
    )


# --------------------------------------------------------------------------- #
# Parte B: topologia no broker
# --------------------------------------------------------------------------- #
def parte_b(broker, mgmt, mgmt_ok):
    evid, diferencas = [], []
    r = subprocess.run(
        [sys.executable, "-u", "workers/setup_broker.py"],
        cwd=RAIZ,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )
    evid.append(f"setup_broker.py exit={r.returncode}")
    evid += [f"  {linha}" for linha in r.stdout.splitlines() if linha.strip()]
    if r.returncode != 0:
        diferencas.append(f"setup_broker.py falhou: {r.stderr.strip()[-300:]}")

    codigo = {f: set(rks) for f, rks in FILAS.items()}
    if codigo != ESPERADO:
        diferencas.append(
            f"setup_broker.FILAS difere da tabela: {codigo} != {ESPERADO}"
        )

    if not mgmt_ok:
        evid.append(
            "API de gerenciamento indisponível: bindings verificados só pelo código e pelos testes funcionais"
        )
        # Pelo menos confirma que todas as filas existem.
        for fila in FILAS_TODAS:
            evid.append(
                f"{fila}: existe (passive declare), Ready={broker.fila(fila)[0]}"
            )
        registrar(
            "B",
            "Topologia no broker",
            not diferencas,
            evid + [f"DIFERENÇA: {d}" for d in diferencas],
        )
        return

    for nome, tipo in ((common.EXCHANGE, "topic"), (common.DLX, "fanout")):
        d = mgmt.get("exchanges", "{vh}", nome)
        evid.append(f"exchange {nome}: type={d.get('type')} durable={d.get('durable')}")
        if d.get("type") != tipo or d.get("durable") is not True:
            diferencas.append(f"exchange {nome} esperado {tipo}/durable")

    EXTRA["mgmt_filas"] = {}
    for fila in FILAS_TODAS:
        d = mgmt.get("queues", "{vh}", fila)
        args = d.get("arguments") or {}
        dlx = args.get("x-dead-letter-exchange")
        EXTRA["mgmt_filas"][fila] = {"durable": d.get("durable"), "arguments": args}
        ligacoes = mgmt.get("queues", "{vh}", fila, "bindings")
        reais = sorted(
            (b.get("source"), b.get("routing_key")) for b in ligacoes if b.get("source")
        )
        evid.append(
            f"fila {fila}: durable={d.get('durable')} x-dead-letter-exchange={dlx} "
            f"bindings={[f'{s}:{rk}' for s, rk in reais]}"
        )
        if d.get("durable") is not True:
            diferencas.append(f"{fila} não é durable")
        if fila == DLQ:
            if dlx is not None:
                diferencas.append("q.dlq não deveria ter DLX")
            if not any(s == common.DLX for s, _ in reais):
                diferencas.append("q.dlq não está ligada a ingressos.dlx")
            extras = [b for b in reais if b[0] != common.DLX]
            if extras:
                diferencas.append(f"q.dlq tem bindings extras: {extras}")
        else:
            if dlx != common.DLX:
                diferencas.append(f"{fila} sem x-dead-letter-exchange={common.DLX}")
            esperado = {(common.EXCHANGE, rk) for rk in ESPERADO[fila]}
            if set(reais) != esperado:
                diferencas.append(
                    f"{fila}: faltando {sorted(esperado - set(reais))}, "
                    f"sobrando {sorted(set(reais) - esperado)}"
                )

    # Bindings da exchange principal para destinos que não estão na tabela.
    origem = mgmt.get("exchanges", "{vh}", common.EXCHANGE, "bindings", "source")
    estranhos = sorted({b.get("destination") for b in origem} - set(ESPERADO))
    evid.append(
        f"destinos ligados a {common.EXCHANGE}: {sorted({b.get('destination') for b in origem})}"
    )
    if estranhos:
        diferencas.append(
            f"{common.EXCHANGE} tem bindings para destinos fora da tabela: {estranhos}"
        )

    # Heartbeat negociado (importa para o risco de canal ocioso na API).
    try:
        for c in mgmt.get("connections"):
            nome_conexao = (c.get("client_properties") or {}).get("connection_name", "")
            if nome_conexao.startswith("e2e "):
                EXTRA["heartbeat"] = c.get("timeout")
                evid.append(f"heartbeat negociado (conexão e2e): {c.get('timeout')} s")
                break
    except Exception as exc:  # noqa: BLE001
        evid.append(f"heartbeat: não consegui ler ({type(exc).__name__})")

    registrar(
        "B",
        "Topologia no broker (exchanges, filas, DLX, bindings)",
        not diferencas,
        evid + [f"DIFERENÇA: {d}" for d in diferencas],
    )


# --------------------------------------------------------------------------- #
# Parte C: testes funcionais
# --------------------------------------------------------------------------- #
def c2_validacao(broker):
    antes = broker.contagens()
    reservas_antes = sql("SELECT COUNT(*) AS n FROM reservas")[0]["n"]
    casos = [
        (
            "sem e-mail",
            {"evento_id": 1, "numero": 1, "nome": "Teste", "cartao": CARTAO_OK},
        ),
        (
            "cartão curto",
            {
                "evento_id": 1,
                "numero": 1,
                "nome": "Teste",
                "email": "a@b.com",
                "cartao": "4111",
            },
        ),
        (
            "assento inexistente",
            {
                "evento_id": 1,
                "numero": 999,
                "nome": "Teste",
                "email": "a@b.com",
                "cartao": CARTAO_OK,
            },
        ),
        (
            "evento inexistente",
            {
                "evento_id": 99,
                "numero": 1,
                "nome": "Teste",
                "email": "a@b.com",
                "cartao": CARTAO_OK,
            },
        ),
    ]
    evid, ok = [f"filas antes: {fmt_filas(antes)}"], True
    for nome, corpo in casos:
        status, resp, seg = http("POST", "/api/reservas", corpo)
        evid.append(f"POST {nome} -> HTTP {status} {resp} ({seg * 1000:.0f} ms)")
        ok &= status == 400
    time.sleep(1)  # dá tempo de uma publicação indevida aparecer nas contagens
    depois = broker.contagens()
    reservas_depois = sql("SELECT COUNT(*) AS n FROM reservas")[0]["n"]
    evid.append(f"filas depois: {fmt_filas(depois)}")
    evid.append(f"linhas em reservas: antes={reservas_antes} depois={reservas_depois}")
    ok &= antes == depois and reservas_antes == reservas_depois
    registrar("C2", "Validação na origem: 400 e nada publicado", ok, evid)


def c1_c3_resposta_imediata(broker):
    workers_conectados = {f: broker.consumidores(f) for f in FILAS}
    antes = broker.contagens()
    status, resp, seg = reservar(1)
    rid = resp.get("reserva_id") if isinstance(resp, dict) else None
    st_banco = status_reserva(rid) if rid else None
    status2, resp2, seg2 = reservar(2)  # 2º POST: conexão do publicador já aberta
    evid = [
        f"consumidores conectados (workers desligados): {workers_conectados}",
        f"POST #1 (publicador frio) -> HTTP {status} {resp} em {seg * 1000:.0f} ms",
        f"GET /api/reservas/{curto(rid)} -> status {st_banco}",
        f"POST #2 (publicador quente) -> HTTP {status2} em {seg2 * 1000:.0f} ms",
    ]
    ok = (
        status == 202
        and seg < 1.0
        and st_banco == "EM_PROCESSAMENTO"
        and status2 == 202
        and seg2 < 1.0
        and not any(workers_conectados.values())
    )
    registrar("C1", "Resposta imediata com workers desligados (202 < 1 s)", ok, evid)

    # C3: cada reserva.criada vira uma cópia em cada fila que assina a routing key.
    esperado = {
        **antes,
        "q.assentos": antes["q.assentos"] + 2,
        "q.notificacoes": antes["q.notificacoes"] + 2,
        "q.status": antes["q.status"] + 2,
    }
    depois = (
        esperar(
            lambda: (lambda c: c if c == esperado else None)(broker.contagens()), 10
        )
        or broker.contagens()
    )
    evid = [
        f"antes:  {fmt_filas(antes)}",
        f"depois: {fmt_filas(depois)}  (2 POSTs aceitos)",
        "esperado: +2 em q.assentos, q.notificacoes, q.status; +0 em q.pagamentos, q.emissao, q.dlq",
    ]
    registrar(
        "C3", "Pub/Sub: reserva.criada copiada para 3 filas", depois == esperado, evid
    )


def c4_fluxo_normal(svc):
    numero = assentos_livres()[0]
    status, resp, seg = reservar(numero)
    rid = resp["reserva_id"]
    final, historico = esperar_status(rid, FINAIS, 40)
    cid = curto(rid)
    # O log sai depois do ack; espera o último elo (notificação do ingresso) aparecer.
    esperar(
        lambda: linhas_log("notificacao_service*.log", f"reserva {cid} | código"), 10
    )
    cadeia = [
        (
            "assentos_service*.log",
            f"<- reserva.criada reserva={cid}",
            "-> assento.reservado",
        ),
        (
            "payment_service*.log",
            f"<- assento.reservado reserva={cid}",
            "-> pagamento.aprovado",
        ),
        (
            "emissao_service*.log",
            f"<- pagamento.aprovado reserva={cid}",
            "-> ingresso.emitido",
        ),
        ("status_service*.log", f"reserva={cid}: status -> CONFIRMADA", ""),
        ("notificacao_service*.log", f"reserva {cid} | código", ""),
    ]
    evid = [
        f"POST assento {numero} -> HTTP {status} em {seg * 1000:.0f} ms, reserva {cid}",
        f"status visto pelo polling: {' -> '.join(historico)}",
    ]
    ok = final == "CONFIRMADA"
    for padrao, entrada, saida in cadeia:
        linhas = [l for l in linhas_log(padrao, entrada) if saida in l]
        evid.append(
            linhas[0] if linhas else f"FALTOU no log {padrao}: '{entrada} ... {saida}'"
        )
        ok &= bool(linhas)
    a = assento(numero)
    ingresso = sql("SELECT codigo FROM ingressos WHERE reserva_id = ?", rid)
    evid.append(f"assento {numero}: {a}")
    evid.append(f"ingressos: {ingresso}")
    ok &= a["status"] == "VENDIDO" and a["reserva_id"] == rid and len(ingresso) == 1
    registrar("C4", "Fluxo normal / coreografia até CONFIRMADA", ok, evid)


def c5_recusa(broker):
    dlq_antes = broker.fila(DLQ)[0]
    numero = assentos_livres()[0]
    status, resp, _ = reservar(numero, CARTAO_SEM_LIMITE)
    rid = resp["reserva_id"]
    final, historico = esperar_status(rid, FINAIS, 40)
    liberado = esperar(
        lambda: (assento(numero) or {}).get("status") == "LIVRE"
        and linhas_log(
            "assentos_service*.log", f"reserva={curto(rid)}: pagamento recusado"
        ),
        15,
    )
    a = assento(numero)
    time.sleep(1)
    dlq_depois = broker.fila(DLQ)[0]
    evid = [
        f"POST assento {numero} cartão ...0000 -> HTTP {status}, reserva {curto(rid)}",
        f"status: {' -> '.join(historico)}",
        *linhas_log("payment_service*.log", f"reserva={curto(rid)}"),
        *linhas_log(
            "assentos_service*.log", f"reserva={curto(rid)}: pagamento recusado"
        ),
        f"assento {numero} depois: {a}",
        f"q.dlq: antes={dlq_antes} depois={dlq_depois}",
    ]
    ok = (
        final == "PAGAMENTO_RECUSADO"
        and liberado
        and a["reserva_id"] is None
        and dlq_antes == dlq_depois
    )
    registrar("C5", "Recusa de negócio: compensação e DLQ inalterada", ok, evid)


def c8_fraude(svc, broker):
    dlq_antes = broker.fila(DLQ)[0]
    numero = assentos_livres()[0]
    status, resp, _ = reservar(numero, CARTAO_FRAUDE)
    rid = resp["reserva_id"]
    cid = curto(rid)
    chegou = esperar(lambda: broker.fila(DLQ)[0] >= dlq_antes + 1, 30)
    # Sem loop: espera mais 2 ciclos de pagamento e confere que nada voltou para q.pagamentos.
    time.sleep(2 * float(os.getenv("PAYMENT_DELAY", "2")) + 1)
    pag_ready, pag_cons = broker.fila("q.pagamentos")
    dlq_depois = broker.fila(DLQ)[0]
    nacks = linhas_log("payment_service*.log", f"reserva={cid}")

    mensagens = broker.espiar(DLQ, lambda corpo: rid.encode() in corpo)
    dlq_pos_inspecao = broker.fila(DLQ)[0]
    evid = [
        f"POST assento {numero} cartão 9999... -> HTTP {status}, reserva {cid}",
        *nacks,
        f"q.dlq: antes={dlq_antes} depois={dlq_depois} (após inspeção com requeue: {dlq_pos_inspecao})",
        f"q.pagamentos: Ready={pag_ready} consumidores={pag_cons}",
        f"payment_service vivo: {svc.vivo('payment_service')}",
        f"status da reserva: {status_reserva(rid)} | assento {numero}: {assento(numero)}",
    ]
    x_death_ok = False
    for m in mensagens:
        props = m["props"]
        mortes = (props.headers or {}).get("x-death") or []
        EXTRA.setdefault("props_persistentes", []).append(
            {
                "fila": DLQ,
                "delivery_mode": props.delivery_mode,
                "message_id": props.message_id,
            }
        )
        evid.append(
            f"basic_get q.dlq: routing_key={m['routing_key']} delivery_mode={props.delivery_mode} "
            f"content_type={props.content_type} message_id={props.message_id}"
        )
        for morte in mortes:
            evid.append(
                f"  x-death: queue={morte.get('queue')} reason={morte.get('reason')} "
                f"count={morte.get('count')} exchange={morte.get('exchange')} "
                f"routing-keys={morte.get('routing-keys')}"
            )
            x_death_ok |= (
                morte.get("queue") == "q.pagamentos"
                and morte.get("reason") == "rejected"
            )
        if not mortes:
            evid.append(f"  headers: {props.headers}")
    ok = (
        bool(chegou)
        and dlq_depois == dlq_antes + 1
        and pag_ready == 0
        and pag_cons == 1
        and svc.vivo("payment_service")
        and len(nacks) == 1
        and "NACK -> DLQ" in nacks[0]
        and x_death_ok
        and dlq_pos_inspecao == dlq_depois
    )
    registrar("C8", "DLQ por fraude (nack requeue=False, sem loop, x-death)", ok, evid)


def c9_payload_corrompido(svc, broker):
    dlq_antes = broker.fila(DLQ)[0]
    r = subprocess.run(
        [sys.executable, "-u", "workers/tools/publicar_invalido.py"],
        cwd=RAIZ,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )
    # Pub/Sub: q.assentos, q.notificacoes e q.status assinam reserva.criada, então
    # cada uma recebe uma cópia, rejeita a sua e a DLQ ganha 3 mensagens.
    esperar(lambda: broker.fila(DLQ)[0] >= dlq_antes + 3, 30)
    time.sleep(1)
    dlq_depois = broker.fila(DLQ)[0]
    invalidas = broker.espiar(DLQ, lambda corpo: b"quebrado" in corpo)
    origens = sorted(
        ((m["props"].headers or {}).get("x-death") or [{}])[0].get("queue", "?")
        for m in invalidas
    )
    vivos = {nome: svc.vivo(nome) for nome in WORKERS}
    logs = linhas_log("*_service*.log", "JSON inválido")
    evid = [
        f"publicar_invalido.py exit={r.returncode}: {r.stdout.strip()}",
        f"q.dlq: antes={dlq_antes} depois={dlq_depois} (uma cópia por fila assinante)",
        f"x-death.queue das mensagens corrompidas na DLQ: {origens}",
        *logs,
        f"workers vivos: {vivos}",
        f"consumidores: { {f: broker.consumidores(f) for f in FILAS} }",
    ]
    ok = (
        r.returncode == 0
        and dlq_depois == dlq_antes + 3
        and all(vivos.values())
        and origens == ["q.assentos", "q.notificacoes", "q.status"]
    )
    registrar("C9", "DLQ por payload corrompido, nenhum worker cai", ok, evid)


def c13_frontend():
    st_index, corpo_index, _ = http("GET", "/")
    st_js, corpo_js, _ = http("GET", "/app.js")
    st_css, _, _ = http("GET", "/style.css")
    js = (RAIZ / "frontend" / "app.js").read_text(encoding="utf-8").splitlines()

    def linha(padrao):
        for i, texto in enumerate(js, 1):
            if padrao in texto:
                return f"frontend/app.js:{i}: {texto.strip()}"
        return f"NÃO ENCONTRADO: {padrao}"

    trechos = [
        linha("fetch(`/api/reservas/${reservaId}`)"),
        linha("setInterval(atualizar, 1000)"),
        linha("rajadaEl.addEventListener"),
        linha('fetch("/api/reservas", {'),
        linha("const SEM_NOVIDADE_SEGUNDOS"),
    ]
    tem_botao = 'id="rajada"' in str(corpo_index)
    ok = (
        st_index == 200
        and st_js == 200
        and st_css == 200
        and tem_botao
        and not any(t.startswith("NÃO") for t in trechos)
    )
    registrar(
        "C13",
        "Frontend servido pela API, com polling e botão de rajada",
        ok,
        [
            f'GET / -> HTTP {st_index} (contém o botão id="rajada": {tem_botao})',
            f"GET /app.js -> HTTP {st_js}, GET /style.css -> HTTP {st_css}",
            *trechos,
        ],
    )


def c10_corrida(svc, broker):
    subir_worker(svc, broker, "assentos_service", 2)
    numero = assentos_livres()[0]
    barreira = threading.Barrier(2)
    respostas = [None, None]

    def disparar(i):
        barreira.wait()  # as duas threads saem juntas
        respostas[i] = reservar(
            numero, nome=f"Pessoa {'AB'[i]}", email=f"pessoa{i + 1}@exemplo.com"
        )

    threads = [threading.Thread(target=disparar, args=(i,)) for i in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    rids = [r[1].get("reserva_id") for r in respostas if r[0] == 202]
    finais = {rid: esperar_status(rid, FINAIS, 40)[0] for rid in rids}
    a = assento(numero)
    evid = [
        f"POST simultâneos para o assento {numero}: "
        + ", ".join(f"HTTP {r[0]} em {r[2] * 1000:.0f} ms" for r in respostas)
    ]
    for rid, final in finais.items():
        linhas = linhas_log(
            "assentos_service*.log", f"<- reserva.criada reserva={curto(rid)}"
        )
        evid.append(
            f"reserva {curto(rid)} -> {final} | {linhas[0] if linhas else 'sem log'}"
        )
    vendidos = sql(
        "SELECT COUNT(*) AS n FROM ingressos i JOIN reservas r ON r.id = i.reserva_id "
        "WHERE r.evento_id = 1 AND r.numero = ?",
        numero,
    )[0]["n"]
    pagamentos = sql(
        "SELECT reserva_id, resultado FROM pagamentos WHERE reserva_id IN (?, ?)",
        *(rids + [None, None])[:2],
    )
    vencedor = [rid for rid, f in finais.items() if f == "CONFIRMADA"]
    evid += [
        f"assento {numero}: {a}",
        f"ingressos para o assento: {vendidos}",
        f"pagamentos das 2 reservas: {pagamentos}",
    ]
    pids = {
        re.search(r"pid=(\d+)", l).group(1)
        for rid in rids
        for l in linhas_log(
            "assentos_service*.log", f"<- reserva.criada reserva={curto(rid)}"
        )
    }
    evid.append(
        f"pids do assentos_service que processaram: {sorted(pids)} "
        f"(réplicas: {svc.pid_real('assentos_service', 1)}, {svc.pid_real('assentos_service', 2)})"
    )
    ok = (
        len(rids) == 2
        and sorted(finais.values()) == ["ASSENTO_INDISPONIVEL", "CONFIRMADA"]
        and len(vencedor) == 1
        and a["status"] == "VENDIDO"
        and a["reserva_id"] == vencedor[0]
        and vendidos == 1
        and len(pagamentos) == 1
    )
    registrar("C10", "Corrida pelo mesmo assento com 2 assentos_service", ok, evid)
    derrubar_worker(svc, broker, "assentos_service", 2)


def c6_queda(svc, broker):
    derrubar_worker(svc, broker, "payment_service", 1)
    antes = broker.contagens()
    rids = []
    for numero in assentos_livres()[:3]:
        status, resp, _ = reservar(numero)
        rids.append(resp["reserva_id"])
    retidas = esperar(
        lambda: broker.fila("q.pagamentos")[0] == antes["q.pagamentos"] + 3, 30
    )
    meio = broker.fila("q.pagamentos")
    status_meio = [status_reserva(r) for r in rids]
    subir_worker(svc, broker, "payment_service", 1)
    esvaziou = esperar(lambda: broker.fila("q.pagamentos")[0] == 0, 40)
    finais = [esperar_status(r, FINAIS, 40)[0] for r in rids]
    evid = [
        f"payment_service derrubado; 3 reservas: {[curto(r) for r in rids]}",
        f"q.pagamentos com o serviço caído: Ready={meio[0]} consumidores={meio[1]}",
        f"status com o serviço caído: {status_meio}",
        f"depois de religar: q.pagamentos Ready={broker.fila('q.pagamentos')[0]}",
        f"status finais: {finais}",
        *[
            l
            for r in rids
            for l in linhas_log("payment_service*.log", f"reserva={curto(r)}")
        ],
    ]
    ok = (
        bool(retidas)
        and meio[0] == 3
        and bool(esvaziou)
        and finais == ["CONFIRMADA"] * 3
    )
    registrar(
        "C6",
        "Queda do payment_service: mensagens retidas e processadas ao religar",
        ok,
        evid,
    )


def c7_ack_manual(svc, broker, mgmt, mgmt_ok):
    # Réplica lenta (8 s) para dar tempo de derrubar no meio do pagamento.
    derrubar_worker(svc, broker, "payment_service", 1)
    subir_worker(svc, broker, "payment_service", 1, {"PAYMENT_DELAY": "8"})
    numero = assentos_livres()[0]
    status, resp, _ = reservar(numero)
    rid = resp["reserva_id"]
    cid = curto(rid)
    # AGUARDANDO_PAGAMENTO + Ready=0 em q.pagamentos = a mensagem está com o worker, sem ack.
    entregue = esperar(
        lambda: status_reserva(rid) == "AGUARDANDO_PAGAMENTO"
        and broker.fila("q.pagamentos")[0] == 0,
        20,
    )
    unacked = None
    if mgmt_ok:
        try:
            d = mgmt.get("queues", "{vh}", "q.pagamentos")
            unacked = d.get("messages_unacknowledged", d.get("unacked"))
        except Exception:  # noqa: BLE001
            pass
    time.sleep(1.5)
    pid_morto = svc.pid_real("payment_service", 1)
    derrubar_worker(svc, broker, "payment_service", 1)
    voltou = esperar(lambda: broker.fila("q.pagamentos")[0] == 1, 20)
    ready_pos_queda = broker.fila("q.pagamentos")[0]
    cobrancas_pos_queda = sql(
        "SELECT COUNT(*) AS n FROM pagamentos WHERE reserva_id = ?", rid
    )[0]["n"]
    subir_worker(svc, broker, "payment_service", 1)
    final, _ = esperar_status(rid, FINAIS, 40)
    cobrancas = sql("SELECT COUNT(*) AS n FROM pagamentos WHERE reserva_id = ?", rid)[
        0
    ]["n"]
    ingressos = sql("SELECT COUNT(*) AS n FROM ingressos WHERE reserva_id = ?", rid)[0][
        "n"
    ]
    logs = linhas_log("payment_service*.log", f"reserva={cid}")
    evid = [
        f"reserva {cid} entregue ao payment_service pid={pid_morto} (PAYMENT_DELAY=8): {bool(entregue)}, "
        f"unacked no painel: {unacked}",
        f"kill no meio do pagamento -> q.pagamentos Ready={ready_pos_queda} (mensagem voltou)",
        f"pagamentos gravados logo após a queda: {cobrancas_pos_queda}",
        f"depois de religar (pid={svc.pid_real('payment_service', 1)}): status={final}, "
        f"pagamentos={cobrancas}, ingressos={ingressos}",
        *logs,
    ]
    ok = (
        bool(entregue)
        and bool(voltou)
        and cobrancas_pos_queda == 0
        and final == "CONFIRMADA"
        and cobrancas == 1
        and ingressos == 1
        and len(logs) == 1
        and pid_morto not in logs[0]
    )
    registrar(
        "C7",
        "Ack manual: worker morto no meio, mensagem reentregue e processada 1 vez",
        ok,
        evid,
    )


def c11_competing_consumers(svc, broker):
    subir_worker(svc, broker, "payment_service", 2)
    pids = {svc.pid_real("payment_service", 1), svc.pid_real("payment_service", 2)}
    # Mesmo comportamento do botão "Teste de rajada" (frontend/app.js): pega 10
    # assentos LIVRE pela API e dispara 10 POST /api/reservas ao mesmo tempo.
    _, assentos, _ = http("GET", "/api/eventos/1/assentos")
    livres = [a["numero"] for a in assentos if a["status"] == "LIVRE"][:10]
    respostas = [None] * len(livres)
    barreira = threading.Barrier(len(livres))

    def disparar(i, numero):
        barreira.wait()
        respostas[i] = reservar(
            numero, CARTAO_OK, f"Rajada {i + 1}", f"rajada{i + 1}@exemplo.com"
        )

    threads = [
        threading.Thread(target=disparar, args=(i, n)) for i, n in enumerate(livres)
    ]
    t0 = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    envio = time.perf_counter() - t0
    rids = [r[1]["reserva_id"] for r in respostas if r[0] == 202]
    finais = [esperar_status(r, FINAIS, 60)[0] for r in rids]
    esperar(
        lambda: all(
            linhas_log("payment_service*.log", f"reserva={curto(r)}") for r in rids
        ),
        10,
    )
    por_pid = {}
    for r in rids:
        for linha in linhas_log(
            "payment_service*.log", f"<- assento.reservado reserva={curto(r)}"
        ):
            pid = re.search(r"pid=(\d+)", linha).group(1)
            por_pid[pid] = por_pid.get(pid, 0) + 1
    tempos = sorted(r[2] for r in respostas)
    evid = [
        f"réplicas do payment_service: {sorted(pids)} (consumidores em q.pagamentos: "
        f"{broker.consumidores('q.pagamentos')})",
        f"rajada: {len(livres)} POSTs simultâneos, {len(rids)} HTTP 202, enviados em {envio:.2f} s "
        f"(resposta: mín {tempos[0] * 1000:.0f} ms, máx {tempos[-1] * 1000:.0f} ms)",
        f"status finais: { {s: finais.count(s) for s in set(finais)} }",
        f"mensagens processadas por pid: {por_pid}",
    ]
    evid += [
        l
        for r in rids
        for l in linhas_log(
            "payment_service*.log", f"<- assento.reservado reserva={curto(r)}"
        )
    ]
    ok = (
        len(rids) == 10
        and all(f in FINAIS for f in finais)
        and len(por_pid) == 2
        and set(por_pid) == pids
        and sum(por_pid.values()) == 10
    )
    registrar(
        "C11", "Competing consumers: rajada de 10 dividida entre 2 réplicas", ok, evid
    )


def c12_idempotencia(svc, broker):
    dlq_antes = broker.fila(DLQ)[0]
    numero = assentos_livres()[0]
    _, resp, _ = reservar(numero)
    rid = resp["reserva_id"]
    cid = curto(rid)
    final, _ = esperar_status(rid, FINAIS, 40)
    esperar(lambda: linhas_log("emissao_service*.log", f"reserva={cid}"), 10)
    codigo_antes = sql("SELECT codigo FROM ingressos WHERE reserva_id = ?", rid)

    # Mesmo envelope (mesmo event_id/message_id) publicado 2x, como numa reentrega.
    envelope = common.build_envelope(
        "assento.reservado",
        rid,
        {
            "evento_id": 1,
            "numero": numero,
            "valor_centavos": 8000,
            "nome": "Teste E2E",
            "email": "e2e@exemplo.com",
            "cartao": CARTAO_OK,
        },
    )
    corpo = json.dumps(envelope).encode("utf-8")
    props = pika.BasicProperties(
        delivery_mode=pika.DeliveryMode.Persistent,
        content_type="application/json",
        message_id=envelope["event_id"],
        type="assento.reservado",
    )
    broker.publicar_bruto("assento.reservado", corpo, props)
    broker.publicar_bruto("assento.reservado", corpo, props)
    # 1 processamento original + 2 duplicatas, em pagamento e em emissão.
    esperar(
        lambda: len(linhas_log("payment_service*.log", f"reserva={cid}")) >= 3
        and len(linhas_log("emissao_service*.log", f"reserva={cid}")) >= 3,
        40,
    )
    time.sleep(1)
    pagamentos = sql("SELECT resultado FROM pagamentos WHERE reserva_id = ?", rid)
    ingressos = sql("SELECT codigo FROM ingressos WHERE reserva_id = ?", rid)
    dlq_depois = broker.fila(DLQ)[0]
    evid = [
        f"reserva {cid} -> {final}; ingresso antes da duplicata: {codigo_antes}",
        f"envelope assento.reservado event_id={curto(envelope['event_id'])} publicado 2x",
        *linhas_log("payment_service*.log", f"reserva={cid}"),
        *linhas_log("emissao_service*.log", f"reserva={cid}"),
        *linhas_log("assentos_service*.log", f"reserva={cid}: assento já"),
        f"linhas em pagamentos: {len(pagamentos)} | linhas em ingressos: {len(ingressos)} {ingressos}",
        f"status final: {status_reserva(rid)} | q.dlq antes={dlq_antes} depois={dlq_depois}",
    ]
    ok = (
        final == "CONFIRMADA"
        and len(pagamentos) == 1
        and len(ingressos) == 1
        and ingressos == codigo_antes
        and dlq_antes == dlq_depois
        and len(linhas_log("payment_service*.log", f"reserva={cid}")) == 3
    )
    registrar(
        "C12",
        "Idempotência: assento.reservado duplicado não cobra nem emite de novo",
        ok,
        evid,
    )


def durabilidade(mgmt_ok):
    evid = []
    filas = EXTRA.get("mgmt_filas", {})
    for fila, d in filas.items():
        evid.append(f"{fila}: durable={d['durable']}")
    for p in EXTRA.get("props_persistentes", []):
        evid.append(
            f"mensagem lida da {p['fila']} com basic_get: delivery_mode={p['delivery_mode']} "
            f"message_id={p['message_id']}"
        )
    evid.append(
        "sem restart do broker (plano gratuito do CloudAMQP): verificado por propriedades"
    )
    ok = (
        mgmt_ok
        and filas
        and all(d["durable"] for d in filas.values())
        and EXTRA.get("props_persistentes")
        and all(p["delivery_mode"] == 2 for p in EXTRA["props_persistentes"])
    )
    registrar(
        "DUR", "Durabilidade (filas durable + delivery_mode=2), sem restart", ok, evid
    )


def idle_api(segundos):
    info(
        f"deixando a API ociosa por {segundos} s (heartbeat negociado: {EXTRA.get('heartbeat')} s)..."
    )
    time.sleep(segundos)
    numero = assentos_livres()[0]
    status, resp, seg = reservar(numero)
    rid = resp.get("reserva_id") if isinstance(resp, dict) else None
    final = esperar_status(rid, FINAIS, 40)[0] if rid else None
    # O log da API diz se a conexão do publicador tinha caído e foi reaberta.
    registrar(
        "IDLE",
        f"API publica depois de {segundos} s ociosa (reconexão)",
        status == 202 and final == "CONFIRMADA",
        [
            f"POST após {segundos} s sem publicar -> HTTP {status} em {seg * 1000:.0f} ms, status final {final}",
            *linhas_log("api.log", "] publica"),
        ],
    )  # "publicador ..." e "publicação falhou ..."


# --------------------------------------------------------------------------- #
# Roteiro
# --------------------------------------------------------------------------- #
def executar(nome, funcao, *args):
    """Um teste que quebra vira FAIL com o motivo, e o roteiro continua."""
    print(f"\n--- {nome} ---", flush=True)
    try:
        funcao(*args)
    except Exception as exc:  # noqa: BLE001
        registrar(
            nome,
            "erro ao executar o teste",
            False,
            [f"{type(exc).__name__}: {exc}", *traceback.format_exc().splitlines()[-6:]],
        )


def main():
    parser = argparse.ArgumentParser(
        description="Verificação de ponta a ponta (purga filas e reseta o banco)."
    )
    parser.add_argument(
        "--sim",
        action="store_true",
        help="confirma que pode purgar as filas e resetar o banco",
    )
    parser.add_argument(
        "--idle",
        type=int,
        default=0,
        help="segundos de API ociosa antes do teste IDLE (0 = pula)",
    )
    parser.add_argument(
        "--manter-filas", action="store_true", help="não purga as filas no final"
    )
    parser.add_argument(
        "--sem-backup",
        action="store_true",
        help="não salva as mensagens antes do 1º purge",
    )
    args = parser.parse_args()

    print(
        "ATENÇÃO: este teste PURGA todas as filas do broker do .env e RESETA o banco SQLite."
    )
    if not args.sim:
        if (
            not sys.stdin.isatty()
            or input("Digite SIM para continuar: ").strip().upper() != "SIM"
        ):
            sys.exit("Cancelado. Use --sim para confirmar.")
    if not common.AMQP_URL:
        sys.exit("AMQP_URL não definida no .env.")
    if porta_ocupada():
        sys.exit("A porta 5000 já está em uso: desligue a API que estiver rodando.")

    LOGS.mkdir(exist_ok=True)
    for antigo in LOGS.glob("*.log"):
        antigo.unlink()

    broker, mgmt, svc = Broker(), Gerenciamento(), Servicos()
    try:
        ocupadas = {f: broker.consumidores(f) for f in FILAS_TODAS}
        if any(ocupadas.values()):
            sys.exit(
                f"PARE: há consumidores conectados ({ocupadas}). Alguém está usando o broker."
            )

        parte_a()

        antes = broker.contagens()
        print(f"\nfilas antes do 1º purge: {fmt_filas(antes)}")
        if any(antes.values()) and not args.sem_backup:
            destino = LOGS / f"backup_filas_{datetime.now():%Y%m%d_%H%M%S}.json"
            n = broker.backup(destino)
            print(f"backup de {n} mensagem(ns) em {destino.relative_to(RAIZ)}")
            if n != sum(antes.values()) or broker.contagens() != antes:
                sys.exit("Backup não bate com as contagens: nada foi purgado.")
        broker.purgar()
        print(f"filas depois do purge: {fmt_filas(broker.contagens())}")

        mgmt_ok = mgmt.disponivel()
        executar("B", parte_b, broker, mgmt, mgmt_ok)

        db.reset()
        subir_api(svc)
        executar("C2", c2_validacao, broker)
        executar("C1/C3", c1_c3_resposta_imediata, broker)

        print("\n--- purge + reset; subindo todos os workers ---", flush=True)
        broker.purgar()
        db.reset()
        for nome in WORKERS:
            subir_worker(svc, broker, nome)

        executar("C4", c4_fluxo_normal, svc)
        executar("C5", c5_recusa, broker)
        executar("C8", c8_fraude, svc, broker)
        executar("C9", c9_payload_corrompido, svc, broker)
        executar("C13", c13_frontend)
        executar("C10", c10_corrida, svc, broker)
        executar("C6", c6_queda, svc, broker)
        executar("C7", c7_ack_manual, svc, broker, mgmt, mgmt_ok)
        executar("C11", c11_competing_consumers, svc, broker)
        executar("C12", c12_idempotencia, svc, broker)
        executar("DUR", durabilidade, mgmt_ok)
        if args.idle:
            executar("IDLE", idle_api, args.idle)
        EXTRA["filas_no_fim"] = broker.contagens()
    finally:
        print("\n--- limpeza ---", flush=True)
        svc.derrubar_todos()
        try:
            if not args.manter_filas:
                broker.purgar()
            print(f"filas no fim: {fmt_filas(broker.contagens())}")
        except Exception as exc:  # noqa: BLE001
            print(f"aviso: não consegui purgar/consultar no fim: {exc!r}")
        broker.fechar()

        (LOGS / "e2e_resultado.json").write_text(
            json.dumps(
                {
                    "quando": datetime.now().isoformat(timespec="seconds"),
                    "resultados": RESULTADOS,
                    "extra": {k: v for k, v in EXTRA.items() if k != "mgmt_filas"},
                },
                ensure_ascii=False,
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )

        print("\n================ RESUMO ================")
        for r in RESULTADOS:
            print(f"{'PASS' if r['ok'] else 'FAIL'}  {r['id']:<6} {r['titulo']}")
        aprovados = sum(r["ok"] for r in RESULTADOS)
        print(
            f"\n{aprovados}/{len(RESULTADOS)} PASS  (detalhes em logs/e2e_resultado.json)"
        )


if __name__ == "__main__":
    main()
