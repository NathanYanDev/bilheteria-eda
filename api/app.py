"""
API HTTP (produtor). Não espera os workers.

Fluxo de POST /api/reservas:
  valida -> grava EM_PROCESSAMENTO -> publica reserva.criada (publisher confirm) -> 202.

A resposta sai assim que o broker confirma a mensagem. Quem bloqueia o assento,
cobra e emite o ingresso são os workers, de forma assíncrona.

Import: este arquivo é executado como `python api/app.py` a partir da raiz.
A pasta workers/ entra no sys.path para importar `db` e `common` como módulos
soltos (o mesmo jeito dos scripts `python workers/...`). Não há pacote instalável.

pika não é thread-safe. O Flask atende vários requests ao mesmo tempo, então
existe UMA conexão/canal, e todo publish passa por um threading.Lock. Se o
canal ocioso cair (heartbeat), a próxima publicação abre outro.
"""

import re
import sys
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pika
from flask import Flask, jsonify, request, send_from_directory

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR / "workers"))

import common  # noqa: E402
import db  # noqa: E402

FRONTEND = BASE_DIR / "frontend"

app = Flask(__name__)

STATUS_FINAIS = {"CONFIRMADA", "ASSENTO_INDISPONIVEL", "PAGAMENTO_RECUSADO"}
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# Uma conexão só: pika.BlockingConnection não pode ser usada por duas threads.
_pub_lock = threading.Lock()
_connection = None
_channel = None


def _fechar_publicador():
    global _connection, _channel
    canal = _channel
    conn = _connection
    _channel = None
    _connection = None
    for recurso in (canal, conn):
        if recurso is None:
            continue
        try:
            recurso.close()
        except pika.exceptions.AMQPError:
            pass


def _canal():
    """Devolve o canal aberto, reconectando se o anterior tiver caído."""
    global _connection, _channel
    if _channel is not None and _channel.is_open:
        return _channel
    _fechar_publicador()
    _connection = common.get_connection("api")
    _channel = _connection.channel()
    # confirm_delivery: basic_publish só retorna depois que o broker gravou
    # a mensagem. Sem isso, o 202 poderia sair e a mensagem se perder no caminho.
    _channel.confirm_delivery()
    return _channel


def publicar_reserva_criada(reserva_id, payload):
    """Publica com confirmação. Uma falha de conexão tenta de novo uma vez."""
    with _pub_lock:
        ultimo = None
        for _ in (1, 2):
            try:
                common.publish_event(_canal(), "reserva.criada", reserva_id, payload)
                return
            except pika.exceptions.AMQPError as exc:
                ultimo = exc
                _fechar_publicador()
        raise ultimo


def _erro(mensagem, status):
    return jsonify({"erro": mensagem}), status


def _validar_reserva(corpo):
    """Devolve (dados, None) ou (None, (mensagem, status_http)).

    Não publica e não grava. Payload inválido tem que terminar em 400 antes
    de qualquer efeito colateral.
    """
    if not isinstance(corpo, dict):
        return None, ("JSON inválido.", 400)

    try:
        evento_id = int(corpo.get("evento_id"))
        numero = int(corpo.get("numero"))
    except (TypeError, ValueError):
        return None, ("Informe evento_id e numero do assento.", 400)

    nome = str(corpo.get("nome") or "").strip()
    email = str(corpo.get("email") or "").strip()
    cartao = re.sub(r"\s+", "", str(corpo.get("cartao") or ""))

    if len(nome) < 2:
        return None, ("Informe o nome.", 400)
    if not EMAIL_RE.match(email):
        return None, ("Informe um e-mail válido.", 400)
    if not re.fullmatch(r"\d{16}", cartao):
        return None, ("O cartão deve ter 16 dígitos (use um número fictício).", 400)

    conn = db.connect()
    try:
        evento = conn.execute("SELECT id FROM eventos WHERE id = ?", (evento_id,)).fetchone()
        if evento is None:
            return None, ("Evento não encontrado.", 400)
        assento = conn.execute(
            "SELECT numero FROM assentos WHERE evento_id = ? AND numero = ?",
            (evento_id, numero),
        ).fetchone()
        if assento is None:
            return None, ("Assento não encontrado.", 400)
    finally:
        conn.close()

    return {
        "evento_id": evento_id,
        "numero": numero,
        "nome": nome,
        "email": email,
        "cartao": cartao,
    }, None


@app.get("/")
def index():
    return send_from_directory(FRONTEND, "index.html")


@app.get("/app.js")
def arquivo_js():
    return send_from_directory(FRONTEND, "app.js")


@app.get("/style.css")
def arquivo_css():
    return send_from_directory(FRONTEND, "style.css")


@app.get("/api/eventos")
def listar_eventos():
    conn = db.connect()
    try:
        rows = conn.execute(
            "SELECT id, nome, local, data_hora, preco_centavos FROM eventos ORDER BY id"
        ).fetchall()
    finally:
        conn.close()
    return jsonify([dict(r) for r in rows])


@app.get("/api/eventos/<int:evento_id>/assentos")
def listar_assentos(evento_id):
    conn = db.connect()
    try:
        evento = conn.execute("SELECT id FROM eventos WHERE id = ?", (evento_id,)).fetchone()
        if evento is None:
            return _erro("Evento não encontrado.", 404)
        rows = conn.execute(
            """
            SELECT numero, status
            FROM assentos
            WHERE evento_id = ?
            ORDER BY numero
            """,
            (evento_id,),
        ).fetchall()
    finally:
        conn.close()
    return jsonify([dict(r) for r in rows])


@app.post("/api/reservas")
def criar_reserva():
    corpo = request.get_json(silent=True)
    dados, falha = _validar_reserva(corpo)
    if falha:
        return _erro(*falha)

    if not common.AMQP_URL:
        return _erro("AMQP_URL não definida. Copie .env.example para .env e preencha a URL do CloudAMQP.", 503)

    reserva_id = str(uuid.uuid4())
    criado_em = datetime.now(timezone.utc).isoformat()
    # O número completo só viaja na mensagem (o payment_service precisa dele
    # para as regras de cartão fictício). No banco ficam só os 4 últimos.
    cartao_final = dados["cartao"][-4:]

    conn = db.connect()
    try:
        conn.execute(
            """
            INSERT INTO reservas (id, evento_id, numero, nome, email, cartao_final, status, criado_em)
            VALUES (?, ?, ?, ?, ?, ?, 'EM_PROCESSAMENTO', ?)
            """,
            (
                reserva_id,
                dados["evento_id"],
                dados["numero"],
                dados["nome"],
                dados["email"],
                cartao_final,
                criado_em,
            ),
        )
        conn.commit()
        try:
            publicar_reserva_criada(
                reserva_id,
                {
                    "evento_id": dados["evento_id"],
                    "numero": dados["numero"],
                    "nome": dados["nome"],
                    "email": dados["email"],
                    "cartao": dados["cartao"],
                },
            )
        except pika.exceptions.AMQPError:
            # Sem confirm, a reserva não existe para o resto do sistema.
            # Apaga a linha para o cliente poder tentar de novo sem duplicar.
            conn.execute("DELETE FROM reservas WHERE id = ?", (reserva_id,))
            conn.commit()
            common.log("api", f"falha ao publicar reserva.criada reserva={common.short_id(reserva_id)}")
            return _erro("Não foi possível publicar a reserva. Tente de novo.", 503)
    finally:
        conn.close()

    common.log("api", f"-> reserva.criada reserva={common.short_id(reserva_id)} | 202")
    return jsonify({"reserva_id": reserva_id, "status": "EM_PROCESSAMENTO"}), 202


@app.get("/api/reservas/<reserva_id>")
def obter_reserva(reserva_id):
    conn = db.connect()
    try:
        row = conn.execute(
            """
            SELECT id, evento_id, numero, nome, email, status, criado_em
            FROM reservas
            WHERE id = ?
            """,
            (reserva_id,),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return _erro("Reserva não encontrada.", 404)
    dados = dict(row)
    dados["reserva_id"] = dados.pop("id")
    dados["final"] = dados["status"] in STATUS_FINAIS
    return jsonify(dados)


if __name__ == "__main__":
    # use_reloader=False: o reloader subiria um segundo processo e uma segunda
    # conexão com o broker. threaded=True é o padrão; o Lock cobre o pika.
    app.run(host="127.0.0.1", port=5000, debug=False, threaded=True, use_reloader=False)
