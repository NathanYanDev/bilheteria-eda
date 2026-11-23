"""emissao_service: gera o código do ingresso depois do pagamento aprovado (Fase 6).

Uso:  python workers/emissao_service.py
"""

import secrets
import sqlite3
import string
from datetime import datetime, timezone

import db
from common import ErroProcessamento, publish_event, run_consumer

SERVICE = "emissao_service"
QUEUE = "q.emissao"
ALFABETO = string.ascii_uppercase + string.digits


def _novo_codigo():
    return "BIL-" + "".join(secrets.choice(ALFABETO) for _ in range(6))


def _codigo_da_reserva(conn, reserva_id):
    row = conn.execute("SELECT codigo FROM ingressos WHERE reserva_id = ?", (reserva_id,)).fetchone()
    return row["codigo"] if row else None


def processar(channel, envelope):
    if envelope["event_type"] != "pagamento.aprovado":
        raise ErroProcessamento(f"evento não tratado: {envelope['event_type']}")
    reserva_id = envelope["reserva_id"]
    if not isinstance(reserva_id, str) or not reserva_id.strip():
        raise ErroProcessamento("envelope sem reserva_id válido")

    conn = db.connect()
    try:
        reserva = conn.execute(
            "SELECT evento_id, numero, nome, email FROM reservas WHERE id = ?", (reserva_id,)
        ).fetchone()
        if reserva is None:
            raise ErroProcessamento("reserva inexistente")

        # Idempotência: reserva_id é UNIQUE em ingressos. Se a mensagem voltou (worker
        # caiu antes do ack), o ingresso já existe: republica o MESMO código, sem emitir outro.
        codigo = _codigo_da_reserva(conn, reserva_id)
        while codigo is None:
            try:
                conn.execute(
                    "INSERT INTO ingressos (codigo, reserva_id, emitido_em) VALUES (?, ?, ?)",
                    (_novo_codigo(), reserva_id, datetime.now(timezone.utc).isoformat()),
                )
                conn.commit()
            except sqlite3.IntegrityError:
                # Colisão de código (raro) ou outra réplica emitiu agora: lê de novo e decide.
                conn.rollback()
            codigo = _codigo_da_reserva(conn, reserva_id)
    finally:
        conn.close()

    # Ordem: processar (gravou) -> publicar -> ack (feito pelo run_consumer).
    publish_event(channel, "ingresso.emitido", reserva_id, {
        "evento_id": reserva["evento_id"],
        "numero": reserva["numero"],
        "nome": reserva["nome"],
        "email": reserva["email"],
        "codigo": codigo,
    })
    return "ingresso.emitido"


if __name__ == "__main__":
    run_consumer(SERVICE, QUEUE, processar)
