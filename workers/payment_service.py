"""payment_service: simula a cobrança do cartão (Fase 5).

Uso:  python workers/payment_service.py   (rode 2x para dividir a carga)
"""

import os
import re
import time
from datetime import datetime, timezone

import db
from common import ErroProcessamento, publish_event, run_consumer

SERVICE = "payment_service"
QUEUE = "q.pagamentos"


# Atraso artificial: sem ele os pagamentos terminam rápido demais e não dá para
# ver duas réplicas dividindo a rajada no vídeo.
def _ler_atraso():
    try:
        return max(0.0, float(os.getenv("PAYMENT_DELAY", "2")))
    except ValueError:
        return 2.0


PAYMENT_DELAY = _ler_atraso()


# --------------------------------------------------------------------------- #
# Validação do payload
# --------------------------------------------------------------------------- #
# Erro permanente: ErroProcessamento faz o run_consumer dar nack(requeue=False) -> DLQ.
def _validar(envelope):
    reserva_id = envelope["reserva_id"]
    if not isinstance(reserva_id, str) or not reserva_id.strip():
        raise ErroProcessamento("envelope sem reserva_id válido")

    payload = envelope["payload"]
    cartao = payload.get("cartao")
    if not isinstance(cartao, str) or not re.fullmatch(r"\d{16}", cartao):
        raise ErroProcessamento("cartão ausente ou sem 16 dígitos")
    return reserva_id, payload, cartao


# --------------------------------------------------------------------------- #
# Regras do cartão fictício (tabela do CLAUDE.md)
# --------------------------------------------------------------------------- #
def _decidir(cartao):
    # Fraude é ERRO, não recusa: a mensagem vai para a DLQ para alguém analisar.
    # Vem antes do "0000" porque um cartão 9999...0000 é fraude, não falta de limite.
    if cartao.startswith("9999"):
        raise ErroProcessamento("cartão suspeito de fraude")
    # Sem limite é uma resposta normal do negócio: publica a recusa e dá ack.
    if cartao.endswith("0000"):
        return "RECUSADO"
    return "APROVADO"


def _resultado_gravado(conn, reserva_id):
    row = conn.execute(
        "SELECT resultado FROM pagamentos WHERE reserva_id = ?", (reserva_id,)
    ).fetchone()
    return row["resultado"] if row else None


# --------------------------------------------------------------------------- #
# assento.reservado -> cobra e publica aprovado/recusado
# --------------------------------------------------------------------------- #
def processar(channel, envelope):
    if envelope["event_type"] != "assento.reservado":
        raise ErroProcessamento(f"evento não tratado: {envelope['event_type']}")
    reserva_id, payload, cartao = _validar(envelope)

    conn = db.connect()
    try:
        reserva = conn.execute(
            "SELECT evento_id, numero, nome, email, cartao_final FROM reservas WHERE id = ?",
            (reserva_id,),
        ).fetchone()
        if reserva is None:
            raise ErroProcessamento("reserva inexistente")
        # Idempotência: se a mensagem voltou (worker caiu antes do ack), a cobrança
        # já está gravada. Republica o mesmo resultado sem cobrar e sem esperar de novo.
        resultado = _resultado_gravado(conn, reserva_id)
    finally:
        # Fecha antes do atraso: não faz sentido segurar o banco enquanto "cobra".
        conn.close()

    if resultado is None:
        time.sleep(PAYMENT_DELAY)
        decisao = _decidir(cartao)

        conn = db.connect()
        try:
            # reserva_id é PRIMARY KEY: se outra réplica gravou primeiro (mensagem
            # duplicada processada em paralelo), o OR IGNORE não cria uma 2ª cobrança.
            # Depois lemos o que ficou gravado, e é ESSE resultado que vale para todos.
            conn.execute(
                "INSERT OR IGNORE INTO pagamentos (reserva_id, resultado, processado_em) VALUES (?, ?, ?)",
                (reserva_id, decisao, datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
            resultado = _resultado_gravado(conn, reserva_id)
        finally:
            conn.close()

    # Dados do próximo evento vêm do banco (fonte confiável), não da mensagem.
    # O número completo do cartão para aqui: daqui para frente só os 4 últimos dígitos.
    dados = {
        "evento_id": reserva["evento_id"],
        "numero": reserva["numero"],
        "nome": reserva["nome"],
        "email": reserva["email"],
        "cartao_final": reserva["cartao_final"],
        "valor_centavos": payload.get("valor_centavos"),
    }

    # Ordem: processar -> publicar -> ack (o ack é feito pelo run_consumer).
    if resultado == "APROVADO":
        publish_event(channel, "pagamento.aprovado", reserva_id, dados)
        return "pagamento.aprovado"

    publish_event(channel, "pagamento.recusado", reserva_id, {**dados, "motivo": "cartão sem limite"})
    return "pagamento.recusado"


if __name__ == "__main__":
    # ACK manual, prefetch=1 (divisão justa entre réplicas), DLQ e reconexão: run_consumer.
    run_consumer(SERVICE, QUEUE, processar)
