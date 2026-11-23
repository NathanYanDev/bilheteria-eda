"""assentos_service: bloqueia, vende e libera assentos (Fase 4).

Uso:  python workers/assentos_service.py   (pode rodar mais de uma instância)
"""

import db
from common import ErroProcessamento, log, publish_event, run_consumer, short_id

SERVICE = "assentos_service"
QUEUE = "q.assentos"


# --------------------------------------------------------------------------- #
# Validação do payload
# --------------------------------------------------------------------------- #
# Campo faltando ou com tipo errado é erro permanente: reprocessar não resolve.
# ErroProcessamento faz o run_consumer dar nack(requeue=False) -> DLQ.
def _ler_reserva_id(envelope):
    # Sem reserva_id o assento ficaria BLOQUEADO com dono NULL: ninguém venderia nem liberaria.
    reserva_id = envelope["reserva_id"]
    if not isinstance(reserva_id, str) or not reserva_id.strip():
        raise ErroProcessamento("envelope sem reserva_id válido")
    return reserva_id


def _inteiro(valor):
    # Aceita só inteiro de verdade (ou texto só com dígitos). int(1.9) viraria 1 e
    # int(True) viraria 1: reservaria o assento errado sem ninguém perceber.
    if isinstance(valor, bool):
        raise ValueError
    if isinstance(valor, int):
        return valor
    if isinstance(valor, str) and valor.strip().isdigit():
        return int(valor)
    raise ValueError


def _ler_assento(payload):
    try:
        return _inteiro(payload["evento_id"]), _inteiro(payload["numero"])
    except (KeyError, ValueError):
        raise ErroProcessamento("payload sem evento_id/numero válidos")


def _ler_texto(payload, campo):
    valor = payload.get(campo)
    if not isinstance(valor, str) or not valor.strip():
        raise ErroProcessamento(f"payload sem '{campo}'")
    return valor


# --------------------------------------------------------------------------- #
# reserva.criada -> tenta bloquear o assento
# --------------------------------------------------------------------------- #
def reserva_criada(channel, envelope):
    reserva_id = _ler_reserva_id(envelope)
    payload = envelope["payload"]
    evento_id, numero = _ler_assento(payload)
    # Valida tudo ANTES de mexer no banco: assim um payload ruim vai para a DLQ
    # sem deixar nenhum assento bloqueado pela metade.
    dados_cliente = {campo: _ler_texto(payload, campo) for campo in ("nome", "email", "cartao")}

    conn = db.connect()
    try:
        # A reserva tem que existir (a API grava antes de publicar) e ser para este
        # mesmo assento. Mensagem "fantasma" bloquearia um assento para sempre.
        reserva = conn.execute(
            "SELECT evento_id, numero FROM reservas WHERE id = ?", (reserva_id,)
        ).fetchone()
        if reserva is None or (reserva["evento_id"], reserva["numero"]) != (evento_id, numero):
            raise ErroProcessamento("reserva inexistente ou de outro assento")

        # Proteção contra reentrega tardia: se esta reserva já passou pelo
        # pagamento (ex.: foi recusada e o assento liberado), não bloqueia de novo.
        ja_pago = conn.execute(
            "SELECT 1 FROM pagamentos WHERE reserva_id = ?", (reserva_id,)
        ).fetchone()
        if ja_pago:
            log(SERVICE, f"reserva já processada pelo pagamento, ignorando assento {numero}")
            return None

        # Reserva atômica: um único UPDATE condicional, nunca "ler e depois escrever".
        # O SQLite executa uma escrita por vez; com duas reservas (ou duas instâncias
        # do serviço) disputando o mesmo assento, só uma encontra status='LIVRE'.
        # O rowcount diz quem ganhou: 1 = bloqueou, 0 = alguém chegou antes.
        cur = conn.execute(
            """
            UPDATE assentos SET status = 'BLOQUEADO', reserva_id = ?
            WHERE evento_id = ? AND numero = ? AND status = 'LIVRE'
            """,
            (reserva_id, evento_id, numero),
        )
        conn.commit()
        conseguiu = cur.rowcount == 1

        if not conseguiu:
            atual = conn.execute(
                "SELECT status, reserva_id FROM assentos WHERE evento_id = ? AND numero = ?",
                (evento_id, numero),
            ).fetchone()
            if atual is None:
                raise ErroProcessamento(f"assento {numero} do evento {evento_id} não existe")

            # Idempotência (entrega at-least-once): se o worker caiu depois do UPDATE
            # e antes do ack, a mensagem volta. O assento já é desta reserva -> sucesso.
            if atual["reserva_id"] == reserva_id:
                if atual["status"] == "VENDIDO":
                    return None  # o fluxo já avançou até a venda; nada a republicar
                conseguiu = True

        preco = conn.execute(
            "SELECT preco_centavos FROM eventos WHERE id = ?", (evento_id,)
        ).fetchone()
    finally:
        conn.close()

    # Publicar só depois do commit (ordem: processar -> publicar -> ack).
    # Se o publish falhar, não há ack: a mensagem volta e cai na idempotência acima.
    if conseguiu:
        # O cartão completo segue só na mensagem, porque o payment_service aplica
        # as regras de cartão fictício. No banco continuam só os 4 últimos dígitos.
        publish_event(channel, "assento.reservado", reserva_id, {
            "evento_id": evento_id,
            "numero": numero,
            "valor_centavos": preco["preco_centavos"] if preco else None,
            **dados_cliente,
        })
        return "assento.reservado"

    publish_event(channel, "assento.indisponivel", reserva_id, {
        "evento_id": evento_id,
        "numero": numero,
        "nome": dados_cliente["nome"],
        "email": dados_cliente["email"],
        "motivo": "assento já reservado por outra pessoa",
    })
    return "assento.indisponivel"


# --------------------------------------------------------------------------- #
# pagamento.aprovado -> BLOQUEADO vira VENDIDO
# --------------------------------------------------------------------------- #
def pagamento_aprovado(channel, envelope):
    reserva_id = _ler_reserva_id(envelope)
    conn = db.connect()
    try:
        # Busca o assento pelo reserva_id (não pelo número): só o dono do bloqueio vende.
        cur = conn.execute(
            "UPDATE assentos SET status = 'VENDIDO' WHERE reserva_id = ? AND status = 'BLOQUEADO'",
            (reserva_id,),
        )
        conn.commit()
        if cur.rowcount == 1:
            log(SERVICE, f"reserva={short_id(reserva_id)}: assento BLOQUEADO -> VENDIDO")
            return None

        # Idempotência: mensagem repetida, o assento já foi vendido para esta reserva.
        vendido = conn.execute(
            "SELECT 1 FROM assentos WHERE reserva_id = ? AND status = 'VENDIDO'", (reserva_id,)
        ).fetchone()
    finally:
        conn.close()

    if vendido:
        log(SERVICE, f"reserva={short_id(reserva_id)}: assento já estava VENDIDO (mensagem repetida)")
        return None
    # Pagamento aprovado sem assento bloqueado é inconsistência: guarda na DLQ para análise.
    raise ErroProcessamento("nenhum assento bloqueado para esta reserva")


# --------------------------------------------------------------------------- #
# pagamento.recusado -> compensação: libera o assento
# --------------------------------------------------------------------------- #
def pagamento_recusado(channel, envelope):
    reserva_id = _ler_reserva_id(envelope)
    conn = db.connect()
    try:
        # Desfaz o bloqueio (saga com compensação) para outra pessoa poder comprar.
        # O filtro por reserva_id garante que só liberamos o bloqueio DESTA reserva.
        cur = conn.execute(
            """
            UPDATE assentos SET status = 'LIVRE', reserva_id = NULL
            WHERE reserva_id = ? AND status = 'BLOQUEADO'
            """,
            (reserva_id,),
        )
        conn.commit()
    finally:
        conn.close()

    # rowcount 0 = já liberado antes (mensagem repetida). Resultado é o mesmo: ack.
    if cur.rowcount == 1:
        log(SERVICE, f"reserva={short_id(reserva_id)}: pagamento recusado, assento BLOQUEADO -> LIVRE")
    else:
        log(SERVICE, f"reserva={short_id(reserva_id)}: assento já estava liberado (mensagem repetida)")
    return None


# --------------------------------------------------------------------------- #
# Roteamento: a fila q.assentos recebe 3 tipos de evento
# --------------------------------------------------------------------------- #
HANDLERS = {
    "reserva.criada": reserva_criada,
    "pagamento.aprovado": pagamento_aprovado,
    "pagamento.recusado": pagamento_recusado,
}


def processar(channel, envelope):
    handler = HANDLERS.get(envelope["event_type"])
    if handler is None:
        raise ErroProcessamento(f"evento não tratado: {envelope['event_type']}")
    return handler(channel, envelope)


if __name__ == "__main__":
    # ACK manual, prefetch=1, nack para DLQ e reconexão ficam no run_consumer (common.py).
    run_consumer(SERVICE, QUEUE, processar)
