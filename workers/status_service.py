"""status_service: mantém reservas.status em dia a partir dos eventos (Fase 6).

A fila q.status recebe TUDO (binding '#'). O status só avança, nunca regride.

Uso:  python workers/status_service.py
"""

import db
from common import ErroProcessamento, log, run_consumer, short_id

SERVICE = "status_service"
QUEUE = "q.status"

# Ranking: o status novo só vale se o rank for MAIOR que o atual. Assim um evento
# que chega fora de ordem (ou repetido) é ignorado. Os dois finais alternativos
# têm o mesmo rank do CONFIRMADA: depois de qualquer um deles nada mais muda.
RANK = {
    "EM_PROCESSAMENTO": 0,
    "AGUARDANDO_PAGAMENTO": 1,
    "PAGO": 2,
    "CONFIRMADA": 3,
    "ASSENTO_INDISPONIVEL": 3,
    "PAGAMENTO_RECUSADO": 3,
}

# event_type -> status da reserva
STATUS_DO_EVENTO = {
    "reserva.criada": "EM_PROCESSAMENTO",
    "assento.reservado": "AGUARDANDO_PAGAMENTO",
    "pagamento.aprovado": "PAGO",
    "ingresso.emitido": "CONFIRMADA",
    "assento.indisponivel": "ASSENTO_INDISPONIVEL",
    "pagamento.recusado": "PAGAMENTO_RECUSADO",
}


def processar(channel, envelope):
    novo = STATUS_DO_EVENTO.get(envelope["event_type"])
    if novo is None:
        return None  # a fila recebe '#': evento que não muda status é só ignorado (ack)
    reserva_id = envelope["reserva_id"]
    if not isinstance(reserva_id, str) or not reserva_id.strip():
        raise ErroProcessamento("envelope sem reserva_id válido")

    anteriores = [s for s, r in RANK.items() if r < RANK[novo]]
    conn = db.connect()
    try:
        # Um único UPDATE condicional (mesma ideia da reserva de assento): a checagem
        # "status atual tem rank menor" e a escrita acontecem juntas, sem corrida.
        cur = conn.execute(
            f"UPDATE reservas SET status = ? WHERE id = ? AND status IN ({','.join('?' * len(anteriores))})",
            (novo, reserva_id, *anteriores),
        )
        conn.commit()
    finally:
        conn.close()

    if cur.rowcount:
        log(SERVICE, f"reserva={short_id(reserva_id)}: status -> {novo}")
    else:
        log(SERVICE, f"reserva={short_id(reserva_id)}: {novo} ignorado (já avançou, repetido ou reserva inexistente)")
    return None


if __name__ == "__main__":
    run_consumer(SERVICE, QUEUE, processar)
