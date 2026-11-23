"""notificacao_service: imprime um "e-mail" simulado para cada evento (Fase 6).

Não publica nada e não mexe no banco: é só mais um assinante do mesmo evento
(Pub/Sub). Se este serviço cair, o resto do fluxo continua funcionando.

Uso:  python workers/notificacao_service.py
"""

from common import ErroProcessamento, log, run_consumer, short_id

SERVICE = "notificacao_service"
QUEUE = "q.notificacoes"

# event_type -> assunto do e-mail simulado
ASSUNTOS = {
    "reserva.criada": "Recebemos sua reserva",
    "assento.indisponivel": "Assento indisponível",
    "pagamento.recusado": "Pagamento recusado",
    "ingresso.emitido": "Seu ingresso está confirmado",
}


def processar(channel, envelope):
    assunto = ASSUNTOS.get(envelope["event_type"])
    if assunto is None:
        raise ErroProcessamento(f"evento não tratado: {envelope['event_type']}")

    payload = envelope["payload"]
    email, nome = payload.get("email"), payload.get("nome")
    if not isinstance(email, str) or not email.strip() or not isinstance(nome, str) or not nome.strip():
        raise ErroProcessamento("payload sem nome/email")

    extra = f" | código {payload['codigo']}" if payload.get("codigo") else ""
    log(SERVICE, f"EMAIL para {nome} <{email}> | {assunto} | reserva {short_id(envelope['reserva_id'])}{extra}")
    return None


if __name__ == "__main__":
    run_consumer(SERVICE, QUEUE, processar)
