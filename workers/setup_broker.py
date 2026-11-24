"""
Declara TODA a topologia do broker: exchanges, filas e bindings.

É a fonte da verdade da topologia.
Pode rodar quantas vezes quiser: declarar algo que já existe com os mesmos
parâmetros não faz nada (operação idempotente).

Uso:  python workers/setup_broker.py
"""

import sys

import pika

from common import DLX, EXCHANGE, get_connection

DLQ = "q.dlq"

# fila -> routing keys ligadas (binding) na exchange topic `ingressos.events`.
# Uma mesma routing key em várias filas = pub/sub: cada fila recebe sua cópia.
FILAS = {
    "q.assentos": ["reserva.criada", "pagamento.aprovado", "pagamento.recusado"],
    "q.pagamentos": ["assento.reservado"],
    "q.emissao": ["pagamento.aprovado"],
    "q.notificacoes": [
        "reserva.criada",
        "assento.indisponivel",
        "pagamento.recusado",
        "ingresso.emitido",
    ],
    "q.status": ["#"],  # '#' casa com qualquer routing key: recebe todos os eventos
}


def main():
    connection = get_connection("setup_broker")
    channel = connection.channel()

    # Exchanges durable: continuam existindo depois de um restart do broker.
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    channel.exchange_declare(exchange=DLX, exchange_type="fanout", durable=True)
    print(f"exchange {EXCHANGE} (topic, durable)")
    print(f"exchange {DLX} (fanout, durable)")

    # DLQ: recebe tudo que chegar na DLX. Fanout ignora routing key, por isso
    # o binding é feito sem ela. A própria DLQ não tem DLX (não há para onde ir).
    channel.queue_declare(queue=DLQ, durable=True)
    channel.queue_bind(queue=DLQ, exchange=DLX)
    print(f"fila {DLQ:<15} <- {DLX}")

    # Filas de trabalho: durable + x-dead-letter-exchange. Quando um consumidor
    # faz basic_nack(requeue=False), o broker republica a mensagem na DLX.
    for fila, routing_keys in FILAS.items():
        channel.queue_declare(
            queue=fila,
            durable=True,
            arguments={"x-dead-letter-exchange": DLX},
        )
        for rk in routing_keys:
            channel.queue_bind(queue=fila, exchange=EXCHANGE, routing_key=rk)
        print(f"fila {fila:<15} <- {EXCHANGE} [{', '.join(routing_keys)}]")

    connection.close()
    print("\nTopologia pronta. Confira em Exchanges/Queues no painel do CloudAMQP.")


if __name__ == "__main__":
    try:
        main()
    except pika.exceptions.ChannelClosedByBroker as exc:
        if exc.reply_code == 406:  # PRECONDITION_FAILED
            sys.exit(
                f"\nERRO: {exc.reply_text}\n"
                "Uma fila/exchange já existe com parâmetros diferentes. Apague-a no painel "
                "do CloudAMQP e rode este script de novo."
            )
        raise
    except pika.exceptions.AMQPConnectionError as exc:
        sys.exit(
            f"Não foi possível conectar ao broker: {exc!r}\nConfira a AMQP_URL no .env."
        )
