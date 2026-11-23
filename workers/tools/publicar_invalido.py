"""Publica um JSON corrompido com a routing key reserva.criada (teste da DLQ).

Uso (a partir da raiz):  python workers/tools/publicar_invalido.py
Cada fila que assina reserva.criada (assentos, notificações, status) recebe a
mensagem, falha no parse e faz nack(requeue=False): ela vai para q.dlq.
"""

import sys
import uuid
from pathlib import Path

import pika

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common import EXCHANGE, get_connection  # noqa: E402

conn = get_connection("publicar_invalido")
ch = conn.channel()
ch.confirm_delivery()
ch.basic_publish(
    exchange=EXCHANGE,
    routing_key="reserva.criada",
    body=b'{"event_id": "x", "event_type": "reserva.criada", "payload": {quebrado',  # JSON truncado
    # Corpo quebrado, mas as propriedades seguem a regra de toda publicação:
    # persistente, JSON e com message_id (para achar a mensagem na DLQ).
    properties=pika.BasicProperties(
        delivery_mode=pika.DeliveryMode.Persistent,
        content_type="application/json",
        message_id=str(uuid.uuid4()),
    ),
)
conn.close()
print("JSON inválido publicado em reserva.criada. Confira q.dlq no painel:")
print("chegam 3 cópias, uma de cada fila que assina reserva.criada (assentos, notificações e status).")
