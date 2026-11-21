"""
Código compartilhado por todos os workers (e pela API):
  - carregamento do .env e conexão com o RabbitMQ (CloudAMQP)
  - montagem do envelope e publicação persistente de eventos
  - log padronizado com nome do serviço + pid
  - run_consumer(): loop de consumo com ACK manual, prefetch=1 e reconexão

A topologia (exchanges, filas, bindings) NÃO é declarada aqui: a fonte da
verdade é setup_broker.py. Os consumidores só consomem filas que já existem.
"""

import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pika
from dotenv import load_dotenv

# Carrega o .env da raiz do projeto, independente de onde o script foi chamado.
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

AMQP_URL = os.getenv("AMQP_URL", "").strip()

# Nomes das exchanges (usados também pelo setup_broker.py).
EXCHANGE = "ingressos.events"  # topic: roteia pelo event_type (routing key)
DLX = "ingressos.dlx"          # fanout: recebe tudo que for rejeitado (nack requeue=False)

# Tempo de espera antes de tentar reconectar quando a conexão cai.
RECONNECT_DELAY = 5


# --------------------------------------------------------------------------- #
# Log
# --------------------------------------------------------------------------- #
def log(service, msg):
    """Imprime `[servico pid=1234] msg`.

    O pid identifica a réplica: com 2 payment_service rodando, é ele que mostra
    qual processo pegou cada mensagem (competing consumers). flush=True para o
    texto aparecer na hora no terminal, sem ficar preso no buffer.
    """
    print(f"[{service} pid={os.getpid()}] {msg}", flush=True)


def short_id(value):
    """Primeiros 6 caracteres de um uuid, só para deixar o log legível."""
    return str(value)[:6] if value else "?"


# --------------------------------------------------------------------------- #
# Conexão
# --------------------------------------------------------------------------- #
def get_connection(name=None):
    """Abre uma BlockingConnection com o broker definido em AMQP_URL.

    `name` aparece na aba "Connections" do painel do CloudAMQP, o que ajuda a
    ver quantas réplicas de cada serviço estão conectadas.
    """
    if not AMQP_URL:
        sys.exit("AMQP_URL não definida. Copie .env.example para .env e preencha a URL do CloudAMQP.")

    params = pika.URLParameters(AMQP_URL)
    if name:
        params.client_properties = {"connection_name": f"{name} pid={os.getpid()}"}
    return pika.BlockingConnection(params)


# --------------------------------------------------------------------------- #
# Publicação
# --------------------------------------------------------------------------- #
def build_envelope(event_type, reserva_id, payload):
    """Monta o envelope padrão (ver CLAUDE.md). event_type == routing key."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "reserva_id": reserva_id,
        "payload": payload or {},
    }


def publish_event(channel, event_type, reserva_id, payload):
    """Publica um evento em `ingressos.events` usando o event_type como routing key.

    - delivery_mode=2 (Persistent): a mensagem é gravada em disco pelo broker.
      Junto com filas durable, sobrevive a um restart do RabbitMQ.
    - message_id = event_id: identifica a mensagem de forma única (útil para
      rastrear no painel e para idempotência).
    - Se o canal estiver em modo confirm (confirm_delivery), basic_publish só
      retorna depois que o broker confirmou o recebimento.

    Retorna o envelope publicado.
    """
    envelope = build_envelope(event_type, reserva_id, payload)
    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key=event_type,
        body=json.dumps(envelope, ensure_ascii=False).encode("utf-8"),
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            content_type="application/json",
            message_id=envelope["event_id"],
            type=event_type,
        ),
    )
    return envelope


# --------------------------------------------------------------------------- #
# Consumo
# --------------------------------------------------------------------------- #
class ErroProcessamento(Exception):
    """Erro permanente: a mensagem nunca vai dar certo (payload inválido, fraude...).

    O run_consumer responde com basic_nack(requeue=False) e a mensagem vai para
    a DLQ. Não faz sentido devolvê-la à fila (requeue=True): ela falharia de novo
    para sempre, num loop infinito.
    """


CAMPOS_ENVELOPE = ("event_id", "event_type", "reserva_id", "payload")


def _parse_envelope(body):
    """Converte o corpo em dict e confere os campos obrigatórios do envelope."""
    try:
        envelope = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ErroProcessamento(f"JSON inválido ({exc})")
    if not isinstance(envelope, dict):
        raise ErroProcessamento("envelope não é um objeto JSON")
    faltando = [c for c in CAMPOS_ENVELOPE if c not in envelope]
    if faltando:
        raise ErroProcessamento(f"campos faltando no envelope: {', '.join(faltando)}")
    if not isinstance(envelope["payload"], dict):
        raise ErroProcessamento("payload não é um objeto JSON")
    return envelope


def run_consumer(service, queue, callback):
    """Consome `queue` para sempre, chamando `callback(channel, envelope)`.

    Contrato do callback:
      - faz a regra de negócio e publica o(s) próximo(s) evento(s) com publish_event;
      - retorna o event_type publicado (str), uma lista deles, ou None;
      - lança ErroProcessamento para erros permanentes (vai para a DLQ).

    Garantias de confiabilidade implementadas aqui (para cada serviço não ter
    que repetir):
      - auto_ack=False: o broker só apaga a mensagem quando recebe nosso ack.
        Se o worker cair no meio, a mensagem volta para a fila (at-least-once).
      - ACK só depois que o callback terminou, ou seja, depois de processar E
        publicar o evento seguinte. Ordem: processar -> publicar -> ack.
      - prefetch_count=1: o broker entrega no máximo 1 mensagem não confirmada
        por consumidor. Assim uma réplica ocupada não acumula mensagens e a
        carga se divide de forma justa entre as réplicas.
      - confirm_delivery(): o publish do callback só retorna quando o broker
        confirmou o recebimento; então, ao dar ack, o próximo evento já está
        seguro no broker.
      - Reconexão: se a conexão cair, espera RECONNECT_DELAY segundos e volta.
        Mensagens que estavam sem ack são reentregues pelo broker.
    """

    def on_message(channel, method, properties, body):
        tag = method.delivery_tag
        entrada = f"<- {method.routing_key}"
        try:
            envelope = _parse_envelope(body)
            entrada += f" reserva={short_id(envelope['reserva_id'])}"
            publicados = callback(channel, envelope)
        except ErroProcessamento as exc:
            channel.basic_nack(delivery_tag=tag, requeue=False)
            log(service, f"{entrada}  |  ERRO: {exc}  |  NACK -> DLQ")
            return
        except pika.exceptions.AMQPError:
            # Problema de conexão/canal (ex.: o publish falhou). Não dá para
            # dar ack nem nack; sobe para o loop de reconexão. Como não houve
            # ack, o broker vai reentregar esta mensagem.
            raise
        except Exception as exc:  # bug ou erro inesperado
            # Também vai para a DLQ: devolver para a fila poderia gerar um loop
            # infinito, e na DLQ a mensagem fica guardada para inspeção.
            channel.basic_nack(delivery_tag=tag, requeue=False)
            log(service, f"{entrada}  |  ERRO inesperado: {exc!r}  |  NACK -> DLQ")
            return

        channel.basic_ack(delivery_tag=tag)
        if isinstance(publicados, (list, tuple)):
            publicados = ", ".join(publicados)
        saida = f"-> {publicados}" if publicados else "(nada publicado)"
        log(service, f"{entrada}  |  {saida}  |  ACK")

    while True:
        connection = None
        try:
            connection = get_connection(service)
            channel = connection.channel()
            channel.confirm_delivery()
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=queue, on_message_callback=on_message, auto_ack=False)
            log(service, f"aguardando mensagens em '{queue}' (Ctrl+C para sair)")
            channel.start_consuming()
        except KeyboardInterrupt:
            log(service, "encerrando...")
            if connection and connection.is_open:
                connection.close()
            break
        except pika.exceptions.ChannelClosedByBroker as exc:
            if exc.reply_code == 404:
                log(service, f"fila '{queue}' não existe. Rode: python workers/setup_broker.py")
            else:
                log(service, f"canal fechado pelo broker: {exc}")
        except pika.exceptions.AMQPError as exc:
            log(service, f"conexão perdida: {exc!r}")
        finally:
            if connection and connection.is_open:
                try:
                    connection.close()
                except pika.exceptions.AMQPError:
                    pass
        log(service, f"reconectando em {RECONNECT_DELAY} s...")
        time.sleep(RECONNECT_DELAY)
