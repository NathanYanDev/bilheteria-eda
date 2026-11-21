# bilheteria-eda

Sistema de reserva de ingressos com **Arquitetura Orientada a Eventos (EDA)** usando RabbitMQ (CloudAMQP).
Atividade 04 (Message Broker) da disciplina Programação Distribuída, Paralela e Concorrente.

## Visão geral

> _A preencher (Fase 7)._ Domínio, problema resolvido e por que usar mensageria assíncrona.

## Arquitetura

> _A preencher._ Diagrama (exchanges, routing keys, filas, DLQ) e tabela de filas.

- Exchange principal: `ingressos.events` (topic, durable)
- Dead Letter Exchange: `ingressos.dlx` (fanout, durable) → `q.dlq`
- Serviços: `assentos_service`, `payment_service`, `emissao_service`, `notificacao_service`, `status_service`

## Como rodar

### Pré-requisitos

- Python 3.10+
- Uma instância do RabbitMQ no CloudAMQP (a URL `amqps://...`)

### Instalação

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env               # Windows: copy .env.example .env  → preencher AMQP_URL
```

### Execução

> _A preencher conforme as fases forem implementadas._ Ordem prevista:

```bash
python workers/setup_broker.py     # topologia do broker
python workers/db.py --reset       # schema + seed
python api/app.py                  # http://localhost:5000
python workers/assentos_service.py # um terminal por worker
python workers/payment_service.py  # rodar 2x para a demonstração de escala
```

## Testes de demonstração

> _A preencher (Fase 7)._

1. Fluxo normal: reserva → 202 imediato → `CONFIRMADA`
2. Caos: worker de pagamento desligado → mensagens retidas em `q.pagamentos`
3. DLQ: cartão `9999...` e payload inválido → `q.dlq`
4. Escala: 2 réplicas de `payment_service` + botão de rajada
