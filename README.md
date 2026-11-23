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

A API é iniciada a partir da raiz (`python api/app.py`). Ela coloca a pasta `workers/` no `sys.path` e importa `db` e `common` como módulos soltos, do mesmo jeito que os scripts dos workers. Não há pacote instalável.

### Testes automatizados e atalhos (Fases 4 e 5)

```bash
python -m unittest discover -s testes -v   # testes unitários (não precisam do broker nem do banco real)
python testes/antes_de_testar.py           # mostra filas/consumidores: LIVRE, ATENÇÃO ou PARE
python testes/subir_tudo.py                # API + assentos_service + 2 payment_service; Ctrl+C desliga tudo
```

- O broker é compartilhado, mas cada integrante tem o próprio SQLite: **uma pessoa testa por vez** (rode o `antes_de_testar.py` antes).
- Na página, a seção "Testes de assento e pagamento" tem botões de cartão aprovado, recusado (`...0000`), fraude (`9999...`) e disputa de 2 pessoas no mesmo assento. Eles acompanham a cor do assento, então funcionam antes da Fase 6.

## Testes de demonstração

> _A preencher (Fase 7)._

1. Fluxo normal: reserva → 202 imediato → `CONFIRMADA`
2. Caos: worker de pagamento desligado → mensagens retidas em `q.pagamentos`
3. DLQ: cartão `9999...` e payload inválido → `q.dlq`
4. Escala: 2 réplicas de `payment_service` + botão de rajada
