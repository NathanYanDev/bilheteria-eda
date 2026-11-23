# bilheteria-eda

Sistema de reserva de ingressos com **Arquitetura Orientada a Eventos (EDA)** usando RabbitMQ (CloudAMQP).
Atividade 04 (Message Broker) da disciplina Programação Distribuída, Paralela e Concorrente.

## Visão geral

Reserva de ingressos para shows. A API responde **202 na hora** e o resto (bloquear assento, cobrar, emitir ingresso, notificar) acontece em serviços independentes que conversam só por eventos no RabbitMQ. Se um serviço cai, as mensagens ficam retidas na fila e são processadas quando ele volta; se um pagamento falha de forma permanente, a mensagem vai para a DLQ. Num fluxo síncrono, um pagamento lento ou fora do ar travaria a requisição inteira.

## Arquitetura

```
API --reserva.criada--> [ingressos.events (topic)]
  q.assentos     <- reserva.criada, pagamento.aprovado, pagamento.recusado
  q.pagamentos   <- assento.reservado
  q.emissao      <- pagamento.aprovado
  q.notificacoes <- reserva.criada, assento.indisponivel, pagamento.recusado, ingresso.emitido
  q.status       <- #
  (nack requeue=False em qualquer fila) --> [ingressos.dlx (fanout)] --> q.dlq
```

| Fila | Bindings | Consumidor | Publica |
|---|---|---|---|
| `q.assentos` | `reserva.criada`, `pagamento.aprovado`, `pagamento.recusado` | assentos_service | `assento.reservado` / `assento.indisponivel` |
| `q.pagamentos` | `assento.reservado` | payment_service (2+ réplicas) | `pagamento.aprovado` / `pagamento.recusado` |
| `q.emissao` | `pagamento.aprovado` | emissao_service | `ingresso.emitido` |
| `q.notificacoes` | `reserva.criada`, `assento.indisponivel`, `pagamento.recusado`, `ingresso.emitido` | notificacao_service | — |
| `q.status` | `#` | status_service | — |
| `q.dlq` | `ingressos.dlx` | ninguém (painel) | — |

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

Um terminal por serviço (ou `python testes/subir_tudo.py` para ligar tudo):

```bash
python workers/setup_broker.py     # topologia do broker
python workers/db.py --reset       # schema + seed
python api/app.py                  # http://localhost:5000
python workers/assentos_service.py # um terminal por worker
python workers/payment_service.py  # rodar 2x para a demonstração de escala
python workers/emissao_service.py
python workers/notificacao_service.py
python workers/status_service.py
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

## Roteiro da demonstração

1. **Fluxo normal**: no site, escolha assento e cartão `4111111111111111` → 202 imediato → status vai `EM_PROCESSAMENTO` → `AGUARDANDO_PAGAMENTO` → `PAGO` → `CONFIRMADA`. Os terminais mostram a cadeia e o e-mail simulado com o código `BIL-XXXXXX`.
2. **Caos**: Ctrl+C no `payment_service` → faça 3 reservas → `q.pagamentos` mostra 3 em `Ready` no painel → religue o serviço → as 3 são processadas.
3. **DLQ**: reserve com cartão `9999111111111111` (fraude) e rode `python workers/tools/publicar_invalido.py` → ambos aparecem em `q.dlq`; nenhum worker trava. Cartão `...0000` é recusa de negócio (não vai para a DLQ) e libera o assento.
4. **Escala**: 2 terminais com `payment_service` → botão "Teste de rajada" (10 reservas) → os `pid` dos logs se alternam.
5. **Disputa**: dois cliques no mesmo assento → um `assento.reservado`, um `assento.indisponivel`.

Testes unitários: `python -m unittest discover -s testes -v`.
