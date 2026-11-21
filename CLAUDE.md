# bilheteria-eda

Sistema de **reserva de ingressos** com Arquitetura Orientada a Eventos (EDA) usando RabbitMQ (CloudAMQP).
Trabalho em grupo (4 pessoas) da disciplina **Programação Distribuída, Paralela e Concorrente**: Atividade 04 – Message Broker.

O foco da avaliação **não é o visual**. São avaliados: desacoplamento, comunicação assíncrona, roteamento,
confiabilidade (durabilidade + ACK manual), tolerância a falhas (DLQ) e escalabilidade horizontal (competing consumers).

## Contexto de aprendizado

Os integrantes são estudantes. Ao implementar:
- Explique **o porquê** das decisões ligadas a concorrência, mensageria e confiabilidade (comentários curtos no código + resumo ao final da tarefa).
- Não adicione dependências, frameworks ou abstrações além do necessário. Código simples e legível > código "esperto".
- Trabalhe **uma fase do KICKOFF.md por vez** e pare para revisão ao final de cada fase.

## Stack

- Python 3.10+
- API: **Flask** (também serve os arquivos estáticos de `/frontend`, então não precisa de CORS)
- Mensageria: **pika** (`BlockingConnection`) com RabbitMQ no **CloudAMQP**
- Banco: **SQLite** compartilhado em `data/bilheteria.db` (modo WAL, `timeout=10`)
- Frontend: HTML + JavaScript puro (`fetch`), sem build
- Config: `python-dotenv`; a URL do broker fica em `AMQP_URL` no `.env` (**nunca commitar o `.env`**)

## Estrutura

```
/frontend   index.html, app.js, style.css
/api        app.py
/workers    common.py            # conexão, publish, decorador de consumo, logs
            db.py                # conexão SQLite, schema, seed
            setup_broker.py      # declara TODA a topologia (exchanges, filas, bindings)
            assentos_service.py
            payment_service.py
            emissao_service.py
            notificacao_service.py
            status_service.py
            tools/publicar_invalido.py   # publica payload corrompido para testar a DLQ
/data       banco SQLite (gerado, no .gitignore)
requirements.txt · README.md · .env.example
```

A API importa `workers/db.py` e `workers/common.py` (ajustar `sys.path` ou rodar como pacote; escolher uma forma e documentar no README).

## Topologia do broker (fonte da verdade: `setup_broker.py`)

- Exchange principal: `ingressos.events`, tipo **topic**, durable
- Dead Letter Exchange: `ingressos.dlx`, tipo **fanout**, durable
- Toda fila é `durable=True` e declarada com `arguments={"x-dead-letter-exchange": "ingressos.dlx"}` (exceto a própria `q.dlq`)

| Fila | Bindings em `ingressos.events` | Consumidor | Publica |
|---|---|---|---|
| `q.assentos` | `reserva.criada`, `pagamento.aprovado`, `pagamento.recusado` | assentos_service | `assento.reservado` / `assento.indisponivel` |
| `q.pagamentos` | `assento.reservado` | payment_service (2+ réplicas) | `pagamento.aprovado` / `pagamento.recusado` |
| `q.emissao` | `pagamento.aprovado` | emissao_service | `ingresso.emitido` |
| `q.notificacoes` | `reserva.criada`, `assento.indisponivel`, `pagamento.recusado`, `ingresso.emitido` | notificacao_service | (nenhum) |
| `q.status` | `#` | status_service | (nenhum; atualiza `reservas.status`) |
| `q.dlq` | bind em `ingressos.dlx` | ninguém (inspeção pelo painel) | (nenhum) |

**Ao mudar a topologia**, atualize esta tabela, o README e o diagrama.
Atenção: redeclarar uma fila existente com argumentos diferentes gera `PRECONDITION_FAILED`. Nesse caso, apagar a fila no painel do CloudAMQP e rodar `setup_broker.py` de novo.

## Regras obrigatórias (não violar)

1. **Publicação persistente**: toda mensagem com `delivery_mode=2` (`pika.DeliveryMode.Persistent`), `content_type="application/json"` e `message_id`.
2. **Proibido `auto_ack=True`**. Consumidores fazem `basic_ack` **somente depois** de concluir a regra de negócio **e** publicar o evento seguinte (ordem: processar → publicar → ack).
3. **Erro de processamento** (JSON inválido, campos faltando, cartão "fraude") → `basic_nack(delivery_tag, requeue=False)` → vai para a DLQ. Nunca `requeue=True` para erro permanente (gera loop infinito).
4. **Recusa de negócio não é erro**: sem limite → publica `pagamento.recusado` + `basic_ack`.
5. **`basic_qos(prefetch_count=1)`** em todos os consumidores (distribuição justa entre réplicas).
6. **Reserva de assento atômica**: nunca "ler e depois escrever". Usar um único
   `UPDATE assentos SET status='BLOQUEADO', reserva_id=? WHERE evento_id=? AND numero=? AND status='LIVRE'`
   e decidir pelo `rowcount`.
7. **Idempotência** (a entrega é *at-least-once*: uma mensagem pode chegar duas vezes se o worker cair antes do ack):
   - assentos: se `rowcount == 0` mas o assento já está bloqueado **pela mesma reserva** → tratar como sucesso.
   - pagamentos: tabela `pagamentos` com `reserva_id` PRIMARY KEY; reprocessamento não cobra de novo.
   - emissão: `ingressos.reserva_id` UNIQUE.
8. **API não espera os workers**: valida → grava reserva com status `EM_PROCESSAMENTO` → publica `reserva.criada` → responde **202** imediatamente.
9. **pika não é thread-safe**: na API, usar uma única conexão/canal protegida por `threading.Lock`, com reconexão automática se a publicação falhar (o canal ocioso pode cair por heartbeat). Ativar `confirm_delivery()` para ter certeza de que o broker recebeu antes de responder 202.
10. **Dados fictícios**: nunca usar cartões reais. Não guardar o número completo do cartão no banco (guardar só os 4 últimos dígitos).

## Formato das mensagens (envelope)

```json
{
  "event_id": "uuid4",
  "event_type": "reserva.criada",
  "occurred_at": "ISO-8601 UTC",
  "reserva_id": "uuid4",
  "payload": { "...": "dados do evento" }
}
```
`event_type` == routing key. Helpers em `common.py` montam o envelope e publicam.

## Regras de simulação de pagamento (`payment_service`)

| Cartão (fictício) | Resultado |
|---|---|
| termina em `0000` | `pagamento.recusado` (sem limite) + ack |
| começa com `9999` | erro de processamento (fraude) → `basic_nack(requeue=False)` → DLQ |
| outros | `pagamento.aprovado` + ack |

Atraso artificial de ~2 s por pagamento (`PAYMENT_DELAY` no `.env`) para a divisão de carga ficar visível.

## Status da reserva (`status_service`, só avança, nunca regride)

`EM_PROCESSAMENTO` → `AGUARDANDO_PAGAMENTO` → `PAGO` → `CONFIRMADA`
Finais alternativos: `ASSENTO_INDISPONIVEL`, `PAGAMENTO_RECUSADO`.
Implementar com um ranking numérico para ignorar eventos que chegarem fora de ordem.

## Logs (servem de evidência no vídeo)

Todo worker imprime com `flush=True` no formato:
`[payment_service pid=1234] <- assento.reservado reserva=ab12cd  |  -> pagamento.aprovado  |  ACK`
O `pid` é essencial para mostrar qual réplica processou cada mensagem (Etapa 5).
Usar ícones/cores simples no terminal é opcional.

## Comandos

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env                                # preencher AMQP_URL
python workers/setup_broker.py                      # topologia
python workers/db.py --reset                        # schema + seed
python api/app.py                                   # http://localhost:5000
python workers/assentos_service.py                  # um terminal por worker
python workers/payment_service.py                   # rodar 2x para a Etapa 5
```

## Convenções

- Nomes de domínio em português (`reserva`, `assento`, `ingresso`); termos técnicos em inglês quando forem da API do pika/Flask.
- Mensagens e logs para o usuário em português.
- Commits pequenos, um por fase, em português: `feat: payment_service com DLQ`.
- Antes de considerar uma fase pronta: rodar o fluxo de ponta a ponta e conferir as filas no painel.
