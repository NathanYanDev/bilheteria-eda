# KICKOFF: bilheteria-eda

Plano de implementação em fases. Execute **uma fase por vez**, revise e faça commit antes de seguir.
As regras técnicas e a topologia estão no `CLAUDE.md`. Este arquivo diz **o que fazer e em que ordem**.

---

## Antes de abrir o Claude Code (feito por uma pessoa do grupo)

- [x] Criar o repositório `bilheteria-eda` (GitHub) e adicionar os outros 3 integrantes
- [x] Colocar `CLAUDE.md` e `KICKOFF.md` na raiz e fazer o primeiro commit
- [x] Criar uma instância gratuita no CloudAMQP e copiar a **AMQP URL** (`amqps://...`)
- [x] Guardar a URL para colocar no `.env` local de cada integrante (não commitar)

## Prompt inicial para colar no Claude Code

> Leia o CLAUDE.md e o KICKOFF.md. Vamos executar a Fase 0. Antes de escrever código, me diga em poucas linhas
> o que vai criar. Ao terminar, explique as decisões e pare para eu revisar.

Para as próximas: `Execute a Fase N do KICKOFF.md.`

---

## Fase 0: Esqueleto do repositório

- [x] Estrutura de pastas do `CLAUDE.md`
- [x] `requirements.txt` (flask, pika, python-dotenv)
- [x] `.env.example` (`AMQP_URL=`, `PAYMENT_DELAY=2`, `DB_PATH=data/bilheteria.db`)
- [x] `.gitignore` (`.env`, `.venv/`, `__pycache__/`, `data/*.db*`)
- [x] `README.md` com esqueleto (seções: visão geral, arquitetura, como rodar, testes de demonstração)

**Pronto quando:** `pip install -r requirements.txt` funciona em um venv limpo.

## Fase 1: Base de mensageria e topologia _(Enunciado, Etapa 1: infraestrutura)_

- [x] `workers/common.py`: carregar `.env`, `get_connection()`, `publish_event(channel, event_type, reserva_id, payload)` com envelope + `delivery_mode=2`, helper de log com nome do serviço + pid, helper `run_consumer(queue, callback)` com `prefetch_count=1`, `auto_ack=False` e loop de reconexão simples
- [x] `workers/setup_broker.py`: declara as 2 exchanges, as 6 filas e todos os bindings da tabela (idempotente, pode rodar várias vezes)

**Pronto quando:** após rodar o setup, o painel do CloudAMQP mostra as exchanges, as filas (com feature `DLX`) e os bindings exatamente como na tabela.

## Fase 2: Banco de dados

- [x] `workers/db.py`: conexão SQLite (WAL, timeout), schema e `--reset` com seed
- [x] Tabelas: `eventos`, `assentos (evento_id, numero, status, reserva_id)`, `reservas`, `pagamentos (reserva_id PK)`, `ingressos (reserva_id UNIQUE)`
- [x] Seed: 2 eventos com 20–30 assentos cada (o suficiente para rajadas de 10)

**Pronto quando:** `python workers/db.py --reset` cria o banco e um SELECT mostra eventos e assentos livres.

## Fase 3: API Flask + Frontend _(Etapa 1: produtor)_

- [x] `GET /api/eventos`, `GET /api/eventos/<id>/assentos`
- [x] `POST /api/reservas`: valida (evento existe, assento existe, nome, e-mail, cartão com 16 dígitos) → grava `EM_PROCESSAMENTO` → publica `reserva.criada` (com publisher confirm) → **202** com `reserva_id`. Payload inválido → 400 **sem** publicar.
- [x] `GET /api/reservas/<id>`: status atual
- [x] Frontend: escolher evento → mapa de assentos (livre/bloqueado/vendido) → formulário → mostra "Reserva recebida / Em processamento" na hora → polling do status a cada 1 s até um status final
- [x] Botão **"Teste de rajada"**: dispara 10 reservas para assentos livres diferentes (necessário para a Etapa 5, já que o enunciado exige a rajada _pela aplicação web_)

**Pronto quando:** uma reserva retorna 202 em menos de 1 s e a mensagem aparece em `Ready` nas filas `q.assentos`, `q.notificacoes` e `q.status` (workers ainda desligados).

## Fase 4: assentos_service _(Etapas 2 e 3)_

- [x] `reserva.criada` → UPDATE atômico → publica `assento.reservado` ou `assento.indisponivel`
- [x] `pagamento.aprovado` → BLOQUEADO → VENDIDO
- [x] `pagamento.recusado` → libera o assento (compensação)
- [x] Idempotência conforme o `CLAUDE.md`; payload inválido → nack para DLQ

**Pronto quando:** duas reservas para o **mesmo assento** resultam em uma `assento.reservado` e uma `assento.indisponivel`, mesmo com duas instâncias do serviço rodando.

## Fase 5: payment_service _(Etapas 2, 4 e 5)_

- [x] Consome `assento.reservado`, aplica as regras de cartão da tabela do `CLAUDE.md`, com atraso de `PAYMENT_DELAY`
- [x] Recusa → `pagamento.recusado` + ack; fraude/payload inválido → `basic_nack(requeue=False)`
- [x] Log com pid em cada mensagem

**Pronto quando:** cartão `...0000` libera o assento, cartão `9999...` aparece em `q.dlq`, e duas instâncias dividem uma rajada de 10.

## Fase 6: emissao_service, notificacao_service, status_service _(Etapa 2)_

- [x] emissão: gera código de ingresso (ex.: `BIL-XXXXXX`), grava, publica `ingresso.emitido`
- [x] notificação: imprime um "e-mail" simulado por evento recebido
- [x] status: mapeia cada evento para o status da reserva (só avança)
- [x] `workers/tools/publicar_invalido.py`: publica um JSON corrompido com routing key `reserva.criada`

**Pronto quando:** o frontend mostra a reserva indo até `CONFIRMADA`, e o script inválido termina na `q.dlq` sem travar nenhum worker.

## Fase 7: Roteiro de testes e README

- [x] README completo: pré-requisitos, `.env`, ordem de execução, comandos de cada teste
- [x] Seção "Roteiro da demonstração" com os 5 testes do vídeo (abaixo)
- [ ] Revisão final com o checklist de requisitos

---

## Checklist de requisitos do enunciado

| Requisito                                   | Onde está                              | Ok  |
| ------------------------------------------- | -------------------------------------- | --- |
| Frontend + API HTTP                         | Fase 3                                 | [ ] |
| Resposta imediata (sem esperar workers)     | `POST /api/reservas` → 202             | [ ] |
| Exchanges, filas e bindings explícitos      | `setup_broker.py`                      | [ ] |
| Pub/Sub: um evento → vários consumidores    | `reserva.criada`, `pagamento.aprovado` | [ ] |
| Coreografia encadeada                       | assentos → pagamento → emissão         | [ ] |
| `durable=True` + `delivery_mode=2`          | setup + `common.py`                    | [ ] |
| Sem `auto_ack`, ack após a regra de negócio | todos os workers                       | [ ] |
| Mensagens retidas com worker caído          | teste de caos                          | [ ] |
| DLX + DLQ com `requeue=False`               | fraude / payload inválido              | [ ] |
| 2+ réplicas na mesma fila + rajada 8–10     | payment_service ×2 + botão de rajada   | [ ] |

## Roteiro de demonstração (vídeo, 10–15 min, todos falam)

1. **Arquitetura** (Pessoa 1): domínio + diagrama (exchanges, routing keys, filas, DLQ)
2. **Fluxo normal** (Pessoa 2): reserva no site → 202 imediato → terminais em cadeia → `CONFIRMADA`
3. **Caos** (Pessoa 3): Ctrl+C no `payment_service` → 3 reservas → painel mostra `Ready` em `q.pagamentos` → religar → tudo processado
4. **DLQ** (Pessoa 4): cartão `9999...` + `publicar_invalido.py` → mensagens em `q.dlq`
5. **Escala + conclusão** (Pessoa 4): 2 terminais `payment_service` → botão de rajada → pids alternando → lições aprendidas

## Entregáveis (prazo: 28/11/2026)

- [ ] `.zip` do projeto (sem `.env`, `.venv` e banco)
- [ ] `.mp4` (ou link não listado)
- [ ] Relatório PDF (máx. 6 páginas): diagrama, tabela de filas, prints comentados (caos, DLQ, escala), comparação síncrono × assíncrono
