# Resultado dos testes: Atividade 04 (Message Broker)

**Data:** 23/11/2026 · **Commit base:** `1ecd33a` + correções O1–O8 (seção "Correções aplicadas")
**Broker:** instância do `.env` no CloudAMQP. O produto é o **LavinMQ** (compatível com AMQP 0-9-1), não o RabbitMQ: o painel tem outra cara, mas tudo o que o enunciado pede (DLX, `x-death`, prefetch, publisher confirms) funcionou.
**Ambiente:** Windows 11, Python 3.14.3, pika 1.5.0, Flask 3.1.3.
**Como reproduzir:** `python testes/e2e.py --sim` (ver README). Foram três execuções completas no CloudAMQP: **rodada 1** (16 verificações, 15 PASS; o único FAIL era falso positivo do próprio scanner, corrigido), **rodada 2** (17/17, com o teste IDLE de 11 min) e **rodada 3**, **depois das correções** (17/17, com IDLE). E mais o `testes/restart_broker.py` (RST1–RST3) num RabbitMQ local.

> Antes do primeiro purge, as 214 mensagens que estavam paradas no broker (q.emissao 30, q.notificacoes 55, q.status 122, q.dlq 7) foram salvas em `logs/backup_filas_20261123_142707.json`.

## Placar

**13/13 testes funcionais PASS** (C1–C13) · Parte A (11 itens) OK · Parte B (topologia) OK · Durabilidade OK (propriedades no CloudAMQP + **restart real** no RabbitMQ local, RST1–RST3) · Reconexão da API OK (provada no log). **Testes unitários: 85 OK** (81 + 4 novos em `testes/test_common.py`).

## Correções aplicadas

Nenhum requisito tinha falhado. Estas eram as observações da auditoria, todas corrigidas e verificadas na rodada 3:

| # | Gravidade | Problema | Correção | Verificação |
|---|---|---|---|---|
| O1 | Média | Erro **transitório** do SQLite (`database is locked`) caía no `except Exception` e ia para a DLQ: uma reserva válida podia se perder sob contenção. | `workers/common.py`: `sqlite3.OperationalError` agora é tratado como temporário. Não há ack nem nack; a conexão é fechada e o broker reentrega quando o worker reconecta (os 5 s de `RECONNECT_DELAY` servem de espera). Erro permanente continua indo para a DLQ. | `testes/test_common.py`: banco travado → sem ack e sem nack, conexão fechada; fraude e bug → `nack(requeue=False)`; sucesso → ack, `auto_ack=False`, `prefetch=1`. O teste do banco travado falha com o código antigo |
| O2 | Média (demo) | Com fraude, a tela ficava em "Aguardando pagamento" sem explicação, para sempre. | `frontend/app.js`: depois de 20 s sem mudança de status, a tela explica as duas causas possíveis (serviço desligado, e a mensagem espera na fila, ou fraude, e a mensagem está na DLQ). O polling **continua**, para o teste de caos voltar a andar quando o serviço religar. | C13: `frontend/app.js:103 const SEM_NOVIDADE_SEGUNDOS = 20` |
| O3 | Baixa | `logs/` fora do `.gitignore` (backup das filas com mensagens do grupo). | `.gitignore`: `logs/`. | `git status` não lista mais `logs/` |
| O4 | Baixa | `publicar_invalido.py` sem `message_id` (regra 1). | `message_id=str(uuid.uuid4())`; a saída também avisa que chegam 3 cópias na DLQ. | C9 PASS: "chegam 3 cópias, uma de cada fila que assina reserva.criada" |
| O5 | Baixa | O README prometia `PAGO` visível na tela. | Roteiro do README corrigido: `PAGO` dura milissegundos e aparece no terminal do `status_service`. | README, "Roteiro da demonstração", item 1 |
| O6 | Baixa | A API não registrava a reconexão (prova só indireta). | `api/app.py`: loga `publicador conectado`, `publicação falhou (...): reconectando` e `publicador reconectado`. | **IDLE (CloudAMQP, rodada 3):** depois de 660 s ociosa, `publicação falhou (StreamLostError): reconectando` → `publicador reconectado ao broker` → 202 em 280 ms → `CONFIRMADA`. **RST3** (RabbitMQ local): mesma sequência, com a API ligada durante o restart do broker |
| O7 | Info | O C9 coloca +3 na DLQ, não +1. | Explicado no README (item 3 do roteiro) e na saída do `publicar_invalido.py`. | C9 |
| O8 | Info | Duas pastas de teste (`tests/` e `testes/`). | `e2e.py` e `restart_broker.py` movidos para `testes/`; README e docstrings atualizados. O `unittest discover` não pega esses scripts (o nome não começa com `test`). | 85 testes unitários OK; e2e rodado de `testes/` |

## Parte A: auditoria estática

Linhas conforme o código **depois** das correções.

| # | Item | Resultado | Evidência (arquivo:linha) |
|---|---|---|---|
| A1 | Estrutura e README com instruções | OK | `frontend/`, `api/`, `workers/`, `requirements.txt`, `README.md:35-66` (instalação e execução) |
| A2 | Nenhum `auto_ack=True` | OK | único consumo: `workers/common.py:218` (`auto_ack=False`). Scanner do e2e: "auto_ack=True: nenhum" |
| A3 | Filas e exchanges duráveis | OK | exchanges `setup_broker.py:35-36`; DLQ `:42`; filas de trabalho `:49-53` |
| A4 | `delivery_mode=2` em toda publicação | OK | `common.py:106` + `message_id` `:108` (todos os serviços e a API publicam por `publish_event`); `publicar_invalido.py:27-29` (agora com `message_id`, O4) |
| A5 | Ack só depois da regra e da publicação | OK | `common.py:182` chama o callback (processa + publica com confirm, `:216`) e só então `:205` `basic_ack`. Publicação depois do commit: `assentos_service.py:93→122/130`, `payment_service.py:103→121/124`, `emissao_service.py:52→61` |
| A6 | `nack(requeue=False)` nos erros, sem `requeue=True` | OK | `common.py:184` (ErroProcessamento) e `:201` (inesperado). Falhas transitórias, sem ack nem nack → reentrega: erro de canal/conexão `:187-191` e banco travado `:192-197` + `:233` (O1). Nenhuma chamada com `requeue=True` |
| A7 | `prefetch_count=1` | OK | `common.py:217` (vale para os 5 workers) |
| A8 | `x-dead-letter-exchange` + DLQ ligada à DLX | OK | `setup_broker.py:52` e `:43` |
| A9 | Reserva atômica, sem "ler e depois escrever" | OK | `assentos_service.py:86-94` (UPDATE condicional + `rowcount`). As leituras em `:67` (a reserva existe?), `:75` (já pago?) e `:97` (idempotência, **depois** do UPDATE falhar) não decidem quem fica com o assento. Venda/compensação também condicionais: `:148-150`, `:180-185`. Mesmo padrão em `status_service.py:50-53`, `payment_service.py:99-101` (PK + `INSERT OR IGNORE`) e `emissao_service.py:46-56` (UNIQUE) |
| A10 | API: 202 sem esperar, lock, reconexão, confirms | OK | grava e faz commit `app.py:231` → publica `:233` → 202 `:254`; `threading.Lock` `:43/:82`; reconexão `_canal()` `:64` + 2ª tentativa `:84-94` (com log, O6); `confirm_delivery()` `:74` |
| A11 | `.env` ignorado e sem credencial commitada | OK | `.gitignore:2` (e agora `logs/`, O3); `git grep "amqps://"` e `git log --all -S "amqps://"` só acham textos de exemplo (`.env.example:3`, `KICKOFF.md:12`, `README.md:40`) |

## Parte B: topologia no broker

`setup_broker.py` exit=0, depois conferido pela HTTP API de gerenciamento (`/api/exchanges`, `/api/queues/.../bindings`). **Nenhuma diferença em relação à tabela do CLAUDE.md.**

```
exchange ingressos.events: type=topic durable=True
exchange ingressos.dlx: type=fanout durable=True
fila q.assentos: durable=True x-dead-letter-exchange=ingressos.dlx bindings=[ingressos.events:pagamento.aprovado, ingressos.events:pagamento.recusado, ingressos.events:reserva.criada]
fila q.pagamentos: durable=True x-dead-letter-exchange=ingressos.dlx bindings=[ingressos.events:assento.reservado]
fila q.emissao: durable=True x-dead-letter-exchange=ingressos.dlx bindings=[ingressos.events:pagamento.aprovado]
fila q.notificacoes: durable=True x-dead-letter-exchange=ingressos.dlx bindings=[ingressos.events:assento.indisponivel, ingressos.events:ingresso.emitido, ingressos.events:pagamento.recusado, ingressos.events:reserva.criada]
fila q.status: durable=True x-dead-letter-exchange=ingressos.dlx bindings=[ingressos.events:#]
fila q.dlq: durable=True x-dead-letter-exchange=None bindings=[ingressos.dlx:q.dlq]
destinos ligados a ingressos.events: [q.assentos, q.emissao, q.notificacoes, q.pagamentos, q.status]   (nenhum extra)
heartbeat negociado: 300 s
```

## Parte C: requisito → teste → resultado

Logs completos da última rodada em `logs/<servico>[_N].log`. Os trechos abaixo são da rodada 1; as rodadas 2 e 3 (esta depois das correções) repetiram tudo com números equivalentes.

| Requisito do enunciado | Teste | Resultado | Evidência |
|---|---|---|---|
| Etapa 1: resposta imediata, sem esperar workers | **C1** | **PASS** | Consumidores conectados: todos 0. `POST` → `HTTP 202 {'status': 'EM_PROCESSAMENTO'}` em **262 ms** (publicador frio, abre a conexão TLS) e **22 ms** no 2º POST. Rodada 2: 237 ms / 23 ms. `GET /api/reservas/eabcad` → `EM_PROCESSAMENTO` |
| Etapa 1: validação na origem | **C2** | **PASS** | sem e-mail → `400 Informe um e-mail válido.`; cartão `4111` → `400 O cartão deve ter 16 dígitos`; assento 999 → `400 Assento não encontrado.`; evento 99 → `400 Evento não encontrado.` Filas antes = depois = todas 0; linhas em `reservas`: 0 → 0 |
| Etapa 2: Pub/Sub | **C3** | **PASS** | Depois de 2 POSTs sem workers: `q.assentos=2, q.notificacoes=2, q.status=2`; `q.pagamentos=0, q.emissao=0, q.dlq=0` |
| Etapa 2: coreografia encadeada | **C4** | **PASS** | `[assentos_service pid=45536] <- reserva.criada reserva=3c5420 \| -> assento.reservado \| ACK` → `[payment_service pid=24560] <- assento.reservado reserva=3c5420 \| -> pagamento.aprovado \| ACK` → `[emissao_service pid=5028] <- pagamento.aprovado reserva=3c5420 \| -> ingresso.emitido \| ACK` → `[status_service pid=43568] reserva=3c5420: status -> CONFIRMADA` → `[notificacao_service pid=2360] EMAIL ... código BIL-PROVBB`. Assento 1 `VENDIDO` com esta reserva; `ingressos` = `BIL-PROVBB` |
| Etapa 2: recusa de negócio + compensação | **C5** | **PASS** | `[payment_service pid=24560] <- assento.reservado reserva=77215d \| -> pagamento.recusado \| ACK`; `[assentos_service pid=45536] reserva=77215d: pagamento recusado, assento BLOQUEADO -> LIVRE`; assento 2 = `LIVRE, reserva_id=None`; status `PAGAMENTO_RECUSADO`; q.dlq 0 → 0 |
| Etapa 4: mensagens retidas com worker caído | **C6** | **PASS** | payment_service morto (consumidores 0); 3 reservas → `q.pagamentos Ready=3`, todas `AGUARDANDO_PAGAMENTO`. Religado (pid=47048) → Ready=0, as 3 `CONFIRMADA`, 3 linhas `pid=47048 ... ACK` |
| Etapa 3: ack manual de verdade | **C7** | **PASS** | Réplica com `PAYMENT_DELAY=8` (pid=47024) recebe a mensagem: painel `unacked=1`, Ready=0. Kill no meio da espera → `q.pagamentos Ready=1` (a mensagem voltou), `pagamentos` = 0 linhas. Religado (pid=30148) → `CONFIRMADA`, `pagamentos=1`, `ingressos=1`, e uma única linha de log: `[payment_service pid=30148] <- assento.reservado reserva=4526e0 \| -> pagamento.aprovado \| ACK` |
| Etapa 4: DLQ por fraude, sem loop | **C8** | **PASS** | `[payment_service pid=24560] <- assento.reservado reserva=129942 \| ERRO: cartão suspeito de fraude \| NACK -> DLQ` (uma única vez). q.dlq 0 → 1; depois de 2 ciclos de pagamento: `q.pagamentos Ready=0`, consumidor vivo. `basic_get` em q.dlq: `delivery_mode=2 content_type=application/json`, `x-death: queue=q.pagamentos reason=rejected count=1 exchange=ingressos.events routing-keys=['assento.reservado']`; devolvida com `basic_nack(requeue=True)` (q.dlq continuou 1) |
| Etapa 4: DLQ por payload corrompido | **C9** | **PASS** (ver O7) | `publicar_invalido.py` exit=0. q.dlq 1 → **4**; `x-death.queue` = `[q.assentos, q.notificacoes, q.status]`; os 3 logs mostram `ERRO: JSON inválido ... \| NACK -> DLQ`; 5 workers vivos e 1 consumidor por fila |
| Concorrência: corrida pelo assento | **C10** | **PASS** | 2 assentos_service (pids 45536 e 35136); 2 POSTs simultâneos (barreira de threads) → 202/202. `pid=35136 <- reserva.criada reserva=9012ba \| -> assento.indisponivel` e `pid=45536 <- reserva.criada reserva=cc8e7c \| -> assento.reservado`. Finais: `ASSENTO_INDISPONIVEL` + `CONFIRMADA`; assento 3 `VENDIDO` para `cc8e7c`; 1 ingresso; 1 pagamento |
| Etapa 5: competing consumers + rajada | **C11** | **PASS** | 2 réplicas (pids 30148 e 7872, consumidores=2). 10 POSTs simultâneos (mesma lógica do botão "Teste de rajada") → 10× 202 em 0,37 s (máx. 367 ms). **Divisão por pid: `{'30148': 5, '7872': 5}`** (rodada 2: `{'15184': 5, '38760': 5}`; rodada 3: `{'22256': 5, '28316': 5}`). 10/10 `CONFIRMADA` |
| Etapa 3: idempotência | **C12** | **PASS** | Reserva `74e6d4` confirmada (`BIL-0EESIK`), depois o mesmo envelope `assento.reservado` (mesmo `event_id`) publicado 2×. 3 processamentos no pagamento (`pid=30148` ×2 e `pid=7872`), 3 na emissão e 2× `assento já estava VENDIDO (mensagem repetida)`. Resultado: **1 linha em `pagamentos`, 1 em `ingressos` (mesmo código `BIL-0EESIK`)**; q.dlq inalterada |
| Etapa 1/5: frontend | **C13** | **PASS** | `GET /` → 200 (tem `id="rajada"`), `/app.js` e `/style.css` → 200. Polling: `frontend/app.js:105` (`fetch(/api/reservas/${reservaId})`) e `:118` (`setInterval(atualizar, 1000)`); rajada: `rajadaEl.addEventListener` → `fetch("/api/reservas")`. Rodada 3: aviso de 20 s sem novidade, `frontend/app.js:103` (O2) |
| Durabilidade | **DUR** + **RST1–RST3** | **PASS** | CloudAMQP (sem restart): as 6 filas `durable=True`, as 2 exchanges `durable=True`; mensagem lida com `basic_get`: `delivery_mode=2`. **Restart real** no RabbitMQ local via Docker: ver a seção abaixo |
| Regra 9: reconexão do publicador da API | **IDLE** (extra) | **PASS** | Com a API sem publicar por **660 s** (mais que 2× o heartbeat de 300 s): `POST → 202`, reserva `CONFIRMADA`. **Rodada 3 (com o log do O6), prova direta:** `[api pid=43532] publicação falhou (StreamLostError): reconectando` → `[api pid=43532] publicador reconectado ao broker` → 202 em 280 ms. O CloudAMQP derrubou a conexão ociosa e a API se recuperou sozinha. Nenhum worker reconectou durante a ociosidade |

### Durabilidade com restart real (RabbitMQ local no Docker)

`python testes/restart_broker.py`: sobe `rabbitmq:3.13-management` num container descartável e aponta a `AMQP_URL` para ele **só naquele processo** (o `.env` e o CloudAMQP não são tocados). Usa um banco separado (`data/teste_restart.db`) e remove o container no final.

| Teste | Resultado | Evidência |
|---|---|---|
| **RST1** Topologia e mensagens persistentes sobrevivem a `docker restart`; a transitória some | **PASS** | Com os workers desligados: 3 reservas (persistentes) + 1 mensagem de controle com `delivery_mode=1`. Antes: `q.assentos=3, q.notificacoes=4, q.status=4`. Broker reiniciado (16 s). Depois: `q.assentos=3, q.notificacoes=3, q.status=3`: as 3 persistentes ficaram e a transitória sumiu. Exchanges `topic/fanout` durable, 6 filas durable com DLX e 11 bindings iguais aos de antes. `delivery_mode` lido depois do restart: `[2]` |
| **RST3** A API ligada durante o restart reconecta sozinha | **PASS** | Mesmo processo da API, depois do restart: `publicação falhou (StreamLostError): reconectando` → `publicador reconectado ao broker` → `HTTP 202 em 121 ms` |
| **RST2** O fluxo continua depois do restart | **PASS** | Workers ligados depois do restart: as 3 reservas publicadas **antes** do restart e a nova do RST3 chegam a `CONFIRMADA` (`[payment_service pid=37784] <- assento.reservado reserva=29adcb \| -> pagamento.aprovado \| ACK`, idem `0191dc`, `9a366f`, `46728c`). Todas as filas terminam em 0 |

Com isso, a durabilidade está verificada **com restart real**, num RabbitMQ local. No CloudAMQP (LavinMQ), continua verificada só pelas propriedades.

## Parte D: entregáveis

- **README do zero:** extraí o `git archive HEAD` numa pasta nova e segui o README ao pé da letra (`python -m venv .venv` → `.venv\Scripts\activate` → `pip install -r requirements.txt` → `cp .env.example .env`). Sem a URL, `setup_broker.py` para com mensagem clara ("AMQP_URL não definida..."). Com a URL, `setup_broker.py` exit=0, `db.py --reset` OK, `python -m unittest discover -s testes` → **81 testes OK** (hoje são 85, com o `test_common.py`). **Não travou em nenhum passo.** Atenção: no PowerShell, o `activate` pode ser bloqueado pela política de execução (`Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`). Só testei com Python 3.13/3.14 (não havia 3.10 na máquina).
- **Conteúdo do `.zip`:** `git archive -o entrega.zip HEAD` gera um zip só com arquivos versionados: **sem `.env`, `.venv/`, `data/bilheteria.db`, `__pycache__/` e `logs/`**. Zipar a pasta pelo Explorer levaria `.env` (com a senha), `.venv/`, o banco, `logs/` e `__pycache__/`. **Recomendação: gerar o zip com `git archive`.**

## Riscos para a gravação do vídeo

1. **Broker compartilhado:** se outro integrante estiver com um worker ligado, ele "rouba" mensagens e os pids do vídeo ficam errados. Rode `python testes/antes_de_testar.py` antes de gravar (precisa dar LIVRE).
2. **Painel do LavinMQ ≠ RabbitMQ:** os nomes das colunas e telas são diferentes dos slides da aula. Ensaie onde ficam Ready/Unacked, Consumers, Bindings e "Get messages".
3. **`PAGO` quase nunca aparece na tela** (O5): `PAGO → CONFIRMADA` leva milissegundos. Mostre o `status -> PAGO` no terminal do `status_service`.
4. **Fraude deixa a reserva em "Aguardando pagamento"** de propósito. Depois de 20 s, a tela explica o motivo (O2); diga na fala que a mensagem está na DLQ para análise. O mesmo aviso aparece no teste de caos, e lá o status volta a andar quando o serviço religa.
5. **`publicar_invalido.py` coloca 3 mensagens na DLQ, não 1** (O7). Se a q.dlq já tiver a mensagem da fraude, ela vai de 1 para 4. O próprio script avisa isso.
6. **Rajada:** com `PAYMENT_DELAY=2` e 2 réplicas, as 10 mensagens levam ~10 s e a divisão fica clara (5/5 nas duas rodadas). Com `PAYMENT_DELAY=0` tudo acaba em menos de 1 s e não dá para ver a alternância.
7. **Caos com Ctrl+C:** o kill no meio do pagamento foi testado (C7). Num Ctrl+C, a mensagem em andamento também volta para a fila. Para mostrar `Ready=3`, faça as 3 reservas **depois** de derrubar o serviço e espere o painel atualizar (o painel do CloudAMQP atualiza a cada poucos segundos).
8. **Reconexão do worker:** se a conexão de um worker cair, ele espera 5 s antes de reconectar (`common.py:35`). Nos testes, cada worker subiu e apareceu como consumidor em ~1 s.
9. **Primeiro POST mais lento** (~250 ms): a API abre a conexão com o broker na primeira reserva. Ainda fica bem abaixo de 1 s.
10. **Ordem das linhas no terminal:** o log sai depois do ACK, então às vezes o `status_service` imprime `CONFIRMADA` antes de o `notificacao_service` imprimir o e-mail. Não é bug: os serviços são independentes.

## Prints recomendados para o relatório PDF

1. **Exchanges** no painel: `ingressos.events` (topic) e `ingressos.dlx` (fanout), ambas durable.
2. **Queues**: as 6 filas com durable e o argumento `x-dead-letter-exchange=ingressos.dlx` (exceto q.dlq).
3. **Bindings de `ingressos.events`**: as routing keys de cada fila (é a tabela de topologia ao vivo).
4. **Frontend logo depois do envio** + aba Network do navegador mostrando `POST /api/reservas 202` com o tempo (prova da resposta imediata).
5. **Coreografia**: os 5 terminais lado a lado com o **mesmo `reserva=`** em cada linha, de `reserva.criada` até o e-mail com o código `BIL-...`.
6. **Caos, antes**: `q.pagamentos` com Ready=3 e Consumers=0. **Caos, depois**: Ready=0 e o terminal do payment_service processando as 3.
7. **Ack manual**: `q.pagamentos` com Unacked=1 durante o pagamento (antes de derrubar).
8. **DLQ**: q.dlq com as mensagens + "Get messages" mostrando o header `x-death` (`queue=q.pagamentos`, `reason=rejected`) e o terminal do payment_service com `NACK -> DLQ`.
9. **Payload corrompido**: os 3 terminais com `JSON inválido | NACK -> DLQ` e os workers continuando vivos (próxima mensagem processada).
10. **Escala**: q.pagamentos com Consumers=2 + os dois terminais do payment_service com pids diferentes se alternando na rajada.
11. **Disputa**: os dois terminais do assentos_service, um com `assento.reservado` e o outro com `assento.indisponivel`, para a mesma cadeira.
12. **Recusa**: terminal com `pagamento.recusado | ACK`, assento de volta a livre no mapa e q.dlq sem mudança (recusa não é erro).