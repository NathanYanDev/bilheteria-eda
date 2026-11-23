"""Teste de durabilidade com RESTART REAL do broker (RabbitMQ local no Docker).

O plano gratuito do CloudAMQP não deixa reiniciar o broker. Aqui sobe um
RabbitMQ descartável no Docker, aponta a AMQP_URL para ele (só neste processo:
o .env e o CloudAMQP não são tocados) e prova que:
  - exchanges, filas e bindings durable continuam existindo depois do restart;
  - mensagens persistentes (delivery_mode=2) sobrevivem; uma transitória de
    controle (delivery_mode=1) some;
  - a API, que continua ligada durante o restart, reconecta sozinha na próxima reserva;
  - ao ligar os workers depois do restart, as reservas chegam a CONFIRMADA.

Uso (a partir da raiz, com o Docker rodando):
    python testes/restart_broker.py                     # remove o container no final
    python testes/restart_broker.py --manter-container  # deixa o painel em http://localhost:15672

Usa um banco separado (data/teste_restart.db) e logs em logs/restart/.
"""

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

CONTAINER = "bilheteria-rmq"
IMAGEM = "rabbitmq:3.13-management"
USUARIO = SENHA = "bilheteria"  # usuário próprio: o guest só aceita conexões de localhost do container

# Precisa vir ANTES de importar common/db (eles leem o ambiente no import).
# load_dotenv não sobrescreve variáveis que já existem, então o .env é ignorado para estas duas.
os.environ["AMQP_URL"] = f"amqp://{USUARIO}:{SENHA}@localhost:5672/%2F"
os.environ["DB_PATH"] = "data/teste_restart.db"

sys.path.insert(0, str(Path(__file__).resolve().parent))
import e2e  # noqa: E402
from e2e import (FINAIS, FILAS_TODAS, RESULTADOS, Broker, Gerenciamento, Servicos, db, esperar,  # noqa: E402
                 esperar_status, fmt_filas, info, pika, registrar, reservar, subir_api, subir_worker)

e2e.LOGS = e2e.RAIZ / "logs" / "restart"


def docker(*args, check=True):
    r = subprocess.run(["docker", *args], capture_output=True, text=True, encoding="utf-8", errors="replace")
    if check and r.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args)}: {r.stderr.strip()}")
    return r.stdout.strip()


def broker_pronto(mgmt):
    """AMQP aceitando conexão E painel respondendo (o painel sobe por último)."""
    try:
        Broker()._conexao().close()
        mgmt.get("overview")
        return True
    except Exception:  # noqa: BLE001  (ainda subindo)
        return False


def topologia(mgmt):
    filas = {q: mgmt.get("queues", "{vh}", q) for q in FILAS_TODAS}
    exchanges = {e: mgmt.get("exchanges", "{vh}", e) for e in (e2e.common.EXCHANGE, e2e.common.DLX)}
    ligacoes = sorted(f"{b['source']}->{b['destination']}:{b['routing_key']}"
                      for b in mgmt.get("bindings", "{vh}") if b.get("source"))
    return {
        "exchanges": {n: (d["type"], d["durable"]) for n, d in exchanges.items()},
        "filas": {n: (d["durable"], (d.get("arguments") or {}).get("x-dead-letter-exchange")) for n, d in filas.items()},
        "bindings": ligacoes,
    }


def main():
    parser = argparse.ArgumentParser(description="Durabilidade com restart real do RabbitMQ (Docker).")
    parser.add_argument("--manter-container", action="store_true", help="não remove o container no final")
    args = parser.parse_args()

    if e2e.porta_ocupada(5000):
        sys.exit("A porta 5000 já está em uso: desligue a API que estiver rodando.")
    e2e.LOGS.mkdir(parents=True, exist_ok=True)
    for antigo in e2e.LOGS.glob("*.log"):
        antigo.unlink()

    mgmt = Gerenciamento()
    mgmt.base = "http://localhost:15672/api"
    broker, svc = Broker(), Servicos()
    try:
        # Só remove o container com o NOSSO nome (sobra de uma execução anterior).
        docker("rm", "-f", CONTAINER, check=False)
        # --hostname fixo: o RabbitMQ guarda os dados numa pasta com o nome do nó;
        # o mesmo container reiniciado mantém o hostname e acha os dados.
        docker("run", "-d", "--name", CONTAINER, "--hostname", CONTAINER, "-p", "5672:5672", "-p", "15672:15672",
               "-e", f"RABBITMQ_DEFAULT_USER={USUARIO}", "-e", f"RABBITMQ_DEFAULT_PASS={SENHA}", IMAGEM)
        info(f"container {CONTAINER} ({IMAGEM}) subindo...")
        if not esperar(lambda: broker_pronto(mgmt), 90, intervalo=2):
            raise RuntimeError("RabbitMQ local não ficou pronto em 90 s")
        info("RabbitMQ local pronto")

        r = subprocess.run([sys.executable, "-u", "workers/setup_broker.py"], cwd=e2e.RAIZ, capture_output=True,
                           text=True, encoding="utf-8", errors="replace", env={**os.environ, "PYTHONIOENCODING": "utf-8"})
        if r.returncode != 0:
            raise RuntimeError(f"setup_broker.py falhou: {r.stdout}{r.stderr}")
        db.reset()

        # 1) Workers desligados: 3 reservas ficam paradas nas filas.
        subir_api(svc)
        rids = []
        for numero in (1, 2, 3):
            status, resp, _ = reservar(numero)
            rids.append(resp["reserva_id"])
        esperar(lambda: broker.fila("q.status")[0] == 3, 15)

        # 2) Controle: uma mensagem TRANSITÓRIA (delivery_mode=1) em fila durable.
        #    Vai para q.notificacoes e q.status (ambas assinam ingresso.emitido).
        envelope = e2e.common.build_envelope("ingresso.emitido", rids[0], {"controle": "transitoria"})
        broker.publicar_bruto("ingresso.emitido", json.dumps(envelope).encode(), pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Transient, content_type="application/json",
            message_id=envelope["event_id"]))
        esperar(lambda: broker.fila("q.status")[0] == 4, 15)
        antes = broker.contagens()
        topo_antes = topologia(mgmt)
        info(f"antes do restart: {fmt_filas(antes)}")

        # 3) Restart de verdade do broker (todas as conexões caem). A API continua
        #    ligada de propósito: a conexão dela morre e a próxima reserva tem que reconectar.
        broker.fechar()
        t0 = time.monotonic()
        docker("restart", CONTAINER)
        if not esperar(lambda: broker_pronto(mgmt), 90, intervalo=2):
            raise RuntimeError("RabbitMQ local não voltou em 90 s depois do restart")
        info(f"broker reiniciado e pronto em {time.monotonic() - t0:.0f} s")
        depois = broker.contagens()
        topo_depois = topologia(mgmt)
        persistentes = broker.espiar("q.assentos", lambda corpo: True)
        modos = sorted({m["props"].delivery_mode for m in persistentes})
        controle = broker.espiar("q.status", lambda corpo: b"transitoria" in corpo)

        esperado = {**antes, "q.notificacoes": antes["q.notificacoes"] - 1, "q.status": antes["q.status"] - 1}
        registrar("RST1", "Restart real: topologia e mensagens persistentes sobrevivem, transitória some",
                  depois == esperado and topo_depois == topo_antes and modos == [2] and not controle, [
                      f"antes:  {fmt_filas(antes)}  (3 reservas persistentes + 1 transitória de controle)",
                      f"depois: {fmt_filas(depois)}",
                      f"esperado depois: {fmt_filas(esperado)}",
                      f"topologia igual antes/depois: {topo_depois == topo_antes}",
                      f"exchanges depois: {topo_depois['exchanges']}",
                      f"filas depois (durable, DLX): {topo_depois['filas']}",
                      f"bindings depois: {len(topo_depois['bindings'])} ({len(topo_antes['bindings'])} antes)",
                      f"delivery_mode das mensagens em q.assentos depois do restart: {modos}",
                      f"mensagem transitória de controle ainda na q.status: {bool(controle)}",
                  ])

        # 4) A mesma API (que perdeu a conexão no restart) aceita uma reserva nova.
        status_novo, resp_novo, seg_novo = reservar(4)
        rid_novo = resp_novo.get("reserva_id") if isinstance(resp_novo, dict) else None
        reconexao = e2e.linhas_log("api.log", "] publica")  # "publicador ..." e "publicação falhou ..."
        registrar("RST3", "API ligada durante o restart reconecta sozinha na próxima reserva",
                  status_novo == 202 and any("reconectado" in l for l in reconexao), [
                      f"POST depois do restart (mesmo processo da API) -> HTTP {status_novo} em {seg_novo * 1000:.0f} ms",
                      *reconexao,
                  ])

        # 5) Liga os workers: as reservas publicadas ANTES do restart (e a nova) terminam o fluxo.
        for nome in e2e.WORKERS:
            subir_worker(svc, broker, nome)
        if rid_novo:
            rids.append(rid_novo)
        finais = [esperar_status(r, FINAIS, 60)[0] for r in rids]
        registrar("RST2", "Reservas publicadas antes do restart (e a nova) chegam a CONFIRMADA",
                  finais == ["CONFIRMADA"] * 4, [
                      f"reservas: {[e2e.curto(r) for r in rids]} -> {finais}",
                      f"filas no fim: {fmt_filas(broker.contagens())}",
                      *[l for r in rids for l in e2e.linhas_log("payment_service*.log", f"reserva={e2e.curto(r)}")],
                  ])
    except Exception as exc:  # noqa: BLE001
        registrar("RST", "erro ao executar", False, [f"{type(exc).__name__}: {exc}"])
    finally:
        svc.derrubar_todos()
        broker.fechar()
        if args.manter_container:
            print(f"\ncontainer {CONTAINER} mantido: painel em http://localhost:15672 ({USUARIO}/{SENHA})")
        else:
            docker("rm", "-f", CONTAINER, check=False)
            print(f"\ncontainer {CONTAINER} removido")
        (e2e.LOGS / "restart_resultado.json").write_text(
            json.dumps(RESULTADOS, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print("\n================ RESUMO ================")
        for r in RESULTADOS:
            print(f"{'PASS' if r['ok'] else 'FAIL'}  {r['id']:<5} {r['titulo']}")


if __name__ == "__main__":
    main()
