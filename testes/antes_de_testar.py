"""Confere se o broker compartilhado está livre antes de você começar a testar.

Uso (a partir da raiz):  python testes/antes_de_testar.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "workers"))

import pika  # noqa: E402

from common import get_connection  # noqa: E402
from setup_broker import DLQ, FILAS  # noqa: E402  (só lê os nomes; não declara nada)


def main():
    conn = get_connection("antes_de_testar")
    ocupado, sobras = False, False

    print(f"{'fila':<16}{'mensagens':>10}{'consumidores':>14}")
    for fila in [*FILAS, DLQ]:
        # passive=True só consulta: não cria nem altera a fila. Cada consulta usa
        # um canal novo porque, se a fila não existir, o broker fecha o canal.
        canal = conn.channel()
        try:
            info = canal.queue_declare(queue=fila, passive=True).method
        except pika.exceptions.ChannelClosedByBroker:
            print(f"{fila:<16}{'NÃO EXISTE':>24}  -> rode: python workers/setup_broker.py")
            continue
        canal.close()
        print(f"{fila:<16}{info.message_count:>10}{info.consumer_count:>14}")

        # Consumidor conectado = algum serviço ligado agora, provavelmente de outra pessoa.
        ocupado |= info.consumer_count > 0
        # Mensagem parada (fora da DLQ) = sobra de um teste anterior, de você ou de outro.
        sobras |= fila != DLQ and info.message_count > 0
    conn.close()

    print()
    if ocupado:
        print("PARE: tem serviço conectado no broker. Alguém está testando agora.")
        print("      Avise no grupo e espere a pessoa terminar (ou desligue os seus serviços).")
        sys.exit(1)
    if sobras:
        print("ATENÇÃO: sobraram mensagens de um teste anterior.")
        print("         Se não forem suas, elas vão cair na DLQ como 'reserva inexistente'.")
        print("         Combine no grupo antes de limpar (Purge) no painel do CloudAMQP.")
        sys.exit(2)
    print("LIVRE: ninguém conectado e filas vazias. Avise no grupo que vai testar e bom teste!")


if __name__ == "__main__":
    main()
