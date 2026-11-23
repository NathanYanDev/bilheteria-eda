"""Testes do run_consumer (common.py): o que acontece com a mensagem em cada tipo de erro.

Não precisa do broker: uma conexão falsa entrega UMA mensagem ao callback e
registra se houve ack, nack ou nenhum dos dois.

Uso:  python -m unittest discover -s testes -v
"""

import contextlib
import io
import json
import sqlite3
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "workers"))

import common  # noqa: E402


class CanalDeMentira:
    def __init__(self, corpo):
        self.corpo = corpo
        self.acks, self.nacks = [], []
        self.callback = None
        self.is_open = True

    def confirm_delivery(self):
        pass

    def basic_qos(self, prefetch_count):
        self.prefetch = prefetch_count

    def basic_consume(self, queue, on_message_callback, auto_ack):
        self.auto_ack = auto_ack
        self.callback = on_message_callback

    def start_consuming(self):
        metodo = SimpleNamespace(delivery_tag=1, routing_key="reserva.criada")
        self.callback(self, metodo, None, self.corpo)
        raise KeyboardInterrupt  # depois de 1 mensagem, encerra o run_consumer

    def basic_ack(self, delivery_tag):
        self.acks.append(delivery_tag)

    def basic_nack(self, delivery_tag, requeue):
        self.nacks.append((delivery_tag, requeue))


class ConexaoDeMentira:
    def __init__(self, canal):
        self.canal = canal
        self.is_open = True
        self.fechada = False

    def channel(self):
        return self.canal

    def close(self):
        self.fechada = True
        self.is_open = False


def rodar(callback):
    """Roda o run_consumer com 1 mensagem válida. Devolve (canal, conexões abertas)."""
    corpo = json.dumps(common.build_envelope("reserva.criada", "r1", {"x": 1})).encode()
    canal = CanalDeMentira(corpo)
    conexoes = []

    def conectar(nome):
        if conexoes:  # 2ª conexão = reconectou; encerra o teste aqui
            raise KeyboardInterrupt
        conexoes.append(ConexaoDeMentira(canal))
        return conexoes[-1]

    with mock.patch.object(common, "get_connection", side_effect=conectar), \
            mock.patch.object(common.time, "sleep"), \
            contextlib.redirect_stdout(io.StringIO()):
        try:
            common.run_consumer("teste", "q.teste", callback)
        except KeyboardInterrupt:
            pass
    return canal, conexoes


class TestRunConsumer(unittest.TestCase):

    def test_sucesso_da_ack(self):
        canal, _ = rodar(lambda ch, env: "assento.reservado")
        self.assertEqual(canal.acks, [1])
        self.assertEqual(canal.nacks, [])
        self.assertFalse(canal.auto_ack)
        self.assertEqual(canal.prefetch, 1)

    def test_erro_permanente_vai_para_dlq(self):
        def falha(ch, env):
            raise common.ErroProcessamento("cartão suspeito de fraude")
        canal, _ = rodar(falha)
        self.assertEqual(canal.acks, [])
        self.assertEqual(canal.nacks, [(1, False)])

    def test_bug_inesperado_vai_para_dlq(self):
        def falha(ch, env):
            raise KeyError("campo")
        canal, _ = rodar(falha)
        self.assertEqual(canal.nacks, [(1, False)])

    def test_banco_travado_nao_vai_para_dlq_e_reconecta(self):
        # "database is locked" é temporário: a reserva é válida e não pode ir para a DLQ.
        def falha(ch, env):
            raise sqlite3.OperationalError("database is locked")
        canal, conexoes = rodar(falha)
        self.assertEqual(canal.acks, [], "não pode dar ack: a reserva não foi processada")
        self.assertEqual(canal.nacks, [], "nack(requeue=False) mandaria uma reserva válida para a DLQ")
        self.assertTrue(conexoes[0].fechada, "fechar a conexão sem ack faz o broker reentregar")


if __name__ == "__main__":
    unittest.main()
