"""Testes unitários do payment_service (Fase 5) e do fluxo assentos -> pagamento.

Rodar a partir da raiz:  python -m unittest discover -s testes -v
"""

import contextlib
import io
import multiprocessing as mp
import os
import shutil
import sys
import tempfile
import unittest
import uuid
from collections import deque
from pathlib import Path
from unittest import mock

# Banco temporário (nunca o data/bilheteria.db). Os processos filhos herdam o caminho.
os.environ.setdefault("DB_PATH", str(Path(tempfile.mkdtemp(prefix="bilheteria_teste_")) / "teste.db"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "workers"))

import pika  # noqa: E402

import assentos_service  # noqa: E402
import common  # noqa: E402
import db  # noqa: E402
import payment_service as svc  # noqa: E402

CARTAO_OK = "4111111111111111"
CARTAO_SEM_LIMITE = "4111111111110000"
CARTAO_FRAUDE = "9999111111111111"


def criar_reserva(evento_id=1, numero=1, cartao=CARTAO_OK):
    reserva_id = str(uuid.uuid4())
    conn = db.connect()
    try:
        conn.execute(
            """
            INSERT INTO reservas (id, evento_id, numero, nome, email, cartao_final, status, criado_em)
            VALUES (?, ?, ?, 'Bruno Lima', 'bruno@exemplo.com', ?, 'EM_PROCESSAMENTO', 'teste')
            """,
            (reserva_id, evento_id, numero, cartao[-4:]),
        )
        conn.commit()
    finally:
        conn.close()
    return reserva_id


def assento_reservado(reserva_id, cartao=CARTAO_OK, evento_id=1, numero=1, **extra):
    payload = {"evento_id": evento_id, "numero": numero, "nome": "Bruno Lima",
               "email": "bruno@exemplo.com", "cartao": cartao, "valor_centavos": 8000, **extra}
    return {"event_id": str(uuid.uuid4()), "event_type": "assento.reservado",
            "reserva_id": reserva_id, "payload": payload}


def pagamentos_gravados():
    conn = db.connect()
    try:
        return [tuple(r) for r in conn.execute("SELECT reserva_id, resultado FROM pagamentos")]
    finally:
        conn.close()


def assento(evento_id, numero):
    conn = db.connect()
    try:
        row = conn.execute("SELECT status, reserva_id FROM assentos WHERE evento_id = ? AND numero = ?",
                           (evento_id, numero)).fetchone()
    finally:
        conn.close()
    return (row["status"], row["reserva_id"])


# Função de topo para o multiprocessing: uma "réplica" do serviço em outro processo.
def _pagar_em_outro_processo(args):
    reserva_id, cartao, atraso = args
    publicados = []
    with mock.patch.object(svc, "PAYMENT_DELAY", atraso), \
            mock.patch.object(svc, "publish_event", lambda ch, tipo, rid, p: publicados.append(tipo)):
        svc.processar(None, assento_reservado(reserva_id, cartao))
    return os.getpid(), publicados[0]


class BasePagamento(unittest.TestCase):

    def setUp(self):
        with contextlib.redirect_stdout(io.StringIO()):
            db.reset()
        self.publicados = []
        fake = lambda ch, tipo, rid, payload: self.publicados.append((tipo, rid, payload))
        for alvo, valor in ((svc, ("publish_event", fake)),
                            (svc, ("PAYMENT_DELAY", 0))):
            patcher = mock.patch.object(alvo, *valor)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.sleep = mock.patch.object(svc.time, "sleep").start()
        self.addCleanup(mock.patch.stopall)

    def pagar(self, reserva_id, cartao=CARTAO_OK, **extra):
        return svc.processar(None, assento_reservado(reserva_id, cartao, **extra))


# --------------------------------------------------------------------------- #
# Regras do cartão
# --------------------------------------------------------------------------- #
class TestRegrasDoCartao(BasePagamento):

    def test_cartao_normal_e_aprovado(self):
        r = criar_reserva()
        self.assertEqual(self.pagar(r), "pagamento.aprovado")
        self.assertEqual(pagamentos_gravados(), [(r, "APROVADO")])

    def test_cartao_terminado_em_0000_e_recusado_sem_ir_para_dlq(self):
        r = criar_reserva(cartao=CARTAO_SEM_LIMITE)
        self.assertEqual(self.pagar(r, CARTAO_SEM_LIMITE), "pagamento.recusado")
        self.assertEqual(pagamentos_gravados(), [(r, "RECUSADO")])
        self.assertEqual(self.publicados[0][2]["motivo"], "cartão sem limite")

    def test_cartao_comecando_com_9999_vai_para_dlq(self):
        r = criar_reserva(cartao=CARTAO_FRAUDE)
        with self.assertRaises(common.ErroProcessamento):
            self.pagar(r, CARTAO_FRAUDE)
        self.assertEqual(self.publicados, [], "fraude não pode publicar evento")
        self.assertEqual(pagamentos_gravados(), [], "fraude não pode gravar cobrança")

    def test_9999_terminado_em_0000_e_fraude_e_nao_recusa(self):
        cartao = "9999111111110000"
        r = criar_reserva(cartao=cartao)
        with self.assertRaises(common.ErroProcessamento):
            self.pagar(r, cartao)

    def test_9999_no_meio_ou_no_fim_nao_e_fraude(self):
        for cartao in ("4111999911111111", "4111111111119999"):
            with self.subTest(cartao=cartao):
                self.assertEqual(self.pagar(criar_reserva(cartao=cartao), cartao), "pagamento.aprovado")

    def test_0000_no_inicio_no_meio_ou_incompleto_nao_e_recusa(self):
        for cartao in ("0000111111111111", "4111000011111111", "4111111111111000", "4111111111110001"):
            with self.subTest(cartao=cartao):
                self.assertEqual(self.pagar(criar_reserva(cartao=cartao), cartao), "pagamento.aprovado")

    def test_atraso_artificial_e_aplicado(self):
        with mock.patch.object(svc, "PAYMENT_DELAY", 2):
            self.pagar(criar_reserva())
        self.sleep.assert_called_once_with(2)


# --------------------------------------------------------------------------- #
# Idempotência: nunca cobrar duas vezes
# --------------------------------------------------------------------------- #
class TestNaoCobraDuasVezes(BasePagamento):

    def test_mensagem_repetida_nao_cobra_de_novo(self):
        r = criar_reserva()
        self.pagar(r)
        self.pagar(r)
        self.assertEqual(pagamentos_gravados(), [(r, "APROVADO")])

    def test_mensagem_repetida_nao_espera_o_atraso_de_novo(self):
        r = criar_reserva()
        self.pagar(r)
        self.sleep.reset_mock()
        self.pagar(r)
        self.sleep.assert_not_called()

    def test_repetida_republica_o_resultado_gravado(self):
        # Se a cobrança foi recusada, a reentrega publica recusado de novo
        # (o assentos_service precisa receber o evento para liberar o assento).
        r = criar_reserva(cartao=CARTAO_SEM_LIMITE)
        self.pagar(r, CARTAO_SEM_LIMITE)
        self.assertEqual(self.pagar(r, CARTAO_SEM_LIMITE), "pagamento.recusado")

    def test_resultado_gravado_vale_mais_que_o_cartao_da_mensagem(self):
        # Mensagem repetida chegou com outro cartão: quem manda é o que já foi cobrado.
        r = criar_reserva()
        self.pagar(r, CARTAO_OK)
        self.assertEqual(self.pagar(r, CARTAO_SEM_LIMITE), "pagamento.aprovado")
        self.assertEqual(pagamentos_gravados(), [(r, "APROVADO")])

    def test_outra_replica_gravou_durante_o_atraso_e_o_resultado_dela_vale(self):
        # Enquanto esta réplica "cobrava" (sleep), outra gravou RECUSADO para a mesma
        # reserva. Esta não pode sobrescrever: tem que publicar o que ficou gravado.
        r = criar_reserva()

        def outra_replica_grava(_segundos):
            conn = db.connect()
            conn.execute("INSERT INTO pagamentos VALUES (?, 'RECUSADO', 'outra')", (r,))
            conn.commit()
            conn.close()

        self.sleep.side_effect = outra_replica_grava
        self.assertEqual(self.pagar(r, CARTAO_OK), "pagamento.recusado")
        self.assertEqual(pagamentos_gravados(), [(r, "RECUSADO")])

    def test_falha_no_publish_depois_de_gravar_e_recuperada_na_reentrega(self):
        r = criar_reserva()
        with mock.patch.object(svc, "publish_event", side_effect=pika.exceptions.AMQPConnectionError()):
            with self.assertRaises(pika.exceptions.AMQPError):
                self.pagar(r)
        self.assertEqual(pagamentos_gravados(), [(r, "APROVADO")])
        self.assertEqual(self.pagar(r), "pagamento.aprovado")
        self.assertEqual(len(pagamentos_gravados()), 1)


# --------------------------------------------------------------------------- #
# Payload inválido -> DLQ, sem cobrar
# --------------------------------------------------------------------------- #
class TestPayloadInvalido(BasePagamento):

    def assertVaiParaDLQ(self, envelope):
        with self.assertRaises(common.ErroProcessamento):
            svc.processar(None, envelope)
        self.assertEqual(self.publicados, [])
        self.assertEqual(pagamentos_gravados(), [])

    def test_cartao_invalido(self):
        r = criar_reserva()
        for cartao in (None, "", "411111111111111", "41111111111111111", "4111 1111 1111 1111",
                       "4111a11111111111", 4111111111111111, ["4111111111111111"]):
            with self.subTest(cartao=cartao):
                self.assertVaiParaDLQ(assento_reservado(r, cartao))

    def test_sem_cartao(self):
        env = assento_reservado(criar_reserva())
        del env["payload"]["cartao"]
        self.assertVaiParaDLQ(env)

    def test_reserva_id_invalido(self):
        for valor in (None, "", "  ", 7):
            with self.subTest(reserva_id=valor):
                self.assertVaiParaDLQ(assento_reservado(valor))

    def test_reserva_fantasma(self):
        self.assertVaiParaDLQ(assento_reservado(str(uuid.uuid4())))

    def test_evento_errado_na_fila(self):
        env = assento_reservado(criar_reserva())
        env["event_type"] = "reserva.criada"
        self.assertVaiParaDLQ(env)

    def test_atraso_nao_e_gasto_com_mensagem_invalida(self):
        self.assertVaiParaDLQ(assento_reservado(criar_reserva(), "123"))
        self.sleep.assert_not_called()


# --------------------------------------------------------------------------- #
# O que vai na mensagem publicada
# --------------------------------------------------------------------------- #
class TestEventoPublicado(BasePagamento):

    def test_cartao_completo_nao_sai_do_payment_service(self):
        for cartao in (CARTAO_OK, CARTAO_SEM_LIMITE):
            with self.subTest(cartao=cartao):
                self.publicados.clear()
                self.pagar(criar_reserva(cartao=cartao), cartao)
                payload = self.publicados[0][2]
                self.assertNotIn("cartao", payload)
                self.assertNotIn(cartao, [str(v) for v in payload.values()])
                self.assertEqual(payload["cartao_final"], cartao[-4:])

    def test_dados_vem_do_banco_e_nao_da_mensagem(self):
        # Mensagem adulterada dizendo outro assento/e-mail: o evento segue com o que está gravado.
        r = criar_reserva(evento_id=1, numero=3)
        self.pagar(r, numero=20, email="hacker@exemplo.com")
        tipo, rid, payload = self.publicados[0]
        self.assertEqual((tipo, rid), ("pagamento.aprovado", r))
        self.assertEqual((payload["evento_id"], payload["numero"], payload["email"]), (1, 3, "bruno@exemplo.com"))

    def test_valor_segue_para_os_proximos_servicos(self):
        self.pagar(criar_reserva())
        self.assertEqual(self.publicados[0][2]["valor_centavos"], 8000)


class TestLeituraDoAtraso(unittest.TestCase):

    def test_payment_delay_invalido_ou_negativo(self):
        for valor, esperado in (("3", 3.0), ("0.5", 0.5), ("abc", 2.0), ("-5", 0.0), ("", 2.0)):
            with self.subTest(valor=valor), mock.patch.dict(os.environ, {"PAYMENT_DELAY": valor}):
                self.assertEqual(svc._ler_atraso(), esperado)


# --------------------------------------------------------------------------- #
# Ponta a ponta sem broker: assentos_service <-> payment_service
# --------------------------------------------------------------------------- #
class BrokerDeMentira:
    """Entrega cada evento publicado ao serviço certo, como os bindings do RabbitMQ."""

    def __init__(self):
        self.fila = deque()
        self.historico = []

    def publicar(self, channel, tipo, reserva_id, payload):
        env = {"event_id": str(uuid.uuid4()), "event_type": tipo, "reserva_id": reserva_id, "payload": payload}
        self.fila.append(env)
        self.historico.append((tipo, reserva_id))

    def rodar(self):
        rotas = {
            "reserva.criada": assentos_service.processar,
            "assento.reservado": svc.processar,
            "pagamento.aprovado": assentos_service.processar,
            "pagamento.recusado": assentos_service.processar,
        }
        while self.fila:
            env = self.fila.popleft()
            if env["event_type"] in rotas:
                rotas[env["event_type"]](None, env)


class TestFluxoCompleto(BasePagamento):

    def setUp(self):
        super().setUp()
        self.broker = BrokerDeMentira()
        mock.patch.object(svc, "publish_event", self.broker.publicar).start()
        mock.patch.object(assentos_service, "publish_event", self.broker.publicar).start()
        mock.patch.object(assentos_service, "log").start()

    def comprar(self, numero, cartao, nome="Cliente"):
        r = criar_reserva(1, numero, cartao)
        self.broker.publicar(None, "reserva.criada", r, {
            "evento_id": 1, "numero": numero, "nome": nome, "email": "c@exemplo.com", "cartao": cartao})
        return r

    def test_compra_aprovada_termina_com_assento_vendido(self):
        r = self.comprar(5, CARTAO_OK)
        self.broker.rodar()
        self.assertEqual(assento(1, 5), ("VENDIDO", r))

    def test_pagamento_recusado_libera_assento_para_outra_pessoa(self):
        a = self.comprar(5, CARTAO_SEM_LIMITE, "Alice")
        self.broker.rodar()
        self.assertEqual(assento(1, 5), ("LIVRE", None))
        b = self.comprar(5, CARTAO_OK, "Bruno")
        self.broker.rodar()
        self.assertEqual(assento(1, 5), ("VENDIDO", b))
        self.assertIn(("pagamento.recusado", a), self.broker.historico)

    def test_duas_pessoas_no_mesmo_assento_so_uma_e_cobrada(self):
        a = self.comprar(5, CARTAO_OK, "Alice")
        b = self.comprar(5, CARTAO_OK, "Bruno")
        self.broker.rodar()
        self.assertEqual(assento(1, 5), ("VENDIDO", a))
        self.assertEqual(pagamentos_gravados(), [(a, "APROVADO")], "quem perdeu o assento não pode ser cobrado")
        self.assertIn(("assento.indisponivel", b), self.broker.historico)

    def test_primeiro_recusado_segundo_perdeu_antes_e_assento_fica_livre(self):
        # A pega o assento, B recebe indisponível, depois o cartão de A é recusado.
        # O assento volta a ficar LIVRE (B precisa fazer uma nova reserva).
        self.comprar(5, CARTAO_SEM_LIMITE, "Alice")
        b = self.comprar(5, CARTAO_OK, "Bruno")
        self.broker.rodar()
        self.assertEqual(assento(1, 5), ("LIVRE", None))
        self.assertIn(("assento.indisponivel", b), self.broker.historico)

    def test_fraude_deixa_assento_bloqueado_e_nada_e_cobrado(self):
        # Comportamento definido no CLAUDE.md: fraude vai para a DLQ e para ali.
        r = self.comprar(5, CARTAO_FRAUDE)
        with self.assertRaises(common.ErroProcessamento):
            self.broker.rodar()
        self.assertEqual(assento(1, 5), ("BLOQUEADO", r))
        self.assertEqual(pagamentos_gravados(), [])

    def test_rajada_de_dez_com_cartoes_misturados(self):
        cartoes = [CARTAO_OK, CARTAO_SEM_LIMITE] * 5
        reservas = [self.comprar(n, c) for n, c in enumerate(cartoes, start=1)]
        self.broker.rodar()
        for numero, (r, cartao) in enumerate(zip(reservas, cartoes), start=1):
            esperado = ("VENDIDO", r) if cartao == CARTAO_OK else ("LIVRE", None)
            self.assertEqual(assento(1, numero), esperado, f"assento {numero}")


# --------------------------------------------------------------------------- #
# Réplicas em processos separados (como 2 terminais do payment_service)
# --------------------------------------------------------------------------- #
class TestReplicas(BasePagamento):

    def test_mesma_mensagem_em_duas_replicas_ao_mesmo_tempo_cobra_uma_vez(self):
        # Pior caso do at-least-once: a mensagem foi reentregue enquanto a 1ª
        # réplica ainda "cobrava". As duas passam pela checagem antes de gravar.
        r = criar_reserva()
        with mp.Pool(2) as pool:
            resultados = pool.map(_pagar_em_outro_processo, [(r, CARTAO_OK, 0.5)] * 2, chunksize=1)
        self.assertEqual(len({pid for pid, _ in resultados}), 2, "não rodou em 2 processos")
        self.assertEqual([tipo for _, tipo in resultados], ["pagamento.aprovado"] * 2)
        self.assertEqual(pagamentos_gravados(), [(r, "APROVADO")])

    def test_duas_replicas_processam_rajada_de_dez_sem_perder_nem_duplicar(self):
        cartoes = [CARTAO_OK, CARTAO_SEM_LIMITE] * 5
        reservas = [criar_reserva(1, n, c) for n, c in enumerate(cartoes, start=1)]
        with mp.Pool(2) as pool:
            resultados = pool.map(_pagar_em_outro_processo,
                                  [(r, c, 0.05) for r, c in zip(reservas, cartoes)], chunksize=1)
        gravados = dict(pagamentos_gravados())
        self.assertEqual(len(gravados), 10)
        for (pid, tipo), r, cartao in zip(resultados, reservas, cartoes):
            esperado = "RECUSADO" if cartao.endswith("0000") else "APROVADO"
            self.assertEqual(gravados[r], esperado)
            self.assertEqual(tipo, f"pagamento.{esperado.lower()}")


def tearDownModule():
    shutil.rmtree(Path(os.environ["DB_PATH"]).parent, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
