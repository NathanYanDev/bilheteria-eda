"""Testes unitários da Fase 6 (emissão, notificação, status). Sem broker.

Rodar a partir da raiz:  python -m unittest discover -s testes -v
"""

import contextlib
import io
import os
import sys
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest import mock

os.environ.setdefault("DB_PATH", str(Path(tempfile.mkdtemp(prefix="bilheteria_teste_")) / "teste.db"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "workers"))

import common  # noqa: E402
import db  # noqa: E402
import emissao_service  # noqa: E402
import notificacao_service  # noqa: E402
import status_service  # noqa: E402


def envelope(tipo, reserva_id, **payload):
    return {"event_id": str(uuid.uuid4()), "event_type": tipo, "reserva_id": reserva_id, "payload": payload}


class Base(unittest.TestCase):
    def setUp(self):
        with contextlib.redirect_stdout(io.StringIO()):
            db.reset()
        self.publicados = []
        fake = lambda ch, tipo, rid, p: self.publicados.append((tipo, rid, p))
        mock.patch.object(emissao_service, "publish_event", fake).start()
        mock.patch.object(status_service, "log").start()
        mock.patch.object(notificacao_service, "log").start()
        self.addCleanup(mock.patch.stopall)
        self.reserva = str(uuid.uuid4())
        conn = db.connect()
        conn.execute(
            "INSERT INTO reservas VALUES (?, 1, 1, 'Ana', 'ana@x.com', '1111', 'EM_PROCESSAMENTO', 't')",
            (self.reserva,),
        )
        conn.commit()
        conn.close()

    def status(self):
        conn = db.connect()
        try:
            return conn.execute("SELECT status FROM reservas WHERE id = ?", (self.reserva,)).fetchone()[0]
        finally:
            conn.close()


class TestEmissao(Base):
    def emitir(self):
        return emissao_service.processar(None, envelope("pagamento.aprovado", self.reserva))

    def test_emite_codigo_e_publica(self):
        self.assertEqual(self.emitir(), "ingresso.emitido")
        codigo = self.publicados[0][2]["codigo"]
        self.assertRegex(codigo, r"^BIL-[A-Z0-9]{6}$")

    def test_mensagem_repetida_republica_mesmo_codigo(self):
        self.emitir()
        self.emitir()
        self.assertEqual(self.publicados[0][2]["codigo"], self.publicados[1][2]["codigo"])
        conn = db.connect()
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM ingressos").fetchone()[0], 1)
        conn.close()

    def test_reserva_inexistente_vai_para_dlq(self):
        with self.assertRaises(common.ErroProcessamento):
            emissao_service.processar(None, envelope("pagamento.aprovado", "nao-existe"))


class TestNotificacao(Base):
    def test_evento_com_email_ok(self):
        self.assertIsNone(notificacao_service.processar(
            None, envelope("ingresso.emitido", self.reserva, nome="Ana", email="ana@x.com", codigo="BIL-ABC123")))

    def test_sem_email_vai_para_dlq(self):
        with self.assertRaises(common.ErroProcessamento):
            notificacao_service.processar(None, envelope("reserva.criada", self.reserva, nome="Ana"))


class TestStatus(Base):
    def aplicar(self, tipo):
        status_service.processar(None, envelope(tipo, self.reserva))

    def test_fluxo_feliz_ate_confirmada(self):
        for tipo in ("reserva.criada", "assento.reservado", "pagamento.aprovado", "ingresso.emitido"):
            self.aplicar(tipo)
        self.assertEqual(self.status(), "CONFIRMADA")

    def test_evento_fora_de_ordem_nao_regride(self):
        self.aplicar("pagamento.aprovado")
        self.aplicar("assento.reservado")
        self.aplicar("reserva.criada")
        self.assertEqual(self.status(), "PAGO")

    def test_finais_alternativos_nao_sao_sobrescritos(self):
        self.aplicar("pagamento.recusado")
        self.aplicar("ingresso.emitido")
        self.assertEqual(self.status(), "PAGAMENTO_RECUSADO")

    def test_assento_indisponivel(self):
        self.aplicar("assento.indisponivel")
        self.assertEqual(self.status(), "ASSENTO_INDISPONIVEL")

    def test_evento_desconhecido_e_ignorado(self):
        self.aplicar("outra.coisa")
        self.assertEqual(self.status(), "EM_PROCESSAMENTO")


if __name__ == "__main__":
    unittest.main()
