"""Testes unitários do assentos_service (Fase 4). Não precisam do RabbitMQ.

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
from pathlib import Path
from unittest import mock

# Banco temporário: os testes nunca tocam o data/bilheteria.db de verdade.
# setdefault: os processos filhos (teste de concorrência) herdam o mesmo caminho.
os.environ.setdefault("DB_PATH", str(Path(tempfile.mkdtemp(prefix="bilheteria_teste_")) / "teste.db"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "workers"))

import pika  # noqa: E402

import assentos_service as svc  # noqa: E402
import common  # noqa: E402
import db  # noqa: E402

CLIENTE = {"nome": "Ana Souza", "email": "ana@exemplo.com", "cartao": "4111111111111111"}


def envelope(event_type, reserva_id, **payload):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "reserva_id": reserva_id,
        "payload": payload,
    }


def criar_reserva(evento_id, numero):
    """Grava a reserva como a API faz antes de publicar reserva.criada."""
    reserva_id = str(uuid.uuid4())
    conn = db.connect()
    try:
        conn.execute(
            """
            INSERT INTO reservas (id, evento_id, numero, nome, email, cartao_final, status, criado_em)
            VALUES (?, ?, ?, 'Ana Souza', 'ana@exemplo.com', '1111', 'EM_PROCESSAMENTO', 'teste')
            """,
            (reserva_id, evento_id, numero),
        )
        conn.commit()
    finally:
        conn.close()
    return reserva_id


def registrar_pagamento(reserva_id, resultado):
    """Simula o payment_service gravando o resultado (Fase 5)."""
    conn = db.connect()
    try:
        conn.execute(
            "INSERT INTO pagamentos (reserva_id, resultado, processado_em) VALUES (?, ?, 'teste')",
            (reserva_id, resultado),
        )
        conn.commit()
    finally:
        conn.close()


def assento(evento_id, numero):
    conn = db.connect()
    try:
        row = conn.execute(
            "SELECT status, reserva_id FROM assentos WHERE evento_id = ? AND numero = ?",
            (evento_id, numero),
        ).fetchone()
    finally:
        conn.close()
    return (row["status"], row["reserva_id"])


def foto_assentos():
    conn = db.connect()
    try:
        return [tuple(r) for r in conn.execute("SELECT * FROM assentos ORDER BY evento_id, numero")]
    finally:
        conn.close()


# Função de topo (não método) para o multiprocessing conseguir chamar em outro processo.
def _disputar_em_outro_processo(args):
    reserva_id, evento_id, numero = args
    publicados = []
    with mock.patch.object(svc, "publish_event", lambda ch, tipo, rid, p: publicados.append(tipo)), \
            mock.patch.object(svc, "log"):
        svc.reserva_criada(None, envelope("reserva.criada", reserva_id,
                                          evento_id=evento_id, numero=numero, **CLIENTE))
    return publicados[0]


class BaseAssentos(unittest.TestCase):
    """Banco zerado + publish_event falso (guarda o que seria enviado ao broker)."""

    def setUp(self):
        with contextlib.redirect_stdout(io.StringIO()):
            db.reset()
        self.publicados = []
        fake = lambda ch, tipo, rid, payload: self.publicados.append((tipo, rid, payload))
        for alvo, valor in (("publish_event", fake), ("log", mock.Mock())):
            patcher = mock.patch.object(svc, alvo, valor)
            patcher.start()
            self.addCleanup(patcher.stop)

    def reservar(self, reserva_id, evento_id, numero, **extra):
        dados = {**CLIENTE, **extra}
        return svc.processar(None, envelope("reserva.criada", reserva_id,
                                            evento_id=evento_id, numero=numero, **dados))

    def aprovar(self, reserva_id):
        return svc.processar(None, envelope("pagamento.aprovado", reserva_id))

    def recusar(self, reserva_id):
        return svc.processar(None, envelope("pagamento.recusado", reserva_id))


# --------------------------------------------------------------------------- #
# Duas pessoas no mesmo assento
# --------------------------------------------------------------------------- #
class TestDisputaMesmoAssento(BaseAssentos):

    def test_duas_pessoas_mesmo_assento_so_a_primeira_ganha(self):
        a, b = criar_reserva(1, 5), criar_reserva(1, 5)
        self.assertEqual(self.reservar(a, 1, 5), "assento.reservado")
        self.assertEqual(self.reservar(b, 1, 5), "assento.indisponivel")
        self.assertEqual(assento(1, 5), ("BLOQUEADO", a))

    def test_perdedor_nao_rouba_o_assento_do_vencedor(self):
        a, b = criar_reserva(1, 5), criar_reserva(1, 5)
        self.reservar(a, 1, 5)
        self.reservar(b, 1, 5)
        self.reservar(b, 1, 5)  # insistindo (reentrega)
        self.assertEqual(assento(1, 5), ("BLOQUEADO", a))

    def test_cinco_pessoas_mesmo_assento_so_uma_reserva(self):
        reservas = [criar_reserva(1, 7) for _ in range(5)]
        resultados = [self.reservar(r, 1, 7) for r in reservas]
        self.assertEqual(resultados.count("assento.reservado"), 1)
        self.assertEqual(resultados.count("assento.indisponivel"), 4)

    def test_tela_desatualizada_assento_ja_vendido(self):
        # Pessoa B ainda vê o assento como livre, mas ele já foi vendido para A.
        a, b = criar_reserva(1, 3), criar_reserva(1, 3)
        self.reservar(a, 1, 3)
        self.aprovar(a)
        self.assertEqual(self.reservar(b, 1, 3), "assento.indisponivel")
        self.assertEqual(assento(1, 3), ("VENDIDO", a))

    def test_indisponivel_leva_dados_para_notificar_o_cliente(self):
        a, b = criar_reserva(1, 5), criar_reserva(1, 5)
        self.reservar(a, 1, 5)
        self.reservar(b, 1, 5)
        tipo, rid, payload = self.publicados[-1]
        self.assertEqual((tipo, rid), ("assento.indisponivel", b))
        self.assertEqual(payload["email"], CLIENTE["email"])
        self.assertNotIn("cartao", payload)  # quem perdeu não segue para o pagamento

    def test_mesmo_numero_em_eventos_diferentes_nao_conflita(self):
        a, b = criar_reserva(1, 5), criar_reserva(2, 5)
        self.assertEqual(self.reservar(a, 1, 5), "assento.reservado")
        self.assertEqual(self.reservar(b, 2, 5), "assento.reservado")

    def test_assentos_diferentes_todos_conseguem(self):
        for numero in range(1, 11):  # a rajada de 10 do frontend
            self.assertEqual(self.reservar(criar_reserva(1, numero), 1, numero), "assento.reservado")


# --------------------------------------------------------------------------- #
# Pagamento recusado libera o assento (compensação)
# --------------------------------------------------------------------------- #
class TestCompensacao(BaseAssentos):

    def test_recusado_libera_o_assento(self):
        a = criar_reserva(1, 4)
        self.reservar(a, 1, 4)
        self.recusar(a)
        self.assertEqual(assento(1, 4), ("LIVRE", None))

    def test_outra_pessoa_consegue_pegar_depois_da_recusa(self):
        a, b = criar_reserva(1, 4), criar_reserva(1, 4)
        self.reservar(a, 1, 4)
        self.recusar(a)
        self.assertEqual(self.reservar(b, 1, 4), "assento.reservado")
        self.assertEqual(assento(1, 4), ("BLOQUEADO", b))

    def test_quem_perdeu_tenta_de_novo_depois_da_recusa(self):
        a, b = criar_reserva(1, 4), criar_reserva(1, 4)
        self.reservar(a, 1, 4)
        self.assertEqual(self.reservar(b, 1, 4), "assento.indisponivel")
        self.recusar(a)
        nova_b = criar_reserva(1, 4)  # B faz uma nova reserva pelo site
        self.assertEqual(self.reservar(nova_b, 1, 4), "assento.reservado")

    def test_recusa_de_uma_reserva_nao_libera_assento_de_outra(self):
        a, b = criar_reserva(1, 4), criar_reserva(1, 4)
        self.reservar(a, 1, 4)
        self.reservar(b, 1, 4)  # perdeu
        self.recusar(b)  # B nunca teve o assento
        self.assertEqual(assento(1, 4), ("BLOQUEADO", a))

    def test_recusa_nao_desfaz_assento_vendido(self):
        a = criar_reserva(1, 4)
        self.reservar(a, 1, 4)
        self.aprovar(a)
        self.recusar(a)  # evento contraditório não pode liberar uma venda
        self.assertEqual(assento(1, 4), ("VENDIDO", a))

    def test_recusa_repetida_nao_quebra(self):
        a = criar_reserva(1, 4)
        self.reservar(a, 1, 4)
        self.recusar(a)
        self.recusar(a)
        self.assertEqual(assento(1, 4), ("LIVRE", None))

    def test_recusa_repetida_depois_de_outro_pegar_nao_libera_o_novo_dono(self):
        a, b = criar_reserva(1, 4), criar_reserva(1, 4)
        self.reservar(a, 1, 4)
        self.recusar(a)
        self.reservar(b, 1, 4)
        self.recusar(a)  # mensagem de A chegou de novo, atrasada
        self.assertEqual(assento(1, 4), ("BLOQUEADO", b))

    def test_reserva_criada_atrasada_nao_rebloqueia_depois_da_recusa(self):
        a = criar_reserva(1, 4)
        self.reservar(a, 1, 4)
        registrar_pagamento(a, "RECUSADO")
        self.recusar(a)
        self.assertIsNone(self.reservar(a, 1, 4))  # reentrega tardia: ignora
        self.assertEqual(assento(1, 4), ("LIVRE", None))


# --------------------------------------------------------------------------- #
# Pagamento aprovado vende o assento
# --------------------------------------------------------------------------- #
class TestVenda(BaseAssentos):

    def test_aprovado_vende(self):
        a = criar_reserva(1, 2)
        self.reservar(a, 1, 2)
        self.aprovar(a)
        self.assertEqual(assento(1, 2), ("VENDIDO", a))

    def test_aprovado_repetido_nao_quebra(self):
        a = criar_reserva(1, 2)
        self.reservar(a, 1, 2)
        self.aprovar(a)
        self.aprovar(a)
        self.assertEqual(assento(1, 2), ("VENDIDO", a))

    def test_aprovado_de_quem_perdeu_vai_para_dlq_e_nao_vende(self):
        a, b = criar_reserva(1, 2), criar_reserva(1, 2)
        self.reservar(a, 1, 2)
        self.reservar(b, 1, 2)
        with self.assertRaises(common.ErroProcessamento):
            self.aprovar(b)
        self.assertEqual(assento(1, 2), ("BLOQUEADO", a))

    def test_aprovado_depois_de_assento_liberado_vai_para_dlq(self):
        a = criar_reserva(1, 2)
        self.reservar(a, 1, 2)
        self.recusar(a)
        with self.assertRaises(common.ErroProcessamento):
            self.aprovar(a)
        self.assertEqual(assento(1, 2), ("LIVRE", None))

    def test_assento_vendido_nao_volta_a_ser_reservado(self):
        a = criar_reserva(1, 2)
        self.reservar(a, 1, 2)
        self.aprovar(a)
        for _ in range(3):
            self.assertEqual(self.reservar(criar_reserva(1, 2), 1, 2), "assento.indisponivel")


# --------------------------------------------------------------------------- #
# Duas pessoas pagando ao mesmo tempo (assentos diferentes)
# --------------------------------------------------------------------------- #
class TestPagamentosSimultaneos(BaseAssentos):

    def test_dois_aprovados_ao_mesmo_tempo_vendem_cada_um_o_seu(self):
        a, b = criar_reserva(1, 10), criar_reserva(1, 11)
        self.reservar(a, 1, 10)
        self.reservar(b, 1, 11)
        self.aprovar(b)
        self.aprovar(a)
        self.assertEqual(assento(1, 10), ("VENDIDO", a))
        self.assertEqual(assento(1, 11), ("VENDIDO", b))

    def test_um_aprovado_e_um_recusado_ao_mesmo_tempo(self):
        a, b = criar_reserva(1, 10), criar_reserva(1, 11)
        self.reservar(a, 1, 10)
        self.reservar(b, 1, 11)
        self.recusar(b)
        self.aprovar(a)
        self.assertEqual(assento(1, 10), ("VENDIDO", a))
        self.assertEqual(assento(1, 11), ("LIVRE", None))


# --------------------------------------------------------------------------- #
# Idempotência (a mesma mensagem pode chegar duas vezes)
# --------------------------------------------------------------------------- #
class TestIdempotencia(BaseAssentos):

    def test_reserva_criada_repetida_republica_reservado(self):
        a = criar_reserva(1, 6)
        self.reservar(a, 1, 6)
        self.assertEqual(self.reservar(a, 1, 6), "assento.reservado")
        self.assertEqual(assento(1, 6), ("BLOQUEADO", a))

    def test_falha_no_publish_depois_do_commit_e_recuperada_na_reentrega(self):
        # Worker bloqueou no banco, mas o broker caiu antes do publish: sem ack,
        # a mensagem volta e o próximo processamento tem que publicar normalmente.
        a = criar_reserva(1, 6)
        with mock.patch.object(svc, "publish_event", side_effect=pika.exceptions.AMQPConnectionError()):
            with self.assertRaises(pika.exceptions.AMQPError):
                self.reservar(a, 1, 6)
        self.assertEqual(assento(1, 6), ("BLOQUEADO", a))
        self.assertEqual(self.reservar(a, 1, 6), "assento.reservado")

    def test_reserva_criada_repetida_depois_da_venda_nao_publica(self):
        a = criar_reserva(1, 6)
        self.reservar(a, 1, 6)
        self.aprovar(a)
        total = len(self.publicados)
        self.assertIsNone(self.reservar(a, 1, 6))
        self.assertEqual(len(self.publicados), total)
        self.assertEqual(assento(1, 6), ("VENDIDO", a))


# --------------------------------------------------------------------------- #
# Payload inválido -> DLQ, sem mexer em nenhum assento
# --------------------------------------------------------------------------- #
class TestPayloadInvalido(BaseAssentos):

    def assertVaiParaDLQ(self, env):
        antes = foto_assentos()
        with self.assertRaises(common.ErroProcessamento):
            svc.processar(None, env)
        self.assertEqual(foto_assentos(), antes, "assento mudou mesmo com payload inválido")
        self.assertEqual(self.publicados, [], "publicou evento mesmo com payload inválido")

    def test_campos_do_assento_invalidos(self):
        r = criar_reserva(1, 1)
        casos = {
            "sem evento_id": {"numero": 1},
            "sem numero": {"evento_id": 1},
            "numero quebrado": {"evento_id": 1, "numero": 1.9},
            "numero booleano": {"evento_id": 1, "numero": True},
            "numero texto": {"evento_id": 1, "numero": "um"},
            "numero negativo": {"evento_id": 1, "numero": -1},
            "numero nulo": {"evento_id": 1, "numero": None},
            "evento lista": {"evento_id": [1], "numero": 1},
        }
        for nome, campos in casos.items():
            with self.subTest(nome):
                self.assertVaiParaDLQ(envelope("reserva.criada", r, **campos, **CLIENTE))

    def test_dados_do_cliente_faltando_ou_vazios(self):
        r = criar_reserva(1, 1)
        for campo in ("nome", "email", "cartao"):
            for valor in (None, "", "   ", 123):
                with self.subTest(campo=campo, valor=valor):
                    dados = {**CLIENTE, campo: valor}
                    self.assertVaiParaDLQ(envelope("reserva.criada", r, evento_id=1, numero=1, **dados))

    def test_reserva_id_invalido(self):
        for valor in (None, "", "   ", 42):
            with self.subTest(reserva_id=valor):
                self.assertVaiParaDLQ(envelope("reserva.criada", valor, evento_id=1, numero=1, **CLIENTE))
                self.assertVaiParaDLQ(envelope("pagamento.aprovado", valor))
                self.assertVaiParaDLQ(envelope("pagamento.recusado", valor))

    def test_assento_ou_evento_inexistente(self):
        self.assertVaiParaDLQ(envelope("reserva.criada", criar_reserva(1, 1), evento_id=1, numero=999, **CLIENTE))
        self.assertVaiParaDLQ(envelope("reserva.criada", criar_reserva(1, 1), evento_id=99, numero=1, **CLIENTE))

    def test_reserva_fantasma_nao_bloqueia_assento(self):
        # reserva_id que não está na tabela reservas (mensagem forjada/corrompida).
        self.assertVaiParaDLQ(envelope("reserva.criada", str(uuid.uuid4()), evento_id=1, numero=1, **CLIENTE))

    def test_payload_diferente_da_reserva_gravada(self):
        # A reserva foi feita para o assento 1, mas a mensagem pede o assento 2.
        r = criar_reserva(1, 1)
        self.assertVaiParaDLQ(envelope("reserva.criada", r, evento_id=1, numero=2, **CLIENTE))

    def test_evento_desconhecido(self):
        self.assertVaiParaDLQ(envelope("assento.qualquercoisa", criar_reserva(1, 1)))

    def test_numero_em_texto_com_digitos_e_aceito(self):
        r = criar_reserva(1, 3)
        self.assertEqual(self.reservar(r, "1", "3"), "assento.reservado")

    def test_json_corrompido_e_barrado_antes_do_servico(self):
        # Caminho real da mensagem: corpo -> _parse_envelope (common.py) -> processar.
        for corpo in (b"{nao e json", b"[]", b'{"event_type": "reserva.criada"}'):
            with self.subTest(corpo=corpo):
                with self.assertRaises(common.ErroProcessamento):
                    common._parse_envelope(corpo)


# --------------------------------------------------------------------------- #
# Conteúdo publicado (contrato com o payment_service)
# --------------------------------------------------------------------------- #
class TestEventoPublicado(BaseAssentos):

    def test_assento_reservado_leva_o_necessario_para_o_pagamento(self):
        a = criar_reserva(1, 8)
        self.reservar(a, 1, 8)
        tipo, rid, payload = self.publicados[0]
        self.assertEqual((tipo, rid), ("assento.reservado", a))
        self.assertEqual(payload["cartao"], CLIENTE["cartao"])
        self.assertEqual(payload["valor_centavos"], 8000)
        self.assertEqual((payload["evento_id"], payload["numero"]), (1, 8))

    def test_cartao_completo_nunca_vai_para_o_banco(self):
        a = criar_reserva(1, 8)
        self.reservar(a, 1, 8)
        self.aprovar(a)
        conn = db.connect()
        try:
            for tabela in ("assentos", "reservas", "pagamentos"):
                for linha in conn.execute(f"SELECT * FROM {tabela}"):
                    self.assertNotIn(CLIENTE["cartao"], [str(v) for v in tuple(linha)])
        finally:
            conn.close()

    def test_pagamento_aprovado_e_recusado_nao_publicam_nada(self):
        a, b = criar_reserva(1, 8), criar_reserva(1, 9)
        self.reservar(a, 1, 8)
        self.reservar(b, 1, 9)
        self.publicados.clear()
        self.assertIsNone(self.aprovar(a))
        self.assertIsNone(self.recusar(b))
        self.assertEqual(self.publicados, [])


# --------------------------------------------------------------------------- #
# Concorrência real: processos separados (como 2 terminais do serviço)
# --------------------------------------------------------------------------- #
class TestConcorrenciaEntreProcessos(BaseAssentos):

    def test_dez_pessoas_no_mesmo_assento_em_4_processos(self):
        jobs = [(criar_reserva(2, 1), 2, 1) for _ in range(10)]
        with mp.Pool(4) as pool:
            resultados = pool.map(_disputar_em_outro_processo, jobs, chunksize=1)
        self.assertEqual(resultados.count("assento.reservado"), 1, resultados)
        self.assertEqual(resultados.count("assento.indisponivel"), 9, resultados)
        vencedor = jobs[resultados.index("assento.reservado")][0]
        self.assertEqual(assento(2, 1), ("BLOQUEADO", vencedor))

    def test_duas_instancias_disputando_vinte_assentos(self):
        jobs = []
        for numero in range(1, 21):
            jobs += [(criar_reserva(2, numero), 2, numero), (criar_reserva(2, numero), 2, numero)]
        with mp.Pool(2) as pool:
            resultados = pool.map(_disputar_em_outro_processo, jobs, chunksize=1)
        for i in range(0, len(resultados), 2):
            par = sorted(resultados[i:i + 2])
            self.assertEqual(par, ["assento.indisponivel", "assento.reservado"], f"assento {i // 2 + 1}")

    def test_rajada_de_dez_assentos_diferentes_em_2_processos(self):
        jobs = [(criar_reserva(2, n), 2, n) for n in range(1, 11)]
        with mp.Pool(2) as pool:
            resultados = pool.map(_disputar_em_outro_processo, jobs, chunksize=1)
        self.assertEqual(resultados, ["assento.reservado"] * 10)


def tearDownModule():
    # Apaga o banco temporário (ignore_errors: no Windows o arquivo WAL pode estar preso).
    shutil.rmtree(Path(os.environ["DB_PATH"]).parent, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
