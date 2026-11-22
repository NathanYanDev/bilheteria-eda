"""
Banco SQLite compartilhado pela API e por todos os workers.

Um único arquivo (DB_PATH) é lido e escrito por vários processos ao mesmo tempo
(a API + os workers, e mais de uma réplica do payment_service). Por isso:

  - journal_mode=WAL: leitores não bloqueiam o escritor, e o escritor não
    bloqueia os leitores. No modo padrão (DELETE/rollback journal) um UPDATE
    trava o arquivo inteiro para todo mundo.
  - timeout=10: se outro processo estiver no meio de uma escrita, esta conexão
    espera até 10 s pelo lock em vez de falhar na hora com "database is locked".
    A reserva de assento é um UPDATE curto; 10 s cobre a contenção de uma rajada.

Uso:
    python workers/db.py --reset    # apaga, recria o schema e carrega o seed
"""

import os
import sqlite3
import sys
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

_db_path = Path(os.getenv("DB_PATH", "data/bilheteria.db"))
DB_PATH = _db_path if _db_path.is_absolute() else BASE_DIR / _db_path

# Dois eventos, cada um com assentos de sobra para uma rajada de 10 reservas.
EVENTOS = [
    # (id, nome, local, data_hora, preco_centavos) — centavos evitam float de dinheiro
    (1, "Show da Banda Aurora", "Teatro Municipal", "2026-12-12 20:00", 8000),
    (2, "Festival Jazz no Parque", "Parque da Cidade", "2026-12-20 18:00", 5000),
]
ASSENTOS_POR_EVENTO = {1: 24, 2: 30}

SCHEMA = """
CREATE TABLE eventos (
    id             INTEGER PRIMARY KEY,
    nome           TEXT    NOT NULL,
    local          TEXT    NOT NULL,
    data_hora      TEXT    NOT NULL,
    preco_centavos INTEGER NOT NULL
);

-- status: LIVRE -> BLOQUEADO (reserva) -> VENDIDO (pagamento aprovado).
-- reserva_id fica preenchido enquanto o assento está BLOQUEADO ou VENDIDO,
-- para a idempotência: reprocessar a mesma reserva reconhece o próprio bloqueio.
CREATE TABLE assentos (
    evento_id  INTEGER NOT NULL REFERENCES eventos(id),
    numero     INTEGER NOT NULL,
    status     TEXT    NOT NULL DEFAULT 'LIVRE'
                       CHECK (status IN ('LIVRE', 'BLOQUEADO', 'VENDIDO')),
    reserva_id TEXT,
    PRIMARY KEY (evento_id, numero)
);

-- cartao_final: só os 4 últimos dígitos. O número completo nunca é gravado.
CREATE TABLE reservas (
    id           TEXT    PRIMARY KEY,
    evento_id    INTEGER NOT NULL,
    numero       INTEGER NOT NULL,
    nome         TEXT    NOT NULL,
    email        TEXT    NOT NULL,
    cartao_final TEXT    NOT NULL,
    status       TEXT    NOT NULL DEFAULT 'EM_PROCESSAMENTO',
    criado_em    TEXT    NOT NULL,
    FOREIGN KEY (evento_id, numero) REFERENCES assentos (evento_id, numero)
);

-- reserva_id é a PK: um segundo processamento da mesma reserva não insere
-- outra cobrança (idempotência do payment_service, entrega at-least-once).
CREATE TABLE pagamentos (
    reserva_id    TEXT PRIMARY KEY REFERENCES reservas(id),
    resultado     TEXT NOT NULL CHECK (resultado IN ('APROVADO', 'RECUSADO')),
    processado_em TEXT NOT NULL
);

-- reserva_id UNIQUE: emitir o ingresso duas vezes para a mesma reserva falha
-- no INSERT, em vez de gerar dois códigos.
CREATE TABLE ingressos (
    codigo     TEXT PRIMARY KEY,
    reserva_id TEXT NOT NULL UNIQUE REFERENCES reservas(id),
    emitido_em TEXT NOT NULL
);
"""


def connect():
    """Abre uma conexão pronta para uso concorrente (WAL + timeout + FK)."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def reset():
    """Apaga as tabelas, recria o schema e insere o seed. Pode rodar várias vezes."""
    conn = connect()
    try:
        # Ordem inversa das FKs. executescript faz COMMIT implícito antes de rodar.
        conn.executescript(
            """
            DROP TABLE IF EXISTS ingressos;
            DROP TABLE IF EXISTS pagamentos;
            DROP TABLE IF EXISTS reservas;
            DROP TABLE IF EXISTS assentos;
            DROP TABLE IF EXISTS eventos;
            """
            + SCHEMA
        )
        conn.executemany(
            "INSERT INTO eventos (id, nome, local, data_hora, preco_centavos) VALUES (?, ?, ?, ?, ?)",
            EVENTOS,
        )
        assentos = [
            (evento_id, numero)
            for evento_id, quantidade in ASSENTOS_POR_EVENTO.items()
            for numero in range(1, quantidade + 1)
        ]
        conn.executemany(
            "INSERT INTO assentos (evento_id, numero) VALUES (?, ?)",
            assentos,
        )
        conn.commit()
        _mostrar(conn)
    finally:
        conn.close()


def _mostrar(conn):
    print(f"banco: {DB_PATH}")
    print(f"{'id':<4} {'evento':<28} {'assentos livres':>15}")
    rows = conn.execute(
        """
        SELECT e.id, e.nome, COUNT(a.numero) AS livres
        FROM eventos e
        JOIN assentos a ON a.evento_id = e.id
        WHERE a.status = 'LIVRE'
        GROUP BY e.id
        ORDER BY e.id
        """
    )
    for row in rows:
        print(f"{row['id']:<4} {row['nome']:<28} {row['livres']:>15}")


if __name__ == "__main__":
    if "--reset" in sys.argv:
        reset()
    else:
        sys.exit("Uso: python workers/db.py --reset")
