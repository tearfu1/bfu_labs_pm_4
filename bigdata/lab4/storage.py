import sqlite3
import json

class DataStorage:
    def __init__(self, db_name="lab_data.db"):
        self.db_name = db_name
        self._init_db()

    def _init_db(self):
        conn = self.get_conn()
        cur = conn.cursor()
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS documents (
                id INTEGER PRIMARY KEY,
                url TEXT UNIQUE,
                title TEXT,
                content TEXT
            );
            CREATE TABLE IF NOT EXISTS links (
                from_id INTEGER,
                to_id INTEGER,
                FOREIGN KEY(from_id) REFERENCES documents(id),
                FOREIGN KEY(to_id) REFERENCES documents(id)
            );
            CREATE TABLE IF NOT EXISTS terms (
                id INTEGER PRIMARY KEY,
                term TEXT UNIQUE
            );
            CREATE TABLE IF NOT EXISTS postings (
                term_id INTEGER,
                doc_id INTEGER,
                freq INTEGER,
                positions TEXT,
                FOREIGN KEY(term_id) REFERENCES terms(id),
                FOREIGN KEY(doc_id) REFERENCES documents(id)
            );
        """)
        conn.commit()
        conn.close()

    def get_conn(self):
        return sqlite3.connect(self.db_name)

    def clear_data(self):
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM documents")
        cur.execute("DELETE FROM links")
        cur.execute("DELETE FROM terms")
        cur.execute("DELETE FROM postings")
        conn.commit()
        conn.close()