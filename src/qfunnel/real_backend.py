import pathlib
import sqlite3

from .program import Backend

DB_DIR = pathlib.Path.home() / '.local' / 'share'
DB_FILE = DB_DIR / 'qfunnel.db'

class RealBackend(Backend):

    def connect_to_db(self):
        DB_DIR.mkdir(parents=True, exist_ok=True)
        # Give the connection a generous timeout, since the database is locked
        # while running qstat.
        return sqlite3.connect(DB_FILE, timeout=60.0)

    def get_cwd(self):
        return str(pathlib.Path.cwd())
