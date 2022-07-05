import argparse
import contextlib
import json
import pathlib
import shlex
import sqlite3
import subprocess
import time

from qfunnel.format import format_box_table, format_date
from qfunnel.parse_sge import parse_table, get_job_id, get_since
from qfunnel.run_sge import capture_sge_command_output
from qfunnel.util import get_current_user

DB_DIR = pathlib.Path.home() / '.local'
DB_FILE = DB_DIR / 'qfunnel.db'

@contextlib.contextmanager
def get_db_connection():
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
    try:
        ensure_db_initialized(conn)
        yield conn
    finally:
        conn.close()

def ensure_db_initialized(conn):
    try:
        get_limit(conn)
    except sqlite3.OperationalError as e:
        if e.args[0] == 'no such table: limit':
            initialize_db(conn)
        elif e.args[0] == 'database is locked':
            # This is ok, because it indicates the database has been created.
            pass
        else:
            raise

def get_limit(conn):
    value, = conn.execute('select "value" from "limit" limit 1').fetchone()
    return value

DEFAULT_LIMIT = 10
TABLES_FILE = pathlib.Path(__file__).parent / 'tables.sqlite'

def initialize_db(conn):
    code = TABLES_FILE.read_text()
    conn.executescript(f'''\
begin;
{code}
insert into "limit"("value") values ({DEFAULT_LIMIT});
commit;
''')

@contextlib.contextmanager
def lock_db(conn):
    def acquire_lock():
        try:
            conn.execute('begin exclusive')
        except sqlite3.OperationalError as e:
            if e.args[0] == 'database is locked':
                print('failed to get lock')
                return False, None
            else:
                raise
        else:
            return True, None
    wait(acquire_lock)
    try:
        yield
    except:
        conn.execute('rollback')
        raise
    else:
        conn.execute('commit')

def wait(func, seconds=1, max_retries=10):
    for i in range(max_retries):
        if i > 0:
            time.sleep(seconds)
        success, result = func()
        if success:
            return result
    raise ValueError('too many retries')

def run_check_impl(conn):
    while try_dequeue_one(conn):
        pass

def try_dequeue_one(conn):
    with lock_db(conn):
        limit = get_limit(conn)
        taken = get_num_taken_slots()
        if taken < limit:
            return dequeue_one(conn)
        else:
            return False

def dequeue_one(conn):
    row = conn.execute('select rowid, "name", "command_json", "cwd" from "jobs" order by rowid asc limit 1').fetchone()
    if row is None:
        return False
    else:
        rowid, name, command_json, cwd = row
        command_args = json.loads(command_json)
        submit_job_to_backend(name, command_args, cwd)
        conn.execute('delete from "jobs" where rowid = ?', (rowid,))
        return True

def get_backend_jobs(user, queue=None):
    args = ['qstat', '-u', user]
    if queue is not None:
        args.extend(['-q', queue])
    rows = parse_table(capture_sge_command_output(args))
    return (row for row in rows if row['user'] == user)

QUEUE = 'gpu@@nlp-gpu'

def get_num_taken_slots():
    return sum(1 for row in get_backend_jobs(user=get_current_user(), queue=QUEUE))

def submit_job_to_backend(name, args, cwd):
    qsub_command_args = ['qsub', '-N', name, '-q', QUEUE, '-l', 'gpu_card=1', '-w', 'w'] + args
    print('; '.join(' '.join(shlex.quote(s) for s in command) for command in [['cd', cwd], qsub_command_args]))
    subprocess.run(qsub_command_args, cwd=cwd)

def run_setlimit(value):
    with get_db_connection() as conn:
        with conn:
            conn.execute('update "limit" set "value" = ?', (value,))
        run_check_impl(conn)

def run_submit(name, command):
    command_json = json.dumps(command, separators=(',', ':'))
    cwd = str(pathlib.Path.cwd())
    with get_db_connection() as conn:
        with conn:
            conn.execute(
                'insert into "jobs"("name", "command_json", "cwd") values (?, ?, ?)',
                (name, command_json, cwd))
        run_check_impl(conn)

def run_list():
    rows = []
    user = get_current_user()
    sge_rows = get_backend_jobs(user=get_current_user())
    for row in sge_rows:
        rows.append((
            get_job_id(row),
            row['name'],
            row['state'],
            row['queue'],
            format_date(get_since(row))
        ))
    with get_db_connection() as conn:
        for name, command_json in conn.execute('select "name", "command_json" from "jobs" order by rowid asc'):
            rows.append((
                '',
                name,
                '-',
                '',
                ''
            ))
        limit = get_limit(conn)
    table = format_box_table(
        head=['ID', 'Name', 'State', 'Queue', 'Since'],
        rows=rows
    )
    for line in table:
        print(line)
    taken = get_num_taken_slots()
    print(f'Taken: {taken}/{limit} ({max(0, limit-taken)} free)')

def run_check():
    with get_db_connection() as conn:
        run_check_impl(conn)

def run_watch(seconds):
    while True:
        run_check()
        try:
            time.sleep(seconds)
        except KeyboardInterrupt:
            print()
            break

def main():

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command', required=True)

    setlimit_parser = subparsers.add_parser('setlimit',
        help='Set the maximum number of jobs that to be run concurrently in '
             'the backend queue.')
    setlimit_parser.add_argument('value', type=int)

    submit_parser = subparsers.add_parser('submit',
        help='Submit a new command to be run. Run it immediately if there is '
             'room in the backend queue, otherwise enqueue it locally to be '
             'run later.')
    submit_parser.add_argument('--name', required=True,
        help='A name to identify this job.')
    submit_parser.add_argument('args', nargs=argparse.REMAINDER,
        help='The command to be run. This should be a full qsub command '
             'except for the "qsub", "-q", and "-l gpu_card=..." parts.')

    list_parser = subparsers.add_parser('list',
        help='List all of the jobs currently running in the backend queue and '
             'queued locally.')

    check_parser = subparsers.add_parser('check',
        help='Check how many jobs are currently running in the backend queue, '
             'and if there is room, dequeue and submit locally queued jobs.')

    watch_parser = subparsers.add_parser('watch',
        help='Enter a loop that runs "check" at regular intervals, in seconds. '
             'Default is every 5 minutes.')
    watch_parser.add_argument('seconds', type=int, nargs='?', default=600)

    args = parser.parse_args()

    if args.command == 'setlimit':
        run_setlimit(args.value)
    elif args.command == 'submit':
        command_args = args.args
        if command_args and command_args[0] == '--':
            command_args = command_args[1:]
        run_submit(args.name, command_args)
    elif args.command == 'list':
        run_list()
    elif args.command == 'check':
        run_check()
    elif args.command == 'watch':
        run_watch(args.seconds)

if __name__ == '__main__':
    main()
