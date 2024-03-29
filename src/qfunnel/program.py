import collections
import contextlib
import dataclasses
import datetime
import itertools
import json
import math
import pathlib
import re
import sqlite3
import sys
import time
import traceback

class Program:

    def __init__(self, backend):
        super().__init__()
        self.backend = backend

    def get_limit(self, queue):
        with self.get_db_connection() as conn:
            result = conn.execute('''\
select "value"
from "limits"
where "queue" = ?
''', (queue,)).fetchone()
            if result is not None:
                return result[0]
            else:
                return None

    def get_all_limits(self):
        with self.get_db_connection() as conn:
            return list(conn.execute('''\
select "queue", "value"
from "limits"
order by "queue" asc
''').fetchall())

    def set_limit(self, queue, limit):
        if limit < 0:
            raise ValueError('limit cannot be negative')
        with self.get_db_connection() as conn:
            with self.lock_db(conn):
                conn.execute('''\
insert or replace into "limits"("queue", "value")
values (?, ?)
''', (queue, limit))

    def delete_limit(self, queue):
        with self.get_db_connection() as conn:
            with self.lock_db(conn):
                conn.execute('''\
delete from "limits"
where "queue" = ?
''', (queue,))

    def submit(self, queues, name, args, deferred=False):
        command_json = json.dumps(args, separators=(',', ':'))
        cwd = self.backend.get_cwd()
        with self.get_db_connection() as conn:
            with self.lock_db(conn):
                curs = conn.execute('''\
insert into "jobs"("name", "command_json", "cwd")
values (?, ?, ?)
''', (name, command_json, cwd))
                job_id = curs.lastrowid
                for queue in queues:
                    conn.execute('''\
insert into "job_queues"("job_id", "queue")
values (?, ?)
''', (job_id, queue))
            if not deferred:
                self.check_impl(conn)

    def list_queue_jobs(self, queue, job_filter=None):
        with self.get_db_connection() as conn:
            jobs = self.get_queue_jobs(conn, queue)
            own_user = self.backend.get_own_user()
            taken = sum(job.slots for job in jobs if job.user == own_user and job.state != '-')
            row = conn.execute('''\
select "value" from "limits" where "queue" = ?
''', (queue,)).fetchone()
            if row is not None:
                limit, = row
            else:
                limit = None
        if job_filter is not None:
            jobs = list(job_filter.filter_jobs(jobs))
        return ListQueueInfo(jobs, Capacity(taken, limit))

    def list_own_jobs(self, job_filter=None):
        with self.get_db_connection() as conn:
            own_backend_jobs = self.backend.get_own_jobs()
            rows = conn.execute('''\
select "id", "name" from "jobs" order by "id" asc
''').fetchall()
            local_jobs = self.get_local_jobs(conn, rows)
            jobs = [*own_backend_jobs, *local_jobs]
            rows = conn.execute('''\
select "queue", "value"
from "limits"
order by "queue" asc
''').fetchall()
            pending_jobs = collections.defaultdict(list)
            for job in own_backend_jobs:
                if job.state == 'qw':
                    pending_jobs[job.queue].append(job)
            queues = []
            for queue, limit in rows:
                queue_jobs = self.merge_pending_and_running_jobs(
                    pending_jobs[queue],
                    self.backend.get_own_running_jobs_in_queue(queue)
                )
                taken = sum(job.slots for job in queue_jobs)
                queues.append((queue, Capacity(taken, limit)))
        if job_filter is not None:
            jobs = list(job_filter.filter_jobs(jobs))
        return ListOwnInfo(jobs, queues)

    def check(self):
        with self.get_db_connection() as conn:
            self.check_impl(conn)

    def watch(self, seconds, stdout=sys.stdout, stderr=sys.stderr):
        while True:
            self.log_message('checking...', stdout)
            try:
                self.check()
            except Exception as e:
                print(traceback.format_exc(), end='', file=stderr)
            self.log_message('...done', stdout)
            time.sleep(seconds)

    def delete(self, job_ids):
        backend_jobs, local_jobs = self.parse_job_ids(job_ids)
        if local_jobs:
            with self.get_db_connection() as conn:
                with self.lock_db(conn):
                    for job_id in local_jobs:
                        self.delete_local_job(conn, job_id)
        if backend_jobs:
            self.backend.delete_jobs(backend_jobs)

    def bump(self, job_filter):
        with self.get_db_connection() as conn:
            with self.lock_db(conn):
                conn.execute('pragma defer_foreign_keys = ON')
                rows = conn.execute('''\
select "id", "name" from "jobs" order by "id" asc
''').fetchall()
                all_jobs = self.get_local_jobs(conn, rows)
                selected_jobs = []
                unselected_jobs = []
                for job in all_jobs:
                    if job_filter.matches_job(job):
                        selected_jobs.append(job)
                    else:
                        unselected_jobs.append(job)
                max_selected_id = max(job.local_id for job in selected_jobs) if selected_jobs else -math.inf
                min_unselected_id = min(job.local_id for job in unselected_jobs) if unselected_jobs else math.inf
                if max_selected_id < min_unselected_id:
                    # Do nothing; the jobs are already in the correct order.
                    pass
                else:
                    offset = max_selected_id + 1 - min_unselected_id
                    for job in reversed(unselected_jobs):
                        new_id = job.local_id + offset
                        conn.execute('''\
update "jobs"
set "id" = ?
where "id" = ?
''', (new_id, job.local_id))
                        # TODO It might be better to use ON UPDATE CASCADE.
                        conn.execute('''\
update "job_queues"
set "job_id" = ?
where "job_id" = ?
''', (new_id, job.local_id))

    @contextlib.contextmanager
    def get_db_connection(self):
        conn = self.backend.connect_to_db()
        try:
            conn.execute('pragma foreign_keys = ON')
            with self.lock_db(conn):
                self.ensure_db_initialized(conn)
            yield conn
        finally:
            conn.close()

    def ensure_db_initialized(self, conn):
        try:
            conn.execute('select "value" from "limits" limit 1')
        except sqlite3.OperationalError as e:
            if e.args[0] == 'no such table: limits':
                self.initialize_db(conn)
            else:
                raise

    def initialize_db(self, conn):
        conn.executescript(TABLES_FILE.read_text())

    @contextlib.contextmanager
    def lock_db(self, conn):
        conn.execute('begin exclusive')
        try:
            yield
        except:
            conn.rollback()
            raise
        else:
            conn.commit()

    def check_impl(self, conn):
        while self.try_dequeue_one(conn):
            pass

    def try_dequeue_one(self, conn):
        with self.lock_db(conn):
            # The "id" of the "job" table determines the priority of each
            # locally buffered job. Using min() on "id" ensures that all the
            # other columns are for the highest-priority job for each queue.
            # (This is a special case in SQLite.)
            # The rowid of "job_queues" determines the order in which to test
            # queues for the same job.
            rows = conn.execute('''\
select
  "job_queues"."queue" as "queue",
  (
    select "value"
    from "limits"
    where "limits"."queue" = "job_queues"."queue"
  ) as "limit",
  min("jobs"."id") as "job_id",
  "jobs"."name" as "name",
  "jobs"."command_json" as "command_json",
  "jobs"."cwd" as "cwd"
from "job_queues"
  join "jobs" on "job_queues"."job_id" = "jobs"."id"
group by "job_queues"."queue"
order by "jobs"."id", "job_queues".rowid asc
''').fetchall()
            for queue, limit, job_id, name, command_json, cwd in rows:
                if limit is None or self.queue_has_open_slots(queue, limit):
                    args = json.loads(command_json)
                    self.delete_local_job(conn, job_id)
                    self.backend.submit_job(queue, name, args, cwd)
                    return True
        return False

    def queue_has_open_slots(self, queue, limit):
        slots = 0
        if slots >= limit:
            return False
        for job in self.enumerate_own_backend_jobs_in_queue(queue):
            slots += job.slots
            if slots >= limit:
                return False
        return True

    def enumerate_own_backend_jobs_in_queue(self, queue):
        # Query the pending jobs before querying the running jobs to avoid
        # a race condition where a pending job becomes a running job in
        # between queries.
        pending_jobs = (
            job
            for job in self.backend.get_own_pending_jobs()
            if job.queue == queue
        )
        running_jobs = self.backend.get_own_running_jobs_in_queue(queue)
        return self.merge_pending_and_running_jobs(pending_jobs, running_jobs)

    def merge_pending_and_running_jobs(self, pending_jobs, running_jobs):
        # Pending jobs should be queried before the running jobs to avoid
        # a race condition where a pending job becomes a running job in
        # between queries.
        pending_jobs_by_id = collections.OrderedDict((job.id, job) for job in pending_jobs)
        running_jobs = list(running_jobs)
        for job in running_jobs:
            pending_jobs_by_id.pop(job.id, None)
        return [*running_jobs, *pending_jobs_by_id.values()]

    def get_local_jobs(self, conn, rows, include_queue=True):
        # We could fetch all the queues for each job in one query using
        # group_concat(), but according to the SQLite docs, the order of
        # concatenation is arbitrary.
        user = self.backend.get_own_user()
        for job_id, name in rows:
            if include_queue:
                queue_rows = conn.execute('''\
select "queue"
from "job_queues"
where "job_id" = ?
order by rowid asc
''', (job_id,)).fetchall()
                queue = ' '.join(queue for queue, in queue_rows)
            else:
                queue = None
            yield Job(
                id=f'x{job_id}',
                user=user,
                name=name,
                slots=1,
                state='-',
                queue=queue,
                # TODO Add timestamp to database?
                since=None,
                local_id=job_id
            )

    def get_queue_jobs(self, conn, queue):
        own_pending_jobs = (
            job
            for job in self.backend.get_own_pending_jobs()
            if job.queue == queue
        )
        running_jobs = self.backend.get_running_jobs_in_queue(queue)
        backend_jobs = self.merge_pending_and_running_jobs(own_pending_jobs, running_jobs)
        rows = conn.execute('''\
select "id", "name"
from "jobs"
where exists (
  select 1 from "job_queues" where "job_id" = "jobs"."id" and "queue" = ?
)
order by "id" asc
''', (queue,)).fetchall()
        local_jobs = self.get_local_jobs(conn, rows)
        return [*backend_jobs, *local_jobs]

    def parse_job_ids(self, job_ids):
        local_job_id_re = re.compile(r'^x([0-9]+)$')
        backend_jobs = []
        local_jobs = []
        for job_id in job_ids:
            m = local_job_id_re.match(job_id)
            if m is not None:
                local_jobs.append(int(m.group(1)))
            else:
                backend_jobs.append(job_id)
        return backend_jobs, local_jobs

    def delete_local_job(self, conn, job_id):
        # TODO It might be better to use ON DELETE CASCADE.
        conn.execute('''\
delete from "job_queues" where "job_id" = ?
''', (job_id,))
        conn.execute('''\
delete from "jobs" where "id" = ?
''', (job_id,))

    def log_message(self, message, file):
        time_str = datetime.datetime.now().strftime('%a %b %d %Y @ %I:%M:%S %p')
        print(f'[{time_str}] {message}', file=file)

class Backend:

    def get_cwd(self):
        """Get the current directory."""
        raise NotImplementedError

    def get_own_user(self):
        """Get the current user."""
        raise NotImplementedError

    def connect_to_db(self):
        """Open a connection to the SQLite database."""
        raise NotImplementedError

    def submit_job(self, queue, name, args, cwd):
        """Submit a command to the backend."""
        raise NotImplementedError

    def delete_jobs(self, job_ids):
        """Cancel one or more running or pending jobs."""
        raise NotImplementedError

    def get_own_jobs(self):
        """Return a list of pending and running jobs for the current user."""
        raise NotImplementedError

    def get_own_pending_jobs(self):
        """Return a list of all pending jobs for the current user."""
        raise NotImplementedError

    def get_own_running_jobs_in_queue(self, queue):
        """Return a list of running jobs in a queue for the current user."""
        raise NotImplementedError

    def get_running_jobs_in_queue(self, queue):
        """Return a list of running jobs in a queue for all users."""
        raise NotImplementedError

@dataclasses.dataclass
class Job:
    id: str
    user: str
    name: str
    slots: int
    state: str
    queue: str
    since: datetime.datetime
    local_id: int=None

@dataclasses.dataclass
class Capacity:
    taken: int
    limit: int

@dataclasses.dataclass
class ListQueueInfo:
    jobs: list
    capacity: Capacity

@dataclasses.dataclass
class ListOwnInfo:
    jobs: list
    queues: list

TABLES_FILE = pathlib.Path(__file__).parent / 'tables.sqlite'

@dataclasses.dataclass
class JobFilter:
    name: str

    def __post_init__(self):
        self._name_re = None

    def filter_jobs(self, jobs):
        return filter(self.matches_job, jobs)

    def matches_job(self, job):
        return (self.name is None or self.name_re.search(job.name) is not None)

    @property
    def name_re(self):
        if self._name_re is None:
            self._name_re = re.compile(self.name)
        return self._name_re

    def is_empty(self):
        return self.name is None
