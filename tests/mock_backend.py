import collections
import contextlib
import datetime
import sqlite3
import tempfile

from qfunnel.program import Backend, Job

OWN_USER = 'myuser'

class MockBackend(Backend):

    def __init__(self, db_file_name):
        super().__init__()
        self.db_file_name = db_file_name
        self.running_jobs_by_name = {}
        self.job_id_counter = 0

    def connect_to_db(self):
        return sqlite3.connect(self.db_file_name)

    def get_cwd(self):
        return '/fake/directory'

    def submit_job(self, queue, name, args, cwd):
        self.add_running_job(queue, name)

    def get_own_pending_jobs(self):
        # TODO Implement pending jobs.
        return []

    def get_running_jobs_in_queue(self, queue):
        return [job for job in self.get_own_running_jobs() if job.queue == queue]

    def get_own_jobs(self):
        return self.get_own_running_jobs()

    def add_running_job(self, queue, name, user=OWN_USER):
        if name in self.running_jobs_by_name:
            raise ValueError
        self.running_jobs_by_name[name] = Job(
            id=self.job_id_counter,
            user=user,
            name=name,
            slots=1,
            state='r',
            queue=queue,
            since=datetime.datetime(2022, 7, 9)
        )
        self.job_id_counter += 1

    def running_jobs(self):
        result = collections.defaultdict(set)
        for job in self.running_jobs_by_name.values():
            result[job.queue].add(job.name)
        return dict(result)

    def get_all_running_jobs(self):
        return sorted(self.running_jobs_by_name.values(), key=lambda job: job.id)

    def get_own_running_jobs(self):
        return [job for job in self.get_all_running_jobs() if job.user == OWN_USER]

    def get_queue_jobs(self, queue):
        return [job for job in self.get_all_running_jobs() if job.queue == queue]

    def finish_job(self, name):
        del self.running_jobs_by_name[name]

@contextlib.contextmanager
def get_mock_backend():
    with tempfile.NamedTemporaryFile() as db_file:
        yield MockBackend(db_file.name)
