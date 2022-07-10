import collections
import contextlib
import dataclasses
import datetime
import sqlite3
import tempfile

from qfunnel.program import Backend, Job

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
        if name in self.running_jobs_by_name:
            raise ValueError
        self.running_jobs_by_name[name] = JobInfo(
            job=Job(
                id=self.job_id_counter,
                name=name,
                slots=1,
                state='r',
                queue=queue,
                since=datetime.datetime(2022, 7, 9)
            ),
            queue=queue
        )
        self.job_id_counter += 1

    def get_all_pending_jobs(self):
        # TODO Implement pending jobs.
        return []

    def get_running_jobs_in_queue(self, queue):
        jobs_in_queue = (job.job for job in self.running_jobs_by_name.values() if job.queue == queue)
        return sorted(jobs_in_queue, key=lambda job: job.id)

    def get_own_jobs(self):
        own_jobs = (job.job for job in self.running_jobs_by_name.values())
        return sorted(own_jobs, key=lambda job: job.id)

    def running_jobs(self):
        result = collections.defaultdict(set)
        for job in self.running_jobs_by_name.values():
            result[job.queue].add(job.job.name)
        return dict(result)

    def finish_job(self, name):
        del self.running_jobs_by_name[name]

@dataclasses.dataclass
class JobInfo:
    job: Job
    queue: str

@contextlib.contextmanager
def get_mock_backend():
    with tempfile.NamedTemporaryFile() as db_file:
        yield MockBackend(db_file.name)
