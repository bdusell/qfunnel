import collections
import contextlib
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
        self.capacities = {}

    def connect_to_db(self):
        return sqlite3.connect(self.db_file_name)

    def get_cwd(self):
        return '/fake/directory'

    def get_own_user(self):
        return 'myuser'

    def submit_job(self, queue, name, args, cwd):
        self.add_job(queue, name)

    def get_own_pending_jobs(self):
        own_user = self.get_own_user()
        return [job for job in self.get_all_jobs() if job.user == own_user and job.state == 'qw']

    def get_own_running_jobs_in_queue(self, queue):
        own_user = self.get_own_user()
        return [job for job in self.get_all_jobs() if job.user == own_user and job.queue == queue and job.state == 'r']

    def get_own_jobs(self):
        own_user = self.get_own_user()
        return [job for job in self.get_all_jobs() if job.user == own_user]

    def get_running_jobs_in_queue(self, queue):
        return [job for job in self.get_all_jobs() if job.queue == queue and job.state == 'r']

    def add_job(self, queue, name, user=None):
        if user is None:
            user = self.get_own_user()
        if name in self.running_jobs_by_name:
            raise ValueError
        capacity = self.capacities.get(queue)
        if capacity is not None:
            num_running = sum(job.slots for job in self.get_all_jobs() if job.queue == queue and job.state == 'r')
            if num_running < capacity:
                state = 'r'
            else:
                state = 'qw'
        else:
            state = 'r'
        self.running_jobs_by_name[name] = Job(
            id=self.job_id_counter,
            user=user,
            name=name,
            slots=1,
            state=state,
            queue=queue,
            since=datetime.datetime(2022, 7, 9)
        )
        self.job_id_counter += 1

    def jobs_with_state(self, state):
        result = collections.defaultdict(set)
        for job in self.running_jobs_by_name.values():
            if job.state == state:
                result[job.queue].add(job.name)
        return dict(result)

    def running_jobs(self):
        return self.jobs_with_state('r')

    def pending_jobs(self):
        return self.jobs_with_state('qw')

    def get_all_jobs(self):
        return sorted(self.running_jobs_by_name.values(), key=lambda job: job.id)

    def finish_job(self, name):
        old_job = self.running_jobs_by_name.pop(name)
        pending_jobs = self.get_own_pending_jobs()
        if pending_jobs:
            pending_jobs[0].state = 'r'

    def set_capacity(self, queue, value):
        self.capacities[queue] = value

@contextlib.contextmanager
def get_mock_backend():
    with tempfile.NamedTemporaryFile() as db_file:
        yield MockBackend(db_file.name)
