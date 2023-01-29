import datetime
import itertools
import os
import pathlib
import sqlite3
import subprocess
import xml.etree.ElementTree

from .program import Backend, Job

DB_DIR = pathlib.Path.home() / '.local' / 'share'
DB_FILE = DB_DIR / 'qfunnel.db'

class RealBackend(Backend):

    def get_cwd(self):
        return str(pathlib.Path.cwd())

    def get_own_user(self):
        return os.environ['USER']

    def connect_to_db(self):
        DB_DIR.mkdir(parents=True, exist_ok=True)
        # Give the connection a generous timeout, since the database is locked
        # while running qstat.
        return sqlite3.connect(DB_FILE, timeout=60.0)

    def submit_job(self, queue, name, args, cwd):
        run_sge_command([
            'qsub',
            '-q', queue,
            '-N', name,
            '-w', 'w',
            *args
        ], cwd=cwd)

    def delete_jobs(self, job_ids):
        run_sge_command(['qdel', *job_ids])

    def get_own_jobs(self):
        output = capture_sge_command_output([
            'qstat',
            '-u', self.get_own_user(),
            '-r',
            '-xml'
        ])
        root = parse_xml_output(output)
        running_jobs = parse_xml_jobs(root.find('queue_info'))
        pending_jobs = parse_xml_jobs(root.find('job_info'))
        return [dict_to_job(job) for job in itertools.chain(running_jobs, pending_jobs)]

    def get_own_pending_jobs(self):
        output = capture_sge_command_output([
            'qstat',
            '-u', self.get_own_user(),
            '-r',
            '-xml'
        ])
        root = parse_xml_output(output)
        pending_jobs = parse_xml_jobs(root.find('job_info'))
        return [dict_to_job(job) for job in pending_jobs]

    def get_own_running_jobs_in_queue(self, queue):
        output = capture_sge_command_output([
            'qstat',
            '-u', self.get_own_user(),
            '-q', queue,
            '-s', 'r',
            '-r',
            '-xml'
        ])
        root = parse_xml_output(output)
        jobs = parse_xml_jobs(root.find('queue_info'))
        return [dict_to_job(job) for job in jobs]

    def get_running_jobs_in_queue(self, queue):
        output = capture_sge_command_output([
            'qstat',
            '-q', queue,
            '-s', 'r',
            '-r',
            '-xml'
        ])
        root = parse_xml_output(output)
        jobs = parse_xml_jobs(root.find('queue_info'))
        return [dict_to_job(job) for job in jobs]

def run_sge_command(args, **kwargs):
    return subprocess.run(args, **kwargs)

def capture_sge_command_output(args):
    return run_sge_command(args, capture_output=True, encoding='ascii').stdout

def parse_xml_output(s):
    return xml.etree.ElementTree.fromstring(s)

def parse_xml_jobs(el):
    for job_list in el.findall('job_list'):
        yield xml_node_to_dict(job_list)

def xml_node_to_dict(el):
    return {
        child.tag : xml_node_to_dict(child) if len(child) > 0 else child.text
        for child in el
    }

def dict_to_job(d):
    return Job(
        id=d['JB_job_number'],
        user=d['JB_owner'],
        name=d['full_job_name'],
        slots=int(d['slots']),
        state=d['state'],
        queue=d['queue_name'] or d['request']['hard_req_queue'],
        since=parse_job_date(d.get('JAT_start_time') or d['JB_submission_time'])
    )

def parse_job_date(s):
    return datetime.datetime.fromisoformat(s)
