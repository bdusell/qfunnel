from qfunnel.cli import Program

from mock_backend import get_mock_backend

def test_limits():
    with get_mock_backend() as backend:
        program = Program(backend)
        assert program.get_limit('gpu@@a') is None
        assert program.get_limit('gpu@@b') is None
        assert program.get_all_limits() == []
        program.set_limit('gpu@@a', 5)
        assert program.get_limit('gpu@@a') == 5
        assert program.get_limit('gpu@@b') is None
        assert program.get_all_limits() == [('gpu@@a', 5)]
        program.set_limit('gpu@@b', 3)
        assert program.get_limit('gpu@@a') == 5
        assert program.get_limit('gpu@@b') == 3
        assert program.get_all_limits() == [('gpu@@a', 5), ('gpu@@b', 3)]
        program.set_limit('gpu@@a', 10)
        assert program.get_limit('gpu@@a') == 10
        program.delete_limit('gpu@@a')
        assert program.get_limit('gpu@@a') is None
        assert program.get_all_limits() == [('gpu@@b', 3)]
        program.delete_limit('gpu@@a')
        assert program.get_limit('gpu@@a') is None
        assert program.get_all_limits() == [('gpu@@b', 3)]

def test_submit_no_limit():
    with get_mock_backend() as backend:
        program = Program(backend)
        program.submit(['gpu@@a'], 'job', ['script.bash'])
        assert backend.running_jobs() == { 'gpu@@a' : {'job'} }

def test_submit_fallback():
    with get_mock_backend() as backend:
        program = Program(backend)
        program.set_limit('gpu@@a', 5)
        program.set_limit('gpu@@b', 3)
        for i in range(0, 5):
            program.submit(['gpu@@a', 'gpu@@b'], f'job-{i}', ['script.bash'])
        assert backend.running_jobs() == { 'gpu@@a' : {f'job-{i}' for i in range(0, 5)} }
        for i in range(5, 8):
            program.submit(['gpu@@a', 'gpu@@b'], f'job-{i}', ['script.bash'])
        assert backend.running_jobs() == {
            'gpu@@a' : {f'job-{i}' for i in range(0, 5)},
            'gpu@@b' : {f'job-{i}' for i in range(5, 8)}
        }
        for i in range(8, 15):
            program.submit(['gpu@@a', 'gpu@@b'], f'job-{i}', ['script.bash'])
        assert backend.running_jobs() == {
            'gpu@@a' : {f'job-{i}' for i in range(0, 5)},
            'gpu@@b' : {f'job-{i}' for i in range(5, 8)}
        }

def test_buffered_fallback():
    with get_mock_backend() as backend:
        program = Program(backend)
        program.set_limit('gpu@@a', 1)
        program.set_limit('gpu@@b', 1)
        program.submit(['gpu@@a'], 'first-a', ['script.bash'])
        assert backend.running_jobs() == { 'gpu@@a' : {'first-a'} }
        program.submit(['gpu@@b'], 'first-b', ['script.bash'])
        assert backend.running_jobs() == { 'gpu@@a' : {'first-a'}, 'gpu@@b' : {'first-b'} }
        program.submit(['gpu@@a'], 'job-a', ['script.bash'])
        assert backend.running_jobs() == { 'gpu@@a' : {'first-a'}, 'gpu@@b' : {'first-b'} }
        program.submit(['gpu@@a', 'gpu@@b'], 'job-ab', ['script.bash'])
        assert backend.running_jobs() == { 'gpu@@a' : {'first-a'}, 'gpu@@b' : {'first-b'} }
        program.submit(['gpu@@a'], 'job-a2', ['script.bash'])
        assert backend.running_jobs() == { 'gpu@@a' : {'first-a'}, 'gpu@@b' : {'first-b'} }
        program.submit(['gpu@@b'], 'job-b', ['script.bash'])
        assert backend.running_jobs() == { 'gpu@@a' : {'first-a'}, 'gpu@@b' : {'first-b'} }
        for i in range(10):
            program.submit(['gpu@@a', 'gpu@@b'], f'extra-{i}', ['script.bash'])
        assert backend.running_jobs() == { 'gpu@@a' : {'first-a'}, 'gpu@@b' : {'first-b'} }
        backend.finish_job('first-a')
        backend.finish_job('first-b')
        program.check()
        assert backend.running_jobs() == { 'gpu@@a' : {'job-a'}, 'gpu@@b' : {'job-ab'} }
        backend.finish_job('job-ab')
        program.check()
        assert backend.running_jobs() == { 'gpu@@a' : {'job-a'}, 'gpu@@b' : {'job-b'} }

def test_submit_pending():
    with get_mock_backend() as backend:
        backend.set_capacity('gpu@@a', 3)
        program = Program(backend)
        program.set_limit('gpu@@a', 5)
        for i in range(10):
            program.submit(['gpu@@a', 'gpu@@b'], f'job-{i}', ['script.bash'])
        assert backend.running_jobs() == {
            'gpu@@a' : {f'job-{i}' for i in range(3)},
            'gpu@@b' : {f'job-{i}' for i in range(5, 10)}
        }
        assert backend.pending_jobs() == { 'gpu@@a' : {f'job-{i}' for i in range(3, 5)} }

def test_list():
    with get_mock_backend() as backend:
        program = Program(backend)
        program.set_limit('gpu@@a', 6)
        program.set_limit('gpu@@b', 0)
        for i in range(10):
            program.submit(['gpu@@a', 'gpu@@b'], f'job-{i}', ['script.bash'])
        info = program.list_own_jobs()
        jobs = info.jobs
        assert len(jobs) == 10
        for job in jobs[:6]:
            assert job.state == 'r'
            assert job.queue == 'gpu@@a'
        for job in jobs[6:]:
            assert job.state == '-'
            assert job.queue == 'gpu@@a gpu@@b'
        assert len(info.queues) == 2
        (name_a, info_a), (name_b, info_b) = info.queues
        assert name_a == 'gpu@@a'
        assert name_b == 'gpu@@b'
        assert info_a.taken == 6
        assert info_a.limit == 6
        assert info_b.taken == 0
        assert info_b.limit == 0

def test_list_queue():
    with get_mock_backend() as backend:
        program = Program(backend)
        for i in range(5):
            backend.add_job('gpu@@a', f'other-{i}', 'otheruser')
        program.set_limit('gpu@@a', 3)
        program.set_limit('gpu@@b', 3)
        for i in range(10):
            program.submit(['gpu@@a', 'gpu@@b'], f'job-{i}', ['script.bash'])
        info = program.list_queue_jobs('gpu@@a')
        jobs = info.jobs
        assert len(jobs) == 5 + 3 + 4
        for job in jobs[:5]:
            assert job.user == 'otheruser'
            assert job.state == 'r'
            assert job.queue == 'gpu@@a'
        for job in jobs[5:8]:
            assert job.user == backend.get_own_user()
            assert job.state == 'r'
            assert job.queue == 'gpu@@a'
        for job in jobs[8:]:
            assert job.state == '-'
            assert job.queue == 'gpu@@a gpu@@b'
        assert info.capacity.taken == 3
        assert info.capacity.limit == 3

def test_list_queue_pending():
    with get_mock_backend() as backend:
        backend.set_capacity('gpu@@a', 3)
        program = Program(backend)
        program.set_limit('gpu@@a', 5)
        for i in range(10):
            program.submit(['gpu@@a'], f'job-{i}', ['script.bash'])
        info = program.list_queue_jobs('gpu@@a')
        jobs = info.jobs
        from pprint import pprint
        pprint(jobs)
        assert len(jobs) == 10
        for job in jobs[:3]:
            assert job.state == 'r'
            assert job.queue == 'gpu@@a'
        for job in jobs[3:5]:
            assert job.state == 'qw'
            assert job.queue == 'gpu@@a'
        for job in jobs[5:]:
            assert job.state == '-'
            assert job.queue == 'gpu@@a'
        assert info.capacity.taken == 5
        assert info.capacity.limit == 5

def test_delete():
    with get_mock_backend() as backend:
        program = Program(backend)
        program.set_limit('gpu@@a', 5)
        for i in range(10):
            program.submit(['gpu@@a'], f'job-{i}', ['script.bash'])
        jobs = program.list_own_jobs().jobs
        assert len(jobs) == 10
        for job in jobs[:5]:
            assert job.state == 'r'
            assert isinstance(job.id, str)
        for job in jobs[5:]:
            assert job.state == '-'
            assert isinstance(job.id, str)
        assert backend.running_jobs() == { 'gpu@@a' : {f'job-{i}' for i in range(5)} }
        program.delete([jobs[1].id, jobs[4].id, jobs[7].id, jobs[8].id])
        assert backend.running_jobs() == { 'gpu@@a' : {'job-0', 'job-2', 'job-3'} }
        jobs = program.list_own_jobs().jobs
        assert len(jobs) == 6
        for i, job_no in enumerate((0, 2, 3)):
            job = jobs[i]
            assert job.state == 'r'
            assert job.name == f'job-{job_no}'
        for i, job_no in enumerate((5, 6, 9)):
            job = jobs[3 + i]
            assert job.state == '-'
            assert job.name == f'job-{job_no}'
