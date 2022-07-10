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

def test_list():
    with get_mock_backend() as backend:
        program = Program(backend)
        program.set_limit('gpu@@a', 6)
        program.set_limit('gpu@@b', 0)
        for i in range(10):
            program.submit(['gpu@@a', 'gpu@@b'], f'job-{i}', ['script.bash'])
        jobs = list(program.list_own_jobs())
        assert len(jobs) == 10
        for job in jobs[:6]:
            assert job.state == 'r'
            assert job.queue == 'gpu@@a'
        for job in jobs[6:]:
            assert job.state == '-'
            assert job.queue == 'gpu@@a gpu@@b'
