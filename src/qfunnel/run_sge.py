import subprocess

from .util import get_env_vars

def run_sge_command(args, **kwargs):
    return subprocess.run(
        args,
        env={
            **get_env_vars([
                'PATH',
                'SGE_ROOT',
                'SGE_CELL',
                'SGE_QMASTER_PORT',
                'SGE_LOAD_AVG',
                'SGE_EXECD_PORT',
                'SGE_CLUSTER_NAME'
            ]),
            'SGE_LONG_QNAMES' : '-1',
            'SGE_LONG_JOB_NAMES' : '-1'
        },
        **kwargs
    )

def capture_sge_command_output(args):
    return run_sge_command(args, capture_output=True, encoding='ascii').stdout

def print_sge_command(args):
    run_sge_command(args)
