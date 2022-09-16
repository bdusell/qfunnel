import argparse
import re

from qfunnel.format import format_box_table, format_date
from qfunnel.program import Program, JobFilter
from qfunnel.real_backend import RealBackend

def print_limit_table(limits):
    head = ['Queue', 'Limit']
    rows = [(queue, str(limit)) for queue, limit in limits]
    for line in format_box_table(head, rows):
        print(line)

def print_job_table(jobs, show_user):
    head = ['ID']
    if show_user:
        head.append('User')
    head.extend(['Name', 'State', 'Queue', 'Since'])
    rows = []
    for job in jobs:
        row = [job.id]
        if show_user:
            row.append(job.user)
        row.extend([
            job.name,
            job.state,
            job.queue,
            format_date(job.since) if job.since is not None else ''
        ])
        rows.append(row)
    for line in format_box_table(head, rows):
        print(line)

def print_capacity_table(limits):
    head = ['Queue', 'Taken', 'Limit', 'Available']
    rows = []
    for queue, capacity in limits:
        rows.append((
            queue,
            str(capacity.taken),
            str(capacity.limit) if capacity.limit is not None else '',
            str(max(0, capacity.limit - capacity.taken)) if capacity.limit is not None else ''
        ))
    for line in format_box_table(head, rows):
        print(line)

def add_job_filter_args(parser):
    parser.add_argument('--name',
        help='Only select jobs whose names match the given regular '
             'expression.')

def get_job_filter(args):
    return JobFilter(name=args.name)

def main():

    parser = argparse.ArgumentParser(
        description=
        'A tool for limiting the number of CRC jobs you submit to certain '
        'queues.'
    )
    subparsers = parser.add_subparsers(dest='command', required=True,
        help='The sub-command to run.')

    limit_parser = subparsers.add_parser('limit',
        help='Show, set, or delete limits on the number of jobs submitted to '
             'each queue. Different queues can have different limits. With no '
             'arguments, show the limits for all queues.')
    limit_parser.add_argument('queue', nargs='?',
        help='A queue specifier as given to `qsub -q`.')
    limit_parser.add_argument('limit', type=int, nargs='?',
        help='If given, set the limit for this queue to this value.')
    limit_parser.add_argument('--delete', action='store_true', default=False,
        help='Rather than showing or setting the limit for this queue, delete '
             'it, making it unlimited.')

    submit_parser = subparsers.add_parser('submit',
        help='Submit a new job to a queue or series of queues. If the number '
             'of submitted jobs in the queue has not reached its limit, the '
             'job will be submitted immediately with `qsub`. Otherwise, it '
             'will be buffered locally so it can be submitted later when there '
             'is room in the queue.')
    submit_parser.add_argument('--queue', required=True, action='append',
        help='The queue to which the job will be submitted. This option can '
             'be given multiple times to specify a series of fallback queues '
             '(order matters), in which case the job will be submitted to the '
             'second queue if the first is full, the third if the first and '
             'second are full, and so on. The job will be buffered locally if '
             'all queues are full.')
    submit_parser.add_argument('--name', required=True,
        help='The name of the job, corresponding to the `qsub -N` option. '
             'This will be shown by `list`.')
    submit_parser.add_argument('--deferred', action='store_true', default=False,
        help='Even if there is room in the queue, do not submit the job with '
             '`qsub`; just buffer it locally. This is much faster than the '
             'alternative, making it very convenient when submitting many '
             'jobs in a loop.')
    submit_parser.add_argument('args', nargs=argparse.REMAINDER,
        help='Arguments that will be passed directly to the `qsub` command. '
             'If you need to pass any options beginning with `-` to `qsub`, '
             'use `--` as the first argument. Do not use the `-q` or `-N` '
             'options.')

    list_parser = subparsers.add_parser('list',
        help='List the status of all of your running, pending, and locally '
             'buffered jobs.')
    list_parser.add_argument('queue', nargs='?',
        help='If given, only list jobs in this queue, including other users\' '
             'jobs.')
    add_job_filter_args(list_parser)

    check_parser = subparsers.add_parser('check',
        help='Check if there are any locally buffered jobs that can be '
             'submitted, and if so, submit them.')

    watch_parser = subparsers.add_parser('watch',
        help='Enter a loop that runs `check` at regular intervals.')
    watch_parser.add_argument('--seconds', type=float, default=600.0,
        help='The number of seconds to wait in between checks. The default is '
             '10 minutes.')

    delete_parser = subparsers.add_parser('delete',
        help='Delete running, pending, or locally buffered jobs. Running and '
             'pending jobs are canceled with `qdel`. Locally buffered jobs are '
             'simply deleted.')
    delete_parser.add_argument('id', nargs='*',
        help='The IDs of the jobs to delete as shown by `list`.')

    args = parser.parse_args()

    program = Program(RealBackend())

    if args.command == 'limit':
        if args.delete:
            if args.queue is None:
                parser.error('missing queue name')
            if args.limit is not None:
                parser.error('cannot use --delete and set a limit at the same time')
            program.delete_limit(args.queue)
        else:
            if args.limit is not None:
                program.set_limit(args.queue, args.limit)
            else:
                if args.queue is not None:
                    limit = program.get_limit(args.queue)
                    if limit is None:
                        print('no limit')
                    else:
                        print(limit)
                else:
                    print_limit_table(program.get_all_limits())
    elif args.command == 'submit':
        command_args = args.args
        if command_args and command_args[0] == '--':
            command_args = command_args[1:]
        program.submit(args.queue, args.name, command_args, args.deferred)
    elif args.command == 'list':
        if args.queue is not None:
            info = program.list_queue_jobs(args.queue, get_job_filter(args))
            print_job_table(info.jobs, show_user=True)
            print()
            print_capacity_table([(args.queue, info.capacity)])
        else:
            info = program.list_own_jobs(get_job_filter(args))
            print_job_table(info.jobs, show_user=False)
            print()
            print_capacity_table(info.queues)
    elif args.command == 'check':
        program.check()
    elif args.command == 'watch':
        try:
            program.watch(args.seconds)
        except KeyboardInterrupt:
            print()
    elif args.command == 'delete':
        program.delete(args.id)
    else:
        raise ValueError

if __name__ == '__main__':
    main()
