import argparse

from qfunnel.format import format_box_table, format_date
from qfunnel.program import Program
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
        row.extend([job.name, job.state, job.queue, format_date(job.since)])
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

def main():

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command', required=True)

    limit_parser = subparsers.add_parser('limit')
    limit_parser.add_argument('--delete', action='store_true', default=False)
    limit_parser.add_argument('queue', nargs='?')
    limit_parser.add_argument('limit', type=int, nargs='?')

    submit_parser = subparsers.add_parser('submit')
    submit_parser.add_argument('--queue', required=True, action='append')
    submit_parser.add_argument('--name', required=True)
    submit_parser.add_argument('args', nargs=argparse.REMAINDER)

    list_parser = subparsers.add_parser('list')
    list_parser.add_argument('queue', nargs='?')

    check_parser = subparsers.add_parser('check')

    watch_parser = subparsers.add_parser('watch')
    watch_parser.add_argument('--seconds', type=float, default=float(60 * 5))

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
        program.submit(args.queue, args.name, command_args)
    elif args.command == 'list':
        if args.queue is not None:
            info = program.list_queue_jobs(args.queue)
            print_job_table(info.jobs, show_user=True)
            print()
            print_capacity_table([(args.queue, info.capacity)])
        else:
            info = program.list_own_jobs()
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
    else:
        raise ValueError

if __name__ == '__main__':
    main()
