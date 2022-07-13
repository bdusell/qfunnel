# QFunnel

QFunnel is a tool for using CRC resources politely. It allows you to queue up a
large number of jobs while running only a limited number of them concurrently,
leaving resources available for other users. QFunnel can run in the background
and automatically submit new jobs whenever space becomes available, without
requiring you to submit new jobs manually.

## Installation

```sh
pip3 install --user git+https://github.com/bdusell/qfunnel
```

The `qfunnel` package provides the `qf` executable under `~/.local/bin`. Make
sure to add `~/.local/bin` to your `PATH`.

## Upgrading

```sh
pip3 uninstall qfunnel
pip3 install --user git+https://github.com/bdusell/qfunnel
```

## Usage

### Manage queue limits

QFunnel allows you to set a maximum number of jobs that it will run
concurrently on a certain queue. Different queues can have different limits.

For example, to set the limit on the queue `gpu@@nlp-gpu` to 10 slots, run:

```sh
qf limit 'gpu@@nlp-gpu' 10
```

Note that the exact form of the queue string matters; `*@@nlp-gpu` and
`gpu@@nlp-gpu` are treated as different queues.

All queues have no limit by default. You can unset a limit by running:

```sh
qf limit --delete 'gpu@@nlp-gpu'
```

You can print the current limit by running:

```sh
qf limit 'gpu@@nlp-gpu'
```

You can print all limits by running:

```sh
qf limit
```

### Submit/enqueue jobs

To submit jobs, use the command `qf submit` instead of `qsub`. When you submit
jobs through QFunnel, it respects the queue limits you set and will buffer jobs
locally if the limit for its queue has been reached. It can submit them later
when space becomes available.

You can submit new jobs using:

```sh
qf submit --queue 'gpu@@nlp-gpu' --name example-job -- -l gpu_card=1 example_job.bash
```

QFunnel only needs to know the queue and name of the job. Any arguments after
`--` will be passed directly to `qsub` when the job is actually submitted, but
you should not pass the `-q` (queue) or `-N` (name) options.

You can use the `--queue` flag multiple times to indicate that a job may be
scheduled on one of multiple queues. In this case, if the first queue is full,
QFunnel will attempt to schedule the job on the second queue, and so on, while
honoring each queue's limit. If all queues are full, the job will be buffered
locally.

```sh
qf submit --queue 'gpu@@nlp-gpu' --queue 'gpu@@csecri' --name example-job -- -l gpu_card=1 example_job.bash
```

There is also a a `--deferred` option that enqueues the job locally but does
not immediately attempt to submit it with `qsub`. This is convenient when
submitting a large number of jobs in a loop, as querying `qstat` and running
`qsub` repeatedly can be quite slow.

```sh
qf submit --queue 'gpu@@nlp-gpu' --name example-job --deferred -- -l gpu_card=1 example_job.bash
```

### List jobs

You can list all of your jobs, including those that have been buffered locally
and those that have been submitted with `qsub` and are visible through `qstat`,
using:

```sh
qf list
```

Locally buffered jobs show up with a status of `-`.

To show all jobs in a queue, including those of other users, you can run:

```sh
qf list 'gpu@@nlp-gpu'
```

### Submit locally buffered jobs

QFunnel needs to periodically poll `qstat` to figure out if there are open
slots to run jobs and, if so, submit them with `qsub`. You can run this check
at any time using:

```sh
qf check
```

### Run the job submission daemon

In order to make QFunnel submit locally buffered jobs automatically whenever
slots become available, you need to leave its daemon running in the background
using:

```sh
qf watch
```

This simply runs `qf check` at regular intervals. The default is to run it
every 5 minutes. You can also set the number of seconds in between checks to
something else, e.g. 30 seconds:

```sh
qf watch --seconds 30
```

To leave it running in the background, you can open a `screen` session on your
workstation, ssh into a CRC frontend, run `qf watch`, and detach from the
screen session by pressing "Ctrl+a" and then "d".

### Cancel jobs

You can cancel one or more jobs at once using:

```sh
qf delete 123 124 125 x12 x13
```

The arguments are IDs for jobs as shown by `qf list`. Note that locally
buffered jobs always have IDs that start with "x".

### Race conditions

QFunnel is designed so that it is safe to run `qf submit` while running `qf
watch` or `qf check` in the background, so you should not need to worry about
race conditions that cause, for example, the same job to be submitted more than
once. If you do notice a race condition, please file a bug report.

### Files

QFunnel stores queue limits and locally buffered jobs in the file
`~/.local/share/qfunnel.db`.
