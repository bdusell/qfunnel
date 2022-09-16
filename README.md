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

## Setting up the daemon

QFunnel needs to have a process running in the background that periodically
checks the number of jobs submitted to the queues and submits locally buffered
jobs when space is available. To do this, log in to a CRC frontend machine and
open a new tmux window.

```sh
tmux
```

In the tmux window, start the daemon by running
```sh
qf watch
```

You must follow the following steps in order to prevent QFunnel from losing
permission to access the AFS file system after you log out of your ssh session,
as described in
[this post](https://unix.stackexchange.com/questions/301530/why-do-i-get-permission-denied-error-when-i-log-out-of-the-ssh-session).
First, create a new window in the same tmux session by pressing Ctrl+b then c.
Then, in that new window, run
```sh
kinit && aklog
```
Enter your password when prompted. Finally, detach from the tmux session by
pressing Ctrl+b then d. QFunnel will now continue to run in the background,
even after you log out of your ssh session.

## Upgrading

* Reattach to the tmux window with `tmux attach` and stop the `qf watch`
  daemon with Ctrl+c.
* Run
  ```sh
  pip3 uninstall qfunnel
  pip3 install --user git+https://github.com/bdusell/qfunnel
  ```
* Restart the daemon.

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
a job through QFunnel, QFunnel respects the queue limits you set and will
buffer a job locally if the limit for its queue has been reached. It can submit
it later when space becomes available.

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

There is also a `--deferred` option that enqueues the job locally but does
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

In both cases, you can filter jobs by name with a regular expression using
`--name`.

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
every 10 minutes. You can also set the number of seconds in between checks to
something else, e.g. 30 seconds:

```sh
qf watch --seconds 30
```

See the section on setting up the daemon above.

### Cancel jobs

You can cancel one or more jobs at once using:

```sh
qf delete 123 124 125 x12 x13
```

The arguments are IDs for jobs as shown by `qf list`. Note that locally
buffered jobs always have IDs that start with "x".

### Reorder locally buffered jobs

You can change the order of locally buffered jobs, before they are submitted,
using `qf bump`, which moves selected jobs to the front of the queue of locally
buffered jobs. You can select jobs by name with a regular expression. The
regular expression matches anyhwere in the string.

```sh
qf bump --name 'foobar-\d+'
```

### Race conditions

QFunnel is designed so that it is safe to run `qf submit` while running `qf
watch` or `qf check` in the background, so you should not need to worry about
race conditions that cause, for example, the same job to be submitted more than
once, or for more jobs to be submitted than there are open slots. If you do
notice a race condition, please file a bug report.

### Files

QFunnel stores queue limits and locally buffered jobs in the file
`~/.local/share/qfunnel.db`.
