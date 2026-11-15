# cl-scope

Structured concurrency on SBCL's native threads. A scope starts tasks, each in a thread, and does not end while one
of them lives. A task is cancelled where it waits. A task that fails either cancels its scope or parks with its
stack, and another thread answers it with a restart.

SBCL only: it uses `sb-thread`, `sb-ext` and `sb-bsd-sockets`, and nothing outside SBCL.

```lisp
(sc:with-scope (s :timeout 30)
  (sc:spawn (lambda () (sc:run "/bin/sh" '("-c" "make test"))))
  (sc:spawn (lambda () (sc:read-line-from socket stream))))

(sc:gather (list (lambda () (fetch a)) (lambda () (fetch b))))   ; => the two results, in order
```

The package is `cl-scope`, nickname `sc`.

## Loading

```lisp
(asdf:load-asd "/path/to/cl-scope/cl-scope.asd")
(asdf:load-system "cl-scope")
```

Or put the directory in `CL_SOURCE_REGISTRY` (`/path/to/cl-scope//:`) and load the system by name.

## Checks

```sh
./check.sh        # 25 checks through ASDF, about 6 s; -v prints what each measured
./mutants.sh      # scope.lisp with one behaviour taken out, seven times; about 2 min
```

`(asdf:test-system "cl-scope")` runs the same checks. Both scripts take the sbcl on PATH, or `$SBCL`.

| Group | Checks | What they show |
|---|---|---|
| S, scopes | 9 | join, sibling cancel, body error, nested scopes inner-first, time limit, closed scope, ordered results, 200 tasks |
| W, waits | 7 | each kind of wait ends at a cancellation; interrupt and `stuck` for the ones that do not |
| P, park | 8 | a failed task parks with its stack and is answered from another thread |
| | 1 | no check left a thread behind |

| Mutant | Caught by |
|---|---|
| a failed task does not cancel its scope | S2 S4 S5 P4 |
| an error never parks | P1 P2 P3 P5 P6 P7 P8 |
| a task spawned with `:restarts nil` offers `retry` all the same | P8 |
| a task whose restart returned is marked running while it waits | P6 P8 |
| `shielded` shields nothing | W7 |
| a cancelled `run` signals only the process, not its group | W3 |
| `cancel` calls no waker | the run does not end in 60 s |

## Scopes

| | |
|---|---|
| `(with-scope (var &key name on-failure timeout grace) body...)` | Evaluate the body with a new scope; return when every task spawned in it has finished. |
| `(spawn fn &key name scope restarts)` | Start `fn` as a task of the current scope. Returns the task. `:restarts nil`: the task offers no `retry`, `give-up` or `finish-with` of its own. |
| `(wait-on lock queue test &key timeout)` | Wait on the waitqueue until `test`, called with the mutex held, gives a value; a cancellation ends the wait. |
| `(gather thunks &key name on-failure timeout)` | One scope, one task per thunk; their first values in order. |
| `(cancel-scope scope &optional reason)` | Cancel the scope and the scopes inside it. |

- **Join.** A scope whose body has returned waits for its tasks. A scope that has ended takes no new task.
- **Failure.** With `:on-failure :cancel`, the default, the first task that fails cancels the others and the scope
  signals `task-error`, which holds the task, its condition and its stack (`task-report`).
- **Body.** A body that leaves by an error cancels the tasks and waits for them; the body's error is what leaves.
- **Time limit.** `:timeout` seconds cancels the scope and signals `timed-out`.
- **Special variables.** A task starts with the values of the variables named in `*inherit*`, and no other
  binding of the thread that spawned it.

## Cancellation

A cancellation is not delivered to a thread. It ends the wait the thread is in, and the wait signals `cancelled`.

| Wait | |
|---|---|
| `(pause seconds)` | sleep |
| `(recv chan &key timeout)`, `(send chan x)`, `(make-chan)` | an unbounded channel |
| `(run program args)` | a child process: output and exit code. A cancellation terminates it and its process group. |
| `(read-line-from socket stream)` | a line from a socket. A cancellation shuts the socket down. |
| `(call-with-waker waker thunk)` | any blocking call, given a function that ends it from another thread |
| `(check)` | no wait: signals `cancelled` if the scope is cancelled |

- **`cancelled` is a `condition`, not an `error`.** `ignore-errors` and a `handler-case` for `error` in a task's
  code do not swallow it.
- **`(shielded body...)`** runs cleanup that must finish outside every scope's cancellation.
- **Last resort.** A task that reaches no wait within `:grace` seconds (default 1) of a cancellation is
  interrupted. One that cannot be interrupted keeps its scope open, and the scope signals `stuck` every grace
  period until it ends.

## Parked tasks

With `:on-failure :park`, a task whose code signals an `error` stops where the error was signalled. Its thread, its
stack and its dynamic bindings stay as they are, and the scope's other tasks go on.

| | |
|---|---|
| `(parked scope)` | the scope's parked tasks |
| `(task-condition task)`, `(task-report task)`, `(task-restarts task)` | what was signalled, the stack, the restart names on offer |
| `(answer task restart &rest args)` | have the task invoke the restart, in its own thread |
| `(ask task thunk &key timeout)` | call `thunk` in the task's thread, where it stopped; its values |
| `*on-park*` | called with the task, in its thread, when it parks |
| `(task-note task)` | why the last answer did nothing: an unknown restart, or wrong arguments |

Every task offers `retry` (run the task again from its start), `give-up` (end it as failed) and `finish-with`
(end it with the given values), beside the restarts its own code establishes. A function redefined while a task
is parked is the one `retry` runs.

A parked task holds its thread until it is answered or its scope is cancelled.

## Limits

- **A wait for a lock** is ended only by the interrupt.
- **The interrupt is asynchronous.** It can land in a cleanup form and cut it short. Nothing here measures how
  often that corrupts state.
- **Tested to 200 threads.**
- **A blocking call in foreign code** needs a waker of its own.

## Related work

Read from their READMEs; neither was run here.

- **[cl-cancel](https://github.com/atgreen/cl-cancel)** has hierarchical cancellation, deadlines, and closing a
  stream to end a blocked read. It covers what the token and the wakers do here. It has no scope: nothing joins
  the tasks, and a failure cancels no sibling.
- **bordeaux-threads** is a portability layer over what `sb-thread` gives. Porting this library to another
  implementation would start there.
- Continuation-passing libraries (`cl-cont`, `green-threads`, `blackbird`) turn a task into a chain of closures.
  A parked task here is a real stack, which is what a backtrace, a restart and `ask` need.
