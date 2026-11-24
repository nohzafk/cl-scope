#!/usr/bin/env bash
# Do the checks test the library? Each mutant is scope.lisp with one behaviour
# taken out, run against the same checks in a copy of this directory. A mutant
# is caught when a check fails or the run does not end within its time limit.
# One at a time: W3 finds its child process by name.
set -u
cd "$(dirname "$0")"
SBCL=${SBCL:-$(command -v sbcl || ls -d /nix/store/*-sbcl-*/bin/sbcl 2>/dev/null | sort | tail -1)}
[ -x "$SBCL" ] || { echo "no sbcl: set SBCL"; exit 1; }
LIMIT=${LIMIT:-60}
D=$(mktemp -d)
trap 'rm -rf "$D"' EXIT
survived=0

# mutant <what it takes out> <perl substitution over the whole file>
mutant() {
  cp scope.lisp test.lisp "$D/"
  perl -0pi -e "$2" "$D/scope.lisp"
  if cmp -s scope.lisp "$D/scope.lisp"; then
    echo "BROKEN   $1: the substitution matched nothing"; survived=1; return
  fi
  # SBCL handles SIGALRM itself, so the limit is a watcher that kills the run.
  (cd "$D" && exec "$SBCL" --noinform --non-interactive --no-sysinit --no-userinit \
    --load scope.lisp --eval '(require :sb-bsd-sockets)' --load test.lisp --eval '(sb-ext:exit :code (if (cl-scope/test:run-all) 0 1) :abort t)') > "$D/out" 2>&1 &
  run=$!
  (sleep "$LIMIT"; kill -9 $run 2>/dev/null) &
  watch=$!
  { wait $run; code=$?; } 2>/dev/null
  kill $watch 2>/dev/null; wait $watch 2>/dev/null
  failed=$(sed -n 's/^FAIL \([A-Z][0-9][0-9]*\).*/\1/p' "$D/out" | tr '\n' ' ')
  if [ $code -eq 0 ]; then echo "SURVIVED $1"; survived=1
  elif [ $code -eq 137 ]; then echo "caught   $1: the run did not end in $LIMIT s"
  else echo "caught   $1: ${failed:-exit $code}"; fi
}

mutant "a failed task does not cancel its scope" \
  's/\(when first \(cancel \(scope-token scope\)/(when nil (cancel (scope-token scope)/'
mutant "an error never parks" \
  's/\(when \(and \(eq \(scope-on-failure scope\) :park\) \(typep c \x27error\)\)/(when nil/'
mutant "a task spawned with :restarts nil offers RETRY all the same" \
  's/\(unless \(task-own-restarts task\)/(unless t/'
mutant "a task whose restart returned is marked running while it waits" \
  's/;; Here only when the restart returned or failed: the task is where it was.\n\s*\(set-state task :parked\)//'
mutant "SHIELDED shields nothing" \
  's/\(let \(\(\*token\* nil\)\) ,\@body\)/(progn ,\@body)/'
mutant "CANCEL calls no waker" \
  's/\(dolist \(w wakers\) \(ignore-errors \(funcall \(cdr w\)\)\)\)/(progn wakers)/'
mutant "a failing waker aborts remaining wakers and child cancellation" \
  's/\(ignore-errors \(funcall \(cdr w\)\)\)/(funcall (cdr w))/'
mutant "outer cancellation during JOIN is swallowed" \
  's/\(:outer-cancelled\n\s*\(check token\)\)/(:outer-cancelled (values-list values))/'
mutant "direct CANCEL-SCOPE during JOIN returns body values" \
  's/\(:cancelled\n\s*nil\)/(:cancelled (values-list values))/'
mutant "JOIN ignores stuck :parked tasks" \
  's/\(member \(task-state task\) \x27\(:running :parked\)\)/(eq (task-state task) :running)/'
mutant "*ON-PARK* is not inherited by spawned tasks" \
  's/\(adjoin \x27\*on-park\* \(adjoin \x27\*inherit\* \*inherit\*\)\)/(adjoin \x27*inherit* *inherit*)/'

[ $survived -eq 0 ] && echo "every mutant caught"
exit $survived
