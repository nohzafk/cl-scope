#!/usr/bin/env bash
# The checks, with the system loaded through ASDF. -v prints the measured
# detail of every check. SBCL names the sbcl to run when none is on PATH.
set -u
cd "$(dirname "$0")"
SBCL=${SBCL:-$(command -v sbcl || ls -d /nix/store/*-sbcl-*/bin/sbcl 2>/dev/null | sort | tail -1)}
[ -x "$SBCL" ] || { echo "no sbcl: set SBCL"; exit 1; }
exec "$SBCL" --noinform --non-interactive --no-sysinit --no-userinit \
  --eval '(require :asdf)' \
  --eval '(asdf:load-asd (merge-pathnames "cl-scope.asd" (uiop:getcwd)))' \
  --eval '(asdf:load-system "cl-scope/test")' \
  --eval '(sb-ext:exit :code (if (cl-scope/test:run-all) 0 1) :abort t)' "$@"
