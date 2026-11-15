;;; Structured concurrency on SBCL's native threads.

(defsystem "cl-scope"
  :description "Scopes that join their tasks, cancellation where a task waits, and failed tasks that park with their stack."
  :version "0.1.0"
  :depends-on ((:require "sb-bsd-sockets"))
  :components ((:file "scope"))
  :in-order-to ((test-op (test-op "cl-scope/test"))))

(defsystem "cl-scope/test"
  :depends-on ("cl-scope")
  :components ((:file "test"))
  :perform (test-op (o c)
             (unless (uiop:symbol-call :cl-scope/test :run-all)
               (error "cl-scope: a check failed"))))
