;;; The checks of cl-scope. (cl-scope/test:run-all) runs them and is true when all pass.
;;;   S  a scope and its tasks: join, failure, timeout, nesting
;;;   W  which waits a cancellation ends, and how fast
;;;   P  a failed task parks and is answered from another thread

(defpackage #:cl-scope/test
  (:use #:cl #:cl-scope)
  (:export #:run-all))
(in-package #:cl-scope/test)
(declaim (optimize (debug 2)))

(defvar *pass* 0)
(defvar *fail* 0)
(defvar *tests* '())
(defvar *verbose* (member "-v" sb-ext:*posix-argv* :test #'string=))

(defun ok (name pass &optional detail)
  (if pass (incf *pass*) (incf *fail*))
  (format t "~a ~a~%" (if pass "ok  " "FAIL") name)
  (when (and detail (or (not pass) *verbose*)) (format t "       ~a~%" detail))
  (finish-output))

(defun info (control &rest args)
  (format t "     ~?~%" control args)
  (finish-output))

(defmacro ms (&body body)
  "Evaluate BODY; the milliseconds it took."
  `(let ((t0 (get-internal-real-time)))
     ,@body
     (round (* 1000 (- (get-internal-real-time) t0)) internal-time-units-per-second)))

(defmacro caught (&body body)
  "The condition BODY ends with, or nil."
  `(handler-case (progn ,@body nil)
     ((or serious-condition cancelled) (c) c)))

(defun threads () (length (sb-thread:list-all-threads)))

(defmacro deftest (name &body body)
  `(setf *tests* (cons (cons ,name (lambda () ,@body))
                       (remove ,name *tests* :key #'car :test #'string=))))

(defun run-tests ()
  (let ((leaks '()))
    (dolist (test (reverse *tests*))
      (let ((before (threads)))
        (handler-case (funcall (cdr test))
          ((or serious-condition cancelled) (c)
            (ok (format nil "~a: the check itself failed" (car test)) nil (princ-to-string c))))
        (loop repeat 200 while (> (threads) before) do (sleep 0.01))
        (when (> (threads) before)
          (push (format nil "~a: ~d threads before, ~d after" (car test) before (threads)) leaks))))
    (ok "no check left a thread behind" (null leaks) (format nil "~{~a~^; ~}" (reverse leaks)))))

(declaim (notinline explode))
(defun explode (n) (error "boom ~a" n))

;;; ---- S: a scope and its tasks ----

(deftest "S1"
  (let* ((a nil) (b nil)
         (took (ms (with-scope (s)
                     (setf a (spawn (lambda () (pause 0.2) :a))
                           b (spawn (lambda () (pause 0.3) (values :b 2))))))))
    (ok "S1 a scope whose body returns at once ends when its last task does"
        (and (<= 290 took 600) (equal (task-values a) '(:a)) (equal (task-values b) '(:b 2))
             (eq (task-state a) :done))
        (format nil "~d ms, ~s ~s" took (task-values a) (task-values b)))))

(deftest "S2"
  (let* ((cleaned nil) (sibling nil) (c nil)
         (took (ms (setf c (caught
                             (with-scope (s)
                               (setf sibling (spawn (lambda () (unwind-protect (pause 5) (setf cleaned t)))
                                                    :name "sleeper"))
                               (spawn (lambda () (pause 0.1) (explode 7)) :name "bad"))))))
         (task (and (typep c 'task-error) (task-error-task c))))
    (ok "S2 a task that fails cancels its sibling, which unwinds; the scope signals TASK-ERROR with the task's stack"
        (and task (search "boom 7" (princ-to-string c)) (equal (task-name task) "bad")
             cleaned (eq (task-state sibling) :cancelled) (< took 600)
             (some (lambda (l) (search "EXPLODE" l)) (task-report task)))
        (format nil "~d ms, ~a, sibling ~a, stack ~s" took c (and sibling (task-state sibling))
                (and task (task-report task))))))

(deftest "S3"
  (let* ((cleaned nil) (c nil)
         (took (ms (setf c (caught
                             (with-scope (s)
                               (spawn (lambda () (unwind-protect (pause 5) (setf cleaned t))))
                               (pause 0.05)
                               (error "body failed")))))))
    (ok "S3 an error in the body cancels the tasks and waits for them; the body's error is what leaves"
        (and (typep c 'simple-error) (search "body failed" (princ-to-string c)) cleaned (< took 500))
        (format nil "~d ms, ~a, cleaned ~a" took c cleaned))))

(deftest "S4"
  (let* ((c nil)
         (took (ms (setf c (caught
                             (with-scope (s)
                               (spawn (lambda () (pause 0.1) (explode 4)) :name "bad")
                               (pause 5)
                               :not-reached))))))
    (ok "S4 a body that waits is woken when a task fails, and the scope signals TASK-ERROR"
        (and (typep c 'task-error) (< took 600))
        (format nil "~d ms, ~a" took c))))

(deftest "S5"
  (let ((order '()) (lock (sb-thread:make-mutex)) (c nil) (took 0))
    (flet ((note (x) (sb-thread:with-mutex (lock) (push x order))))
      (setf took
            (ms (setf c (caught
                          (with-scope (outer)
                            (spawn (lambda ()
                                     (unwind-protect
                                          (with-scope (inner)
                                            (spawn (lambda () (unwind-protect (pause 5) (note :grandchild))))
                                            (spawn (lambda () (unwind-protect (pause 5) (note :grandchild))))
                                            (pause 5))
                                       (note :child)))
                                   :name "parent-task")
                            (spawn (lambda () (pause 0.1) (explode 5)) :name "bad")))))))
    (ok "S5 cancelling a scope cancels the scopes inside it; the inner tasks finish before their parent"
        (and (typep c 'task-error) (equal (reverse order) '(:grandchild :grandchild :child)) (< took 600))
        (format nil "~d ms, ~a, order ~s" took c (reverse order)))))

(deftest "S6"
  (let* ((c nil)
         (took (ms (setf c (caught (with-scope (s :timeout 0.3)
                                     (spawn (lambda () (pause 5)))
                                     (spawn (lambda () (pause 5))))))))
         (r nil)
         (quick (ms (setf r (with-scope (s :timeout 5) (spawn (lambda () (pause 0.1))) :returned)))))
    (ok "S6 a time limit cancels the scope and signals TIMED-OUT; a scope that finishes first does not wait for it"
        (and (typep c 'timed-out) (<= 290 took 700) (eq r :returned) (< quick 400))
        (format nil "limit 300 ms: ~d ms, ~a; finished early: ~d ms, ~s" took c quick r))))

(deftest "S7"
  (let* ((saved nil)
         (c (progn (with-scope (s) (setf saved s))
                   (caught (spawn (lambda () 1) :scope saved))))
         (d (caught (let ((sc::*scope* nil)) (spawn (lambda () 1))))))
    (ok "S7 a scope that has ended takes no new task; SPAWN outside a scope is an error"
        (and (typep c 'error) (search "has ended" (princ-to-string c)) (typep d 'error))
        (format nil "~a | ~a" c d))))

(defvar *who* :global)

(deftest "S8"
  (let* ((r (gather (loop for i from 1 to 6 collect (let ((i i)) (lambda () (pause (* 0.02 (- 7 i))) (* i i))))))
         (w (let ((*who* :parent) (*inherit* '(*who*)))
              (gather (list (lambda () *who*) (lambda () (first (gather (list (lambda () *who*))))))))))
    (ok "S8 GATHER gives the results in order; a task starts with the special variables named in *INHERIT*"
        (and (equal r '(1 4 9 16 25 36)) (equal w '(:parent :parent)))
        (format nil "~s ~s" r w))))

(deftest "S9"
  (let* ((n 200) (r nil) (c nil)
         (all (ms (setf r (gather (loop repeat n collect (lambda () (pause 0.1) 1))))))
         (cut (ms (setf c (caught (with-scope (s :timeout 0.1)
                                    (dotimes (i n) (spawn (lambda () (pause 5))))))))))
    (info "~d tasks that each wait 100 ms: ~d ms in all; ~d tasks cancelled at a 100 ms limit: ~d ms in all" n all n cut)
    (ok "S9 200 tasks at once: all join, and all are cancelled within 300 ms of the limit"
        (and (= (reduce #'+ r) n) (< all 1000) (typep c 'timed-out) (< cut 400))
        (format nil "~d ms, ~d ms" all cut))))

(deftest "S10"
  (let* ((after-gather :not-reached)
         (after-inner :not-reached)
         (ran-when-cancelled nil)
         (c1 (caught
               (with-scope (outer :timeout 0.1)
                 (let ((r (gather (list (lambda () (pause 5) :a) (lambda () (pause 5) :b)))))
                   (setf after-gather r)))))
         (c2 (caught
               (with-scope (outer :timeout 0.1)
                 (let ((r (with-scope (inner)
                            (spawn (lambda () (pause 5)))
                            :body-done)))
                   (setf after-inner r)))))
         (c3 (caught
               (with-scope (outer)
                 (cancel-scope outer :stop)
                 (with-scope (inner)
                   (setf ran-when-cancelled t))))))
    (ok "S10 an outer cancellation during JOIN (or before entry) signals CANCELLED instead of returning"
        (and (typep c1 'timed-out) (eq after-gather :not-reached)
             (typep c2 'timed-out) (eq after-inner :not-reached)
             (null c3) (null ran-when-cancelled))
        (format nil "gather ~a/~s, inner ~a/~s, pre-cancelled ~a/~a"
                c1 after-gather c2 after-inner c3 ran-when-cancelled))))

(deftest "S11"
  (let* ((sleeper nil)
         (r1 (with-scope (s)
               (setf sleeper (spawn (lambda () (pause 5))))
               (spawn (lambda () (pause 0.05) (cancel-scope s)))
               :body-value))
         (r2 (with-scope (s)
               (spawn (lambda () (pause 0.05) (cancel-scope s :timeout)))
               :body-value)))
    (ok "S11 a direct CANCEL-SCOPE during JOIN returns NIL (and never signals TIMED-OUT without :timeout)"
        (and (null r1) (eq (task-state sleeper) :cancelled) (null r2))
        (format nil "r1 ~s (~a), r2 ~s" r1 (and sleeper (task-state sleeper)) r2))))

(deftest "S12"
  (let* ((cleaned nil) (c nil)
         (took (ms (setf c (caught
                             (with-scope (s :grace 0.2)
                               (spawn (lambda () (unwind-protect (pause 5) (setf cleaned t))))
                               (spawn (lambda () :never) :name 123)))))))
    (ok "S12 a failed SPAWN leaves no phantom live task behind and does not wedge JOIN"
        (and (typep c 'error) cleaned (< took 500))
        (format nil "~d ms, ~a, cleaned ~a" took c cleaned))))

;;; ---- W: which waits a cancellation ends ----

(defun cancelled-after (seconds thunk &key (grace 1))
  "Run THUNK as the one task of a scope with a time limit; the milliseconds past the limit, and the task."
  (let* ((task nil)
         (took (ms (caught (with-scope (s :timeout seconds :grace grace)
                             (setf task (spawn thunk)))))))
    (values (- took (round (* 1000 seconds))) task)))

(defun no-process-p (pattern)
  (/= 0 (nth-value 1 (shielded (run "pgrep" (list "-f" pattern) :search t)))))

(deftest "W1"
  (multiple-value-bind (late task) (cancelled-after 0.2 (lambda () (pause 30)))
    (let ((chan (make-chan)))
      (multiple-value-bind (late2 task2) (cancelled-after 0.2 (lambda () (recv chan)))
        (info "pause: ~d ms after the cancellation; recv on an empty channel: ~d ms" late late2)
        (ok "W1 a cancellation ends a pause and a wait on a channel"
            (and (< late 100) (< late2 100) (eq (task-state task) :cancelled) (eq (task-state task2) :cancelled))
            (format nil "~d ms, ~d ms" late late2))))))

(deftest "W2"
  (multiple-value-bind (out code) (run "/bin/echo" '("hi"))
    (let ((quick (ms (run "/bin/echo" '("hi")))))
      (multiple-value-bind (late task) (cancelled-after 0.3 (lambda () (run "/bin/sleep" '("30.123"))))
        (info "/bin/echo runs in ~d ms; a sleeping child process ended ~d ms after the cancellation" quick late)
        (ok "W2 a cancellation ends a wait for a child process, and the process is gone"
            (and (equal out (format nil "hi~%")) (eql code 0) (< late 300)
                 (eq (task-state task) :cancelled) (no-process-p "sleep 30.123"))
            (format nil "~s ~s, ~d ms, ~a, gone ~a" out code late (task-state task) (no-process-p "sleep 30.123")))))))

(deftest "W3"
  (multiple-value-bind (late task)
      (cancelled-after 0.3 (lambda () (run "/bin/sh" '("-c" "sleep 31.456; echo done"))))
    (info "a shell with a sleeping child of its own ended ~d ms after the cancellation" late)
    (ok "W3 the processes a child process started are gone too"
        (and (< late 300) (eq (task-state task) :cancelled) (no-process-p "sleep 31.456"))
        (format nil "~d ms, ~a, grandchild gone ~a" late (task-state task) (no-process-p "sleep 31.456")))))

(deftest "W4"
  (let* ((server (make-instance 'sb-bsd-sockets:inet-socket :type :stream :protocol :tcp))
         (port (progn (sb-bsd-sockets:socket-bind server #(127 0 0 1) 0)
                      (sb-bsd-sockets:socket-listen server 5)
                      (nth-value 1 (sb-bsd-sockets:socket-name server))))
         (lines '()) (task nil) (accepted nil)
         (took (ms (caught
                     (with-scope (s :timeout 0.4)
                       (setf task
                             (spawn (lambda ()
                                      (let ((sock (make-instance 'sb-bsd-sockets:inet-socket :type :stream :protocol :tcp)))
                                        (unwind-protect
                                             (progn
                                               (sb-bsd-sockets:socket-connect sock #(127 0 0 1) port)
                                               (let ((stream (sb-bsd-sockets:socket-make-stream sock :input t :output t)))
                                                 (loop (push (read-line-from sock stream) lines))))
                                          (sb-bsd-sockets:socket-close sock))))))
                       (setf accepted (sb-bsd-sockets:socket-accept server))
                       (let ((stream (sb-bsd-sockets:socket-make-stream accepted :input t :output t)))
                         (write-line "first line" stream)
                         (finish-output stream)))))))
    (when accepted (sb-bsd-sockets:socket-close accepted))
    (sb-bsd-sockets:socket-close server)
    (info "a read on a socket that gets no more data ended ~d ms after the cancellation" (- took 400))
    (ok "W4 a cancellation ends a read on a socket; what arrived before it was read"
        (and (equal lines '("first line")) (< (- took 400) 200) (eq (task-state task) :cancelled))
        (format nil "~d ms past the limit, lines ~s, ~a" (- took 400) lines (and task (task-state task))))))

(deftest "W5"
  (multiple-value-bind (late task) (cancelled-after 0.2 (lambda () (let ((n 0)) (loop (incf n)))) :grace 0.3)
    (multiple-value-bind (late2 task2) (cancelled-after 0.2 (lambda () (sleep 30)) :grace 0.3)
      (info "grace 300 ms: a loop that never waits ended ~d ms after the cancellation, a plain SLEEP ~d ms" late late2)
      (ok "W5 a task that reaches no wait of ours is interrupted one grace period after the cancellation"
          (and (<= 280 late 700) (<= 280 late2 700) (eq (task-state task) :cancelled) (eq (task-state task2) :cancelled))
          (format nil "~d ms ~a, ~d ms ~a" late (task-state task) late2 (task-state task2))))))

(defvar *release* nil)

(deftest "W6"
  (setf *release* nil)
  (let* ((stucks 0) (c nil) (task nil)
         (took (ms (setf c (caught
                             (handler-bind ((stuck (lambda (s)
                                                     (declare (ignore s))
                                                     (when (= (incf stucks) 2) (setf *release* t)))))
                               (with-scope (s :timeout 0.1 :grace 0.2)
                                 (setf task (spawn (lambda () (sb-sys:without-interrupts (loop until *release*)))
                                                   :name "deaf")))))))))
    (ok "W6 a task that cannot be interrupted keeps its scope open; the scope signals STUCK each grace period until it ends"
        (and (typep c 'timed-out) (= stucks 2) (<= 650 took 1200))
        (format nil "~d ms, ~a, STUCK ~d times, task ~a" took c stucks (and task (task-state task))))))

(deftest "W7"
  (let* ((trace '())
         (took (ms (caught
                     (with-scope (s :timeout 0.1)
                       (spawn (lambda ()
                                (unwind-protect (pause 5)
                                  (push (handler-case (progn (pause 0.1) :finished) (cancelled () :cut)) trace)
                                  (shielded (pause 0.1) (push :shielded-finished trace))))))))))
    (ok "W7 a wait in a cleanup form is cancelled too, unless it is SHIELDED; the scope waits for the shielded one"
        (and (equal (reverse trace) '(:cut :shielded-finished)) (<= 190 took 500))
        (format nil "~d ms, ~s" took (reverse trace)))))

(deftest "W8"
  (let* ((woken nil) (child-cancelled nil) (ready (make-chan)) (c nil)
         (took (ms (setf c (caught
                             (with-scope (outer :timeout 0.15 :grace 2)
                               (spawn (lambda ()
                                        (with-scope (inner)
                                          (spawn (lambda ()
                                                   (unwind-protect (pause 2)
                                                     (setf child-cancelled t))))
                                          (send ready :child)
                                          (pause 2))))
                               (spawn (lambda ()
                                        (let ((ch (make-chan)))
                                          (call-with-waker
                                           (lambda () (setf woken t) (send ch :ok))
                                           (lambda ()
                                             (send ready :waker)
                                             (recv ch :timeout 2))))))
                               (recv ready)
                               (recv ready)
                               ;; Registered last so CANCEL invokes this failing waker first.
                               (call-with-waker
                                (lambda () (error "broken waker"))
                                (lambda () (pause 2)))))))))
    (ok "W8 a waker that signals an error does not stop other wakers or child scopes from cancelling"
        (and (typep c 'timed-out) woken child-cancelled (< took 600))
        (format nil "~d ms, ~a, woken ~a, child ~a" took c woken child-cancelled))))

;;; ---- P: a failed task parks ----

(defvar *parks* (make-chan))
(setf *on-park* (lambda (task) (send *parks* task)))

(defun operator (fn &optional (n 1))
  "A thread outside every scope that calls FN with each of the next N tasks that park."
  (setf *parks* (make-chan))
  (sb-thread:make-thread
   (lambda ()
     (dotimes (i n)
       (multiple-value-bind (task got) (recv *parks* :timeout 5)
         (when got (funcall fn task)))))
   :name "operator"))

(defvar *calls* (make-hash-table :synchronized t))

(declaim (notinline fetch))
(defun fetch (i)
  "A stand-in for a tool call: item 3 does not exist."
  (restart-case
      (progn
        (incf (gethash i *calls* 0))
        (if (= i 3) (error "no such item ~a" i) (* i 10)))
    (use-value (v) :report "Use a value as the result." v)
    (retry-with (j) :report "Fetch another item." (fetch j))))

(deftest "P1"
  (clrhash *calls*)
  (let* ((seen nil)
         (op (operator (lambda (task)
                         (pause 0.3)
                         (setf seen (list (task-state task) (task-restarts task)
                                          (mapcar #'task-state (remove task (scope-tasks (task-scope task))))
                                          (task-report task)))
                         (answer task 'retry-with 6))))
         (r (gather (loop for i from 1 to 5 collect (let ((i i)) (lambda () (pause 0.05) (fetch i))))
                    :on-failure :park)))
    (sb-thread:join-thread op)
    (ok "P1 one of five tasks fails and parks; the other four finish; RETRY-WITH from another thread completes the group"
        (and (equal r '(10 20 60 40 50))
             (eq (first seen) :parked)
             (subsetp '(use-value retry-with retry give-up finish-with) (second seen))
             (equal (third seen) '(:done :done :done :done))
             (some (lambda (l) (search "FETCH" l)) (fourth seen))
             (loop for i in '(1 2 3 4 5 6) always (= 1 (gethash i *calls* 0))))
        (format nil "~s; seen while parked: ~s; calls ~s" r seen
                (loop for i from 1 to 6 collect (gethash i *calls* 0))))))

(defvar *depth* 0)

(deftest "P2"
  (let* ((seen nil) (task-thread nil)
         (op (operator (lambda (task)
                         (setf task-thread (task-thread task)
                               seen (multiple-value-list
                                     (ask task (lambda () (values *depth* sb-thread:*current-thread*
                                                                  (princ-to-string (task-condition sc::*task*)))))))
                         (answer task 'use-value :patched))))
         (r (gather (list (lambda () (let ((*depth* 7)) (fetch 3)))) :on-failure :park)))
    (sb-thread:join-thread op)
    (ok "P2 ASK evaluates in the parked task's own thread: it reads a binding only that thread has"
        (and (equal r '(:patched)) (eql (first seen) 7) (eq (second seen) task-thread)
             (search "no such item 3" (third seen)))
        (format nil "~s ~s" r seen))))

(declaim (notinline price))
(defun price (x) (declare (ignorable x)) (error "PRICE is not written yet"))

(deftest "P3"
  (let* ((threads '())
         (op (operator (lambda (task)
                         (declare (ignorable task))
                         (setf (symbol-function 'price) (lambda (x) (* x 2)))
                         (answer task 'retry))))
         (r (gather (list (lambda () (push sb-thread:*current-thread* threads) (price 21))) :on-failure :park)))
    (sb-thread:join-thread op)
    (ok "P3 a function is redefined while the task is parked; RETRY runs the task again in the same thread"
        (and (equal r '(42)) (= (length threads) 2) (eq (first threads) (second threads)))
        (format nil "~s, ~d runs, same thread ~a" r (length threads) (eq (first threads) (second threads))))))

(deftest "P4"
  (let* ((cleaned nil) (sibling nil)
         (op (operator (lambda (task) (answer task 'give-up))))
         (c (caught (with-scope (s :on-failure :park)
                      (spawn (lambda () (fetch 3)) :name "bad")
                      (setf sibling (spawn (lambda () (unwind-protect (pause 5) (setf cleaned t)))))))))
    (sb-thread:join-thread op)
    (ok "P4 GIVE-UP ends the parked task as failed: the scope is cancelled and signals TASK-ERROR"
        (and (typep c 'task-error) cleaned (eq (task-state sibling) :cancelled))
        (format nil "~a, sibling ~a" c (and sibling (task-state sibling))))))

(deftest "P5"
  (setf *parks* (make-chan))
  (let* ((cleaned nil) (task nil) (c nil)
         (took (ms (setf c (caught (with-scope (s :on-failure :park :timeout 0.3)
                                     (setf task (spawn (lambda () (unwind-protect (fetch 3) (setf cleaned t)))))))))))
    (ok "P5 a parked task nobody answers is cancelled with its scope, and unwinds"
        (and (typep c 'timed-out) cleaned (eq (task-state task) :cancelled) (<= 290 took 700))
        (format nil "~d ms, ~a, cleaned ~a, ~a" took c cleaned (and task (task-state task))))))

(deftest "P6"
  (let* ((seen nil)
         (op (operator (lambda (task)
                         (answer task 'no-such-restart)
                         (pause 0.1)
                         (push (list (task-state task) (task-note task)) seen)
                         (answer task 'use-value)
                         (pause 0.1)
                         (push (list (task-state task) (task-note task)) seen)
                         (answer task 'use-value 1))))
         (r (gather (list (lambda () (fetch 3))) :on-failure :park)))
    (sb-thread:join-thread op)
    (ok "P6 an unknown restart, and one called with the wrong arguments, leave the task parked and say why"
        (and (equal r '(1))
             (eq (first (second seen)) :parked) (search "no restart named" (second (second seen)))
             (eq (first (first seen)) :parked) (search "the answer failed" (second (first seen))))
        (format nil "~s ~s" r (reverse seen)))))

(deftest "P7"
  (let* ((count nil)
         (op (progn
               (setf *parks* (make-chan))
               (sb-thread:make-thread
                (lambda ()
                  (let ((a (recv *parks* :timeout 5)) (b (recv *parks* :timeout 5)))
                    (setf count (length (parked (task-scope a))))
                    (answer b 'use-value (task-name b))
                    (pause 0.1)
                    (answer a 'use-value (task-name a)))))))
         (r (gather (list (lambda () (fetch 3)) (lambda () (fetch 3)) (lambda () (fetch 1)))
                    :name "pair" :on-failure :park)))
    (sb-thread:join-thread op)
    (ok "P7 two tasks parked at once are listed together and answered in any order"
        (and (equal r '("pair/0" "pair/1" 10)) (eql count 2))
        (format nil "~s, ~a parked" r count))))

(deftest "P8"
  (let* ((seen '()) (task nil)
         (op (operator (lambda (task)
                         (push (copy-list (task-restarts task)) seen)
                         (answer task 'look)
                         (pause 0.1)
                         (push (task-state task) seen)
                         (answer task 'use-value 7)))))
    (with-scope (s :on-failure :park)
      (setf task (spawn (lambda () (restart-bind ((look (lambda () nil))) (fetch 3))) :restarts nil)))
    (sb-thread:join-thread op)
    (ok "P8 a task spawned with :restarts nil offers only the restarts of its own code; a restart that returns leaves the task parked"
        (and (equal (task-values task) '(7))
             (eq (first seen) :parked)
             (member 'look (second seen)) (member 'use-value (second seen))
             (not (intersection '(retry give-up finish-with) (second seen))))
        (format nil "~s ~s" (task-values task) (reverse seen)))))

(deftest "P9"
  (let* ((c nil) (task nil)
         (took (let ((*on-park* (lambda (tk) (declare (ignore tk)) (sleep 2)))
                     (*inherit* (adjoin '*on-park* *inherit*)))
                 (ms (setf c (caught
                               (with-scope (s :on-failure :park :timeout 0.15 :grace 0.2)
                                 (setf task (spawn (lambda () (fetch 3)))))))))))
    (ok "P9 a parked task stuck in *ON-PARK* is still interrupted after the grace period"
        (and (typep c 'timed-out) (eq (task-state task) :cancelled) (<= 300 took 800))
        (format nil "~d ms, ~a, ~a" took c (and task (task-state task))))))

(defun run-all ()
  "Run every check, print one line each and the totals. True when none failed."
  (setf *pass* 0 *fail* 0)
  (run-tests)
  (format t "~%~d passed, ~d failed~%" *pass* *fail*)
  (finish-output)
  (zerop *fail*))
