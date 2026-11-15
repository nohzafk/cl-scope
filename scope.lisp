;;; Structured concurrency on SBCL's native threads: a scope starts tasks, each
;;; in a thread, and does not end while one of them lives. A task is cancelled
;;; where it waits, not where it happens to be; a task that fails either
;;; cancels its scope or parks with its stack, to be answered with a restart
;;; from another thread.

(eval-when (:compile-toplevel :load-toplevel :execute)
  (require :sb-bsd-sockets))

(defpackage #:cl-scope
  (:nicknames #:sc)
  (:use #:cl)
  (:export #:with-scope #:spawn #:gather #:cancel-scope
           #:cancelled #:cancelled-reason #:task-error #:task-error-task #:task-error-condition
           #:timed-out #:stuck #:stuck-tasks
           #:check #:shielded #:call-with-waker #:wait-on
           #:pause #:make-chan #:send #:recv #:run #:read-line-from
           #:task-name #:task-state #:task-values #:task-condition #:task-report #:task-restarts
           #:task-note #:task-thread #:task-scope #:scope-tasks #:scope-name
           #:parked #:answer #:ask #:retry #:give-up #:finish-with
           #:*on-park* #:*inherit*))

(in-package #:cl-scope)

;; How a frame of this file's own functions starts when a backtrace prints it
;; from another package.
(defparameter +own-frames+ (format nil "~a::" (package-name *package*)))

(defmacro locked ((lock) &body body)
  `(sb-thread:with-mutex (,lock) ,@body))

(defun now ()
  "The time in seconds, as a rational."
  (/ (get-internal-real-time) internal-time-units-per-second))

;;; ---- cancellation ----

;; Not an ERROR and not a SERIOUS-CONDITION: a handler for either, or
;; IGNORE-ERRORS, in a task's code does not swallow a cancellation.
(define-condition cancelled (condition)
  ((reason :initarg :reason :reader cancelled-reason))
  (:report (lambda (c s) (format s "cancelled: ~s" (cancelled-reason c)))))

;; What a scope's tasks watch. Cancelling a token cancels the tokens made
;; under it, and calls each waker: a function that ends one wait in progress.
(defstruct (token (:constructor %make-token (parent)))
  (lock (sb-thread:make-mutex :name "token"))
  parent
  (reason nil)   ; non-nil once cancelled
  (origin nil)   ; the token CANCEL was called on
  (children '())
  (wakers '()))

(defvar *token* nil "The token of the scope this thread runs in; nil outside any scope.")

(defun make-token (&optional (parent *token*))
  (let ((token (%make-token parent)))
    (when parent
      (locked ((token-lock parent))
        (if (token-reason parent)
            (setf (token-origin token) (token-origin parent)
                  (token-reason token) (token-reason parent))
            (push token (token-children parent)))))
    token))

(defun drop-token (token)
  (let ((parent (token-parent token)))
    (when parent
      (locked ((token-lock parent))
        (setf (token-children parent) (delete token (token-children parent)))))))

(defun cancel (token reason &optional (origin token))
  "Cancel TOKEN and every token under it. True when this call did it."
  (let ((wakers '()) (children '()))
    (locked ((token-lock token))
      (when (token-reason token) (return-from cancel nil))
      (setf (token-origin token) origin
            (token-reason token) reason)
      (shiftf wakers (token-wakers token) '())
      (shiftf children (token-children token) '()))
    (dolist (w wakers) (funcall (cdr w)))
    (dolist (c children) (cancel c reason origin))
    t))

(defun check (&optional (token *token*))
  "Signal CANCELLED when TOKEN is cancelled."
  (let ((reason (and token (token-reason token))))
    (when reason (error 'cancelled :reason reason))))

(defun add-waker (token fn)
  "Have FN called when TOKEN is cancelled. Nil when it already is."
  (let ((cell (cons token fn)))
    (locked ((token-lock token))
      (unless (token-reason token)
        (push cell (token-wakers token))
        cell))))

(defun remove-waker (token cell)
  (locked ((token-lock token))
    (setf (token-wakers token) (delete cell (token-wakers token) :test #'eq))))

(defmacro shielded (&body body)
  "Evaluate BODY outside every scope's cancellation: for the cleanup that must finish."
  `(let ((*token* nil)) ,@body))

;;; ---- waits ----

(defun wait-on (lock queue test &key timeout (token *token*))
  "Wait until TEST, called with LOCK held, gives a value, and return it; nil when
TIMEOUT seconds pass first. Signals CANCELLED when TOKEN is cancelled before or
during the wait and TEST has no value."
  (let ((deadline (and timeout (+ (now) timeout)))
        (cell (and token (add-waker token (lambda () (locked (lock) (sb-thread:condition-broadcast queue)))))))
    (unwind-protect
         (locked (lock)
           (loop
             (let ((v (funcall test))) (when v (return v)))
             (check token)
             (let ((left (and deadline (- deadline (now)))))
               (when (and left (<= left 0)) (return nil))
               (sb-thread:condition-wait queue lock :timeout (and left (float left 1d0))))))
      (when cell (remove-waker token cell)))))

(defstruct (chan (:constructor make-chan ()))
  (lock (sb-thread:make-mutex :name "chan"))
  (queue (sb-thread:make-waitqueue))
  (items '()))

(defun send (chan x)
  (locked ((chan-lock chan))
    (setf (chan-items chan) (nconc (chan-items chan) (list x)))
    (sb-thread:condition-broadcast (chan-queue chan)))
  x)

(defun recv (chan &key timeout)
  "The next item of CHAN and t; nil and nil when TIMEOUT seconds pass first."
  (let ((got (wait-on (chan-lock chan) (chan-queue chan)
                      (lambda () (when (chan-items chan) (list (pop (chan-items chan)))))
                      :timeout timeout)))
    (if got (values (car got) t) (values nil nil))))

(defun pause (seconds)
  "Sleep SECONDS; a cancellation ends it at once."
  (recv (make-chan) :timeout seconds)
  nil)

(defun call-with-waker (waker thunk &optional (token *token*))
  "Call THUNK, a blocking operation that WAKER ends when called from another
thread. When TOKEN is cancelled, CANCELLED is signalled in place of whatever
THUNK returned or signalled."
  (check token)
  (let ((cell (and token (add-waker token waker))))
    (unwind-protect
         (handler-bind ((error (lambda (e) (declare (ignore e)) (check token))))
           (check token)
           (multiple-value-prog1 (funcall thunk) (check token)))
      (when cell (remove-waker token cell)))))

(defun stop-process (process signal)
  (when (sb-ext:process-alive-p process)
    (ignore-errors (sb-ext:process-kill process signal :process-group))
    (ignore-errors (sb-ext:process-kill process signal :pid))))

(defun run (program args)
  "Run PROGRAM with ARGS; its output as a string, and its exit code. A
cancellation terminates the process, and its process group when it leads one."
  (let ((p (sb-ext:run-program program args :wait nil :input nil :output :stream :error :output)))
    (unwind-protect
         (call-with-waker
          (lambda () (stop-process p 15))
          (lambda ()
            (let ((out (with-output-to-string (s)
                         (loop for line = (read-line (sb-ext:process-output p) nil)
                               while line do (write-line line s)))))
              (loop while (sb-ext:process-alive-p p) do (pause 0.002))
              (values out (sb-ext:process-exit-code p)))))
      (stop-process p 9)
      (loop repeat 500 while (sb-ext:process-alive-p p) do (sleep 0.002))
      (sb-ext:process-close p))))

(defun read-line-from (socket stream)
  "A line from STREAM, the stream of SOCKET, or :eof. A cancellation shuts the socket down."
  (call-with-waker
   (lambda () (ignore-errors (sb-bsd-sockets:socket-shutdown socket :direction :io)))
   (lambda () (read-line stream nil :eof))))

;;; ---- scopes and tasks ----

;; Signalled with SIGNAL, once per grace period, by a scope that was cancelled
;; and still waits for tasks an interrupt did not stop.
(define-condition stuck (condition)
  ((scope :initarg :scope) (tasks :initarg :tasks :reader stuck-tasks)))

(defstruct (scope (:constructor %make-scope))
  name token
  (lock (sb-thread:make-mutex :name "scope"))
  (queue (sb-thread:make-waitqueue))
  (tasks '())
  (live 0)             ; tasks whose thread has not finished
  (failure nil)        ; the first task that failed
  (closed nil)
  (on-failure :cancel) ; :cancel, or :park
  (grace 1))

(defstruct (task (:constructor %make-task))
  name scope fn thread bindings
  (own-restarts t)     ; nil: the task offers no RETRY, GIVE-UP or FINISH-WITH of its own
  (state :running)     ; :running :parked :done :failed :cancelled
  (values '())
  condition report restarts note
  (inbox (make-chan))) ; where a parked task takes its answers

(define-condition task-error (error)
  ((task :initarg :task :reader task-error-task)
   (condition :initarg :condition :reader task-error-condition))
  (:report (lambda (c s)
             (format s "task ~a failed: ~a" (task-name (task-error-task c)) (task-error-condition c)))))

(define-condition timed-out (error)
  ((scope :initarg :scope) (seconds :initarg :seconds))
  (:report (lambda (c s)
             (format s "scope ~a reached its time limit of ~a s"
                     (scope-name (slot-value c 'scope)) (slot-value c 'seconds)))))

(defvar *scope* nil "The scope SPAWN adds to.")
(defvar *task* nil "The task this thread runs.")
(defvar *joining* nil "True in a thread that is waiting for its scope's tasks.")
(defvar *on-park* nil "Called with the task, in its thread, when a task parks.")
(defvar *inherit* '() "The special variables whose values a new task starts with.")

(defun frame-text (line)
  (let ((p (search ": " line)))
    (if (and p (plusp p) (< p 5) (every #'digit-char-p (subseq line 0 p)))
        (subseq line (+ p 2))
        line)))

(defun stack-lines (&optional (count 8))
  "The frames of this thread between the signalling call and the task's own start."
  (let* ((text (with-output-to-string (s)
                 (let ((*print-length* 6) (*print-level* 3))
                   (sb-debug:print-backtrace :stream s :count 60))))
         (lines (loop with start = 0
                      for end = (position #\Newline text :start start)
                      collect (string-trim " " (subseq text start end))
                      while end do (setf start (1+ end))))
         (frames (remove "" (mapcar #'frame-text lines) :test #'string=))
         (rest (member-if-not (lambda (f)
                                (or (search +own-frames+ f) (search "Backtrace for" f)
                                    (eql 0 (search "(ERROR " f)) (eql 0 (search "(SIGNAL " f))
                                    (eql 0 (search "(SB-KERNEL:" f))))
                              frames))
         (end (position-if (lambda (f) (search +own-frames+ f)) rest)))
    (subseq rest 0 (min count (or end (length rest))))))

(defun set-state (task state)
  (let ((scope (task-scope task)))
    (locked ((scope-lock scope))
      (setf (task-state task) state)
      (sb-thread:condition-broadcast (scope-queue scope)))))

(defun park (task condition)
  "Stop TASK where CONDITION was signalled until an answer invokes a restart.
Returns only by a restart's transfer, or by a cancellation."
  (setf (task-restarts task) (mapcar #'restart-name (compute-restarts condition)))
  (set-state task :parked)
  (when *on-park* (funcall *on-park* task))
  (loop
    (let ((answer (recv (task-inbox task))))
      (if (functionp answer)
          (funcall answer)
          (let ((restart (find-restart (car answer) condition)))
            (cond ((null restart)
                   (setf (task-note task) (format nil "no restart named ~a is on offer" (car answer))))
                  (t
                   (setf (task-note task) nil)
                   (set-state task :running)
                   (handler-case (apply #'invoke-restart restart (cdr answer))
                     (error (e)
                       (setf (task-note task) (format nil "the answer failed: ~a" e))))
                   ;; Here only when the restart returned or failed: the task is where it was.
                   (set-state task :parked))))))))

(defun finish-task (task state)
  (let ((scope (task-scope task)) (first nil))
    (locked ((scope-lock scope))
      (setf (task-state task) state)
      (when (and (eq state :failed) (null (scope-failure scope)))
        (setf (scope-failure scope) task
              first t))
      (decf (scope-live scope))
      (sb-thread:condition-broadcast (scope-queue scope)))
    (when first (cancel (scope-token scope) (list :failed (task-name task))))))

(defun run-task (task)
  (let* ((scope (task-scope task))
         (*scope* scope)
         (*token* (scope-token scope))
         (*task* task)
         (state :failed))
    (unwind-protect
         (progv (mapcar #'car (task-bindings task)) (mapcar #'cdr (task-bindings task))
           (block run
             (handler-case
                 (handler-bind
                     ((serious-condition
                        (lambda (c)
                          (setf (task-condition task) c
                                (task-report task) (stack-lines))
                          ;; An error parks when the scope says so; anything else, a full stack
                          ;; or heap among them, unwinds.
                          (when (and (eq (scope-on-failure scope) :park) (typep c 'error))
                            (park task c))
                          (return-from run))))
                   (loop
                     (unless (task-own-restarts task)
                       (setf (task-values task) (multiple-value-list (funcall (task-fn task)))
                             state :done)
                       (return-from run))
                     (restart-case
                         (progn
                           (setf (task-values task) (multiple-value-list (funcall (task-fn task)))
                                 state :done)
                           (return-from run))
                       (retry ()
                         :report "Run the task again from its start."
                         (setf (task-condition task) nil))
                       (give-up ()
                         :report "End the task as failed; its scope is cancelled."
                         (return-from run))
                       (finish-with (&rest values)
                         :report "End the task with these values."
                         (setf (task-values task) values
                               (task-condition task) nil
                               state :done)
                         (return-from run)))))
               (cancelled () (setf state :cancelled)))))
      (finish-task task state))))

(defun spawn (fn &key name (scope *scope*) (restarts t))
  "Start FN as a task of SCOPE, in a thread of its own. The task. With :restarts
nil the task offers no RETRY, GIVE-UP or FINISH-WITH: a parked one is answered
only with the restarts its own code establishes."
  (unless scope (error "SPAWN needs a scope: call it inside WITH-SCOPE, or pass :scope."))
  (let ((task (%make-task :fn fn :scope scope :own-restarts restarts
                          :bindings (loop for s in (adjoin '*inherit* *inherit*)
                                          when (boundp s) collect (cons s (symbol-value s))))))
    (locked ((scope-lock scope))
      (when (scope-closed scope)
        (error "scope ~a has ended; it takes no new task" (scope-name scope)))
      (setf (task-name task)
            (or name (format nil "~a/~d" (or (scope-name scope) "scope") (length (scope-tasks scope)))))
      (push task (scope-tasks scope))
      (incf (scope-live scope)))
    (setf (task-thread task)
          (sb-thread:make-thread #'run-task :arguments (list task) :name (task-name task)))
    task))

(defun interrupt (task)
  "The last resort for a task that reaches no wait: signal CANCELLED wherever its thread is."
  (let ((thread (task-thread task)))
    (when (and thread (sb-thread:thread-alive-p thread))
      (ignore-errors
       (sb-thread:interrupt-thread
        thread (lambda () (unless *joining* (error 'cancelled :reason :interrupted))))))))

(defun join (scope)
  "Wait until every task of SCOPE has finished. Once the scope is cancelled, a
task still running after one grace period is interrupted, and after each
further one STUCK is signalled."
  (let* ((token (scope-token scope))
         (lock (scope-lock scope))
         (queue (scope-queue scope))
         (grace (scope-grace scope))
         (cell (add-waker token (lambda () (locked (lock) (sb-thread:condition-broadcast queue)))))
         (since nil)
         (stage 0))
    (unwind-protect
         (loop
           (let ((late '()))
             (locked (lock)
               (when (zerop (scope-live scope))
                 (setf (scope-closed scope) t)
                 (return))
               (if (null (token-reason token))
                   (sb-thread:condition-wait queue lock)
                   (progn
                     (unless since (setf since (now)))
                     (let ((due (+ since (* grace (1+ stage)))))
                       (if (< (now) due)
                           (sb-thread:condition-wait queue lock :timeout (float (- due (now)) 1d0))
                           (setf stage (1+ stage)
                                 late (remove :running (scope-tasks scope) :key #'task-state :test-not #'eq)))))))
             (when late
               (if (= stage 1)
                   (mapc #'interrupt late)
                   (signal 'stuck :scope scope :tasks late)))))
      (when cell (remove-waker token cell))))
  (dolist (task (scope-tasks scope))
    (when (task-thread task) (sb-thread:join-thread (task-thread task) :default nil))))

(defun call-with-scope (fn &key name (on-failure :cancel) timeout (grace 1))
  (let* ((token (make-token *token*))
         (scope (%make-scope :name name :token token :on-failure on-failure :grace grace))
         (timer (and timeout
                     (sb-ext:make-timer (lambda () (cancel token :timeout)) :thread t :name "scope time limit")))
         (ok nil)
         (values '()))
    (when timer (sb-ext:schedule-timer timer timeout))
    (unwind-protect
         (handler-case
             (let ((*scope* scope) (*token* token))
               (setf values (multiple-value-list (funcall fn scope))
                     ok t))
           (cancelled (c)
             ;; A cancellation that began at this scope ends here; one from outside goes on.
             (if (eq (token-origin token) token)
                 (setf ok :own)
                 (error c))))
      (let ((*joining* t))
        (unless ok (cancel token :scope-exit))
        (join scope)
        (when timer (sb-ext:unschedule-timer timer))
        (drop-token token)))
    (let ((failed (scope-failure scope)))
      (cond (failed
             (error 'task-error :task failed :condition (task-condition failed)))
            ((and (eq (token-reason token) :timeout) (eq (token-origin token) token))
             (error 'timed-out :scope scope :seconds timeout))
            ((eq ok :own) nil)
            (t (values-list values))))))

(defmacro with-scope ((var &rest options &key name on-failure timeout grace) &body body)
  "Evaluate BODY with VAR bound to a new scope, and return only when every task
spawned in it has finished. :on-failure :cancel (the default): the first task
that fails cancels the others, and TASK-ERROR is signalled here. :on-failure
:park: a task whose code signals an error stops there, with its stack, until
ANSWER gives it a restart; the others go on. :timeout seconds cancels the scope
and signals TIMED-OUT. :grace is how long a cancelled task has to reach a wait."
  (declare (ignore name on-failure timeout grace))
  `(call-with-scope (lambda (,var) (declare (ignorable ,var)) ,@body) ,@options))

(defun cancel-scope (scope &optional (reason :asked))
  (cancel (scope-token scope) reason))

(defun gather (thunks &key name (on-failure :cancel) timeout)
  "Call each of THUNKS as a task of one scope; their first values, in order."
  (let ((tasks '()))
    (with-scope (s :name (or name "gather") :on-failure on-failure :timeout timeout)
      (setf tasks (mapcar (lambda (f) (spawn f)) thunks)))
    (mapcar (lambda (task) (first (task-values task))) tasks)))

;;; ---- a parked task, from another thread ----

(defun parked (scope)
  (locked ((scope-lock scope))
    (remove :parked (scope-tasks scope) :key #'task-state :test-not #'eq)))

(defun answer (task restart &rest args)
  "Have the parked TASK invoke RESTART with ARGS, in its own thread, where it stopped."
  (send (task-inbox task) (cons restart args))
  task)

(defun ask (task thunk &key (timeout 5))
  "Call THUNK in the thread of the parked TASK, where it stopped, and return its values."
  (let ((reply (make-chan)))
    (send (task-inbox task)
          (lambda ()
            (send reply (handler-case (multiple-value-list (funcall thunk))
                          (error (e) (list :error (princ-to-string e)))))))
    (values-list (shielded (recv reply :timeout timeout)))))
