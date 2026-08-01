(ns clojure-mcp.training-log
  "Opt-in training-log emit for clojure-mcp sessions.

   Off by default. Enable by setting the `CLOJURE_MCP_TRAINING_DIR`
   environment variable to a writable directory before starting the
   server. Each intercepted tool call becomes one line of
   `session-<uuid>-turns.jsonl`; a `session-<uuid>-summary.edn`
   snapshots on JVM shutdown.

   The emitted shape mirrors a schema used by an external training-
   corpus pipeline for LLM fine-tuning. Anyone can point at that
   pipeline OR any other ingest that reads the same shape.

   Zero effect on tool behaviour:
   - The MCP response is NOT blocked on emit I/O. The interceptor
     hands the (name, args, result) triple to a SERIALISED background
     writer (single-thread executor with an unbounded queue) and
     returns immediately. Unbounded is intentional for this opt-in
     tool: dropping training records to protect memory is worse than
     the training records themselves — and the queue can only grow
     if disk I/O is stalled indefinitely, which is a system-wide
     problem the operator should already be seeing.
   - Any Exception during emit is caught, WARN-logged, and counted
     in `failure-count` (public for observability). Fatal JVM errors
     (Error subclasses like OutOfMemoryError) are deliberately NOT
     caught — those signal a system-wide problem the tool caller
     should know about.
   - Turn accounting is TWO-STAGE: the reservation step gives every
     submitted call a unique sequential `:turn-index` (never
     collides), but the session-level `:turn-count` only advances on
     WRITE SUCCESS. The summary's `:tool-call-count` reflects records
     that actually landed in the JSONL, not records that were
     submitted-but-failed.

   Concurrency:
   - Turn-index reservation + JSONL append run inside a serialised
     writer (single-thread executor). Concurrent callbacks reserve
     unique sequential indexes and their lines land in submission
     order. Verified by the concurrent-emission test.

   Non-goals:
   - No network I/O. The emit writes local files; dispatch to any
     server is a separate concern.
   - No PII scrubbing. Callers should treat the training dir as
     sensitive (contains file contents + shell arguments)."
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [taoensso.timbre :as log])
  (:import [java.time Instant]
           [java.util UUID]
           [java.util.concurrent Executors ExecutorService TimeUnit]))

(def ^:private training-dir
  (some-> (System/getenv "CLOJURE_MCP_TRAINING_DIR") str/trim not-empty))

(def enabled?
  "True when the CLOJURE_MCP_TRAINING_DIR env var is set to a
   non-empty string. Callers can short-circuit expensive setup with
   this — the interceptor itself also no-ops when false."
  (some? training-dir))

(defonce ^:private session-state
  (atom {:session-id nil :turn-index 0 :tools-used #{}
         :started-at nil :turn-count 0}))

(defonce ^{:doc "Emit-side failure counter. Bumped once per exception
  swallowed by the emit path (either the per-turn record or the shutdown
  summary write). Public so ops can graph 'training-log emit health'
  without reading logs."}
  failure-count (atom 0))

;; Single-thread serialised writer. All record + write work runs here;
;; the caller thread (the MCP tool callback) returns immediately after
;; submit, so response latency is unaffected by disk I/O.
(defonce ^:private ^ExecutorService writer-executor
  (let [thread-factory
        (reify java.util.concurrent.ThreadFactory
          (newThread [_ r]
            (doto (Thread. r "clojure-mcp-training-log")
              (.setDaemon true))))]
    (Executors/newSingleThreadExecutor thread-factory)))

(defn- ensure-session! []
  (swap! session-state
         (fn [s]
           (cond-> s
             (nil? (:session-id s))
             (assoc :session-id (str (UUID/randomUUID))
                    :started-at (str (Instant/now))
                    :turn-index 0)))))

(defn- turns-path []
  (io/file training-dir
           (str "session-" (:session-id @session-state) "-turns.jsonl")))

(defn- summary-path []
  (io/file training-dir
           (str "session-" (:session-id @session-state) "-summary.edn")))

(defn- record-tool-call-sync!
  "Runs on the writer-executor thread. Two-stage accounting:
   1. RESERVE: atomically increment `:turn-index` (single CAS via
      swap-vals!) — every submitted call gets a unique sequential
      index even if writes race.
   2. WRITE: build + append the JSONL line. On success, bump
      `:turn-count` + `:tools-used` so summary counters reflect
      records that ACTUALLY landed (not just ones that were
      submitted-but-failed). On failure, `:turn-index` is still
      consumed (leaves an honest gap that a curator UI can detect)
      but summary counts stay accurate."
  [tool-name arg-map result-strs error?]
  (try
    (ensure-session!)
    (let [[before after]
          (swap-vals! session-state
                      (fn [s] (update s :turn-index inc)))
          reserved-index (:turn-index before)
          turn {:session-id     (:session-id after)
                :turn-index     reserved-index
                :model          "unknown"
                :user           nil
                :reasoning      nil
                :tool-calls     [{:tool   tool-name
                                  :input  (or arg-map {})
                                  :output (apply str result-strs)
                                  :status (if error? :error :ok)
                                  :ms     nil}]
                :outcome        (if error? :error :verified)
                :evidence-hash  nil
                :read-only?     false
                :timestamp      (str (Instant/now))}
          line (json/write-str turn)]
      (io/make-parents (turns-path))
      (spit (turns-path) (str line "\n") :append true)
      ;; Advance the "actually-written" counters AFTER a successful
      ;; write — summary EDN's counts must not drift from the JSONL.
      (swap! session-state
             (fn [s] (-> s
                         (update :turn-count inc)
                         (update :tools-used conj tool-name)))))
    (catch Exception e
      (swap! failure-count inc)
      (log/warn "clojure-mcp training emit failed"
                {:tool tool-name :error (.getMessage e)}))))

(defn record-tool-call!
  "Called from `create-async-tool`'s continuation AFTER the tool_fn's
   `clj-result-k` fires. Non-blocking: submits the record job to a
   bounded serialised writer and returns immediately. Callers see no
   latency from disk I/O and no exception can propagate.

   Idempotent for the disabled case — no-op when the env var is unset."
  [tool-name arg-map result-strs error?]
  (when enabled?
    (try
      (.submit writer-executor
               ^Runnable
               (fn [] (record-tool-call-sync!
                        tool-name arg-map result-strs error?)))
      (catch java.util.concurrent.RejectedExecutionException e
        ;; Executor is shutting down — count + carry on.
        (swap! failure-count inc)
        (log/warn "clojure-mcp training emit rejected (executor closed)"
                  {:tool tool-name :error (.getMessage e)})))
    nil))

(defn- write-summary! []
  (when enabled?
    (try
      (let [{:keys [session-id started-at tools-used turn-count]} @session-state]
        (when session-id
          (spit (summary-path)
                (pr-str
                 {:session-id       session-id
                  :source           "clojure-mcp"
                  :started-at       started-at
                  :ended-at         (str (Instant/now))
                  :turn-count       turn-count
                  :tool-call-count  turn-count
                  :tools-used       (vec (sort tools-used))
                  :project          (System/getProperty "user.dir")
                  :outcome          :verified
                  :read-only?       false
                  :provenance       {:generator-model "unknown"
                                     :verity-version  "n/a"
                                     :verity-commit   "n/a"}
                  :quality-hints    {}}))))
      (catch Exception e
        (swap! failure-count inc)
        (log/warn "clojure-mcp training summary write failed"
                  {:error (.getMessage e)})))))

(defn- flush-and-write-summary! []
  ;; Drain in-flight writes first so the summary's turn-count matches
  ;; what actually landed in the JSONL. Log a warning if drain times
  ;; out — a discarded false from awaitTermination would let
  ;; write-summary! run against unsettled state.
  (try
    (.shutdown writer-executor)
    (let [drained? (.awaitTermination writer-executor 5 TimeUnit/SECONDS)]
      (when-not drained?
        (swap! failure-count inc)
        (log/warn "clojure-mcp training-log drain timed out — summary counts may lag"
                  {:pending-tasks-approx "unknown (executor doesn't expose it)"})))
    (catch InterruptedException _
      (log/warn "clojure-mcp training-log drain interrupted")))
  (write-summary!))

(when enabled?
  (log/info "clojure-mcp training-log enabled" {:dir training-dir})
  (.addShutdownHook (Runtime/getRuntime) (Thread. ^Runnable flush-and-write-summary!)))
