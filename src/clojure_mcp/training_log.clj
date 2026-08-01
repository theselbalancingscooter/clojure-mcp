(ns clojure-mcp.training-log
  "Opt-in training-log emit for clojure-mcp sessions.

   Off by default. Enable by setting the `CLOJURE_MCP_TRAINING_DIR`
   environment variable to a writable directory before starting the
   server. Each intercepted tool call becomes one line of
   `session-<uuid>-turns.jsonl`; a `session-<uuid>-summary.edn`
   snapshots on JVM shutdown.

   The emitted shape mirrors the PRD-09 §2.1/§2.2 schema used by the
   verity/memory-hawk corpus pipeline (see the ADR link in the PR
   description). Anyone can point at that pipeline OR any other
   ingest that reads the same shape.

   Zero effect on tool behaviour: if the emit throws or the disk is
   full, the caller sees nothing — WARN goes to the logger and the
   tool response continues normally. Emit-side failure MUST NOT
   propagate to the client.

   Non-goals:
   - No network I/O. The emit writes local files; dispatch to any
     server is a separate concern.
   - No PII scrubbing. Callers should treat the training dir as
     sensitive (contains file contents + shell arguments).
   - No hook for tool responses whose bytes reveal secrets — a
     downstream ingest is expected to redact."
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [taoensso.timbre :as log])
  (:import [java.time Instant]
           [java.util UUID]))

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

(defn record-tool-call!
  "Called from `create-async-tool`'s continuation AFTER the tool_fn's
   `clj-result-k` fires. Args are captured verbatim from the MCP call;
   the result is the vector of strings the tool passed to `clj-result-k`.

   Never throws — emit failures are logged and swallowed so a training-log
   bug can never break a tool response."
  [tool-name arg-map result-strs error?]
  (when enabled?
    (try
      (ensure-session!)
      (let [before  @session-state
            _       (swap! session-state
                           (fn [s] (-> s
                                       (update :turn-index inc)
                                       (update :turn-count inc)
                                       (update :tools-used conj tool-name))))
            turn    {:session-id     (:session-id before)
                     :turn-index     (:turn-index before)
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
            line    (json/write-str turn)]
        (io/make-parents (turns-path))
        (spit (turns-path) (str line "\n") :append true))
      (catch Throwable e
        (log/warn "clojure-mcp training emit failed"
                  {:tool tool-name :error (.getMessage e)})))))

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
      (catch Throwable e
        (log/warn "clojure-mcp training summary write failed"
                  {:error (.getMessage e)})))))

(when enabled?
  (log/info "clojure-mcp training-log enabled" {:dir training-dir})
  (.addShutdownHook (Runtime/getRuntime) (Thread. ^Runnable write-summary!)))
