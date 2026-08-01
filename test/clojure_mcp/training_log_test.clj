(ns clojure-mcp.training-log-test
  "Tests for the opt-in training-log emit. All hermetic — write to a
   fresh tmp dir per test, assert JSONL round-trips, then delete.
   The `record-tool-call!` fn must NEVER throw (a bug there could
   otherwise break every tool call), so error-path tests are here too."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [clojure-mcp.training-log :as tl])
  (:import [java.util UUID]
           [java.util.concurrent CountDownLatch Executors TimeUnit]))

(defn- fresh-dir []
  (let [d (str (System/getProperty "java.io.tmpdir")
               "/clojure-mcp-tl-test-" (UUID/randomUUID))]
    (io/make-parents (str d "/x"))
    d))

(defn- drain! []
  ;; The record path is now async. Tests need to wait for the writer
  ;; thread to finish before asserting on file contents. Submit a
  ;; blocking no-op and await it — this guarantees any earlier submit
  ;; has run because the executor is single-threaded.
  (let [latch (CountDownLatch. 1)]
    (.submit @#'tl/writer-executor
             ^Runnable (fn [] (.countDown latch)))
    (.await latch 5 TimeUnit/SECONDS)))

(defn- with-training-dir* [dir f]
  (let [original @#'tl/training-dir]
    (try
      (alter-var-root #'tl/training-dir (constantly dir))
      (alter-var-root #'tl/enabled? (constantly (some? dir)))
      (reset! @#'tl/session-state
              {:session-id nil :turn-index 0 :tools-used #{}
               :started-at nil :turn-count 0})
      (reset! tl/failure-count 0)
      (f)
      (finally
        (alter-var-root #'tl/training-dir (constantly original))
        (alter-var-root #'tl/enabled? (constantly (some? original)))))))

(defmacro with-training-dir [dir & body]
  `(with-training-dir* ~dir (fn [] ~@body)))

(defn- list-turn-lines [dir]
  (let [d (io/file dir)]
    (when (.exists d)
      (->> (.listFiles d)
           (filter #(string/ends-with? (.getName ^java.io.File %) "-turns.jsonl"))
           (mapcat #(string/split-lines (slurp %)))
           (remove string/blank?)
           vec))))

(deftest records-tool-call-when-enabled-test
  (let [dir (fresh-dir)]
    (with-training-dir dir
      (tl/record-tool-call! "clojure_eval" {:code "(+ 1 2)"} ["3"] false)
      (drain!)
      (let [lines (list-turn-lines dir)]
        (is (= 1 (count lines)) "one JSONL line per tool call")
        (is (string/includes? (first lines) "\"clojure_eval\""))
        (is (string/includes? (first lines) "\"(+ 1 2)\""))
        (is (string/includes? (first lines) "\"turn-index\":0"))))))

(deftest no-op-when-disabled-test
  (with-training-dir nil
    (tl/record-tool-call! "clojure_eval" {:code "(+ 1 2)"} ["3"] false)
    (is (false? tl/enabled?))))

(deftest error-flag-sets-outcome-error-test
  (let [dir (fresh-dir)]
    (with-training-dir dir
      (tl/record-tool-call! "clojure_eval" {:code "(/ 1 0)"} ["ArithmeticException"] true)
      (drain!)
      (let [lines (list-turn-lines dir)]
        (is (= 1 (count lines)))
        (is (string/includes? (first lines) "\"outcome\":\"error\""))
        (is (string/includes? (first lines) "\"status\":\"error\""))))))

(deftest monotonic-turn-index-across-calls-test
  (let [dir (fresh-dir)]
    (with-training-dir dir
      (dotimes [i 5]
        (tl/record-tool-call! "clojure_eval" {:code (str "(inc " i ")")}
                              [(str (inc i))] false))
      (drain!)
      (let [lines (list-turn-lines dir)]
        (is (= 5 (count lines)))
        (is (= [0 1 2 3 4]
               (mapv (fn [line]
                       (let [m (re-find #"\"turn-index\":(\d+)" line)]
                         (Long/parseLong (second m))))
                     lines)))))))

(deftest concurrent-emit-preserves-unique-indexes-test
  (testing (str "many threads hammering record-tool-call! concurrently must NOT "
                "produce duplicate turn-indexes — the writer-executor serialises "
                "reservation + append.")
    (let [dir (fresh-dir)
          n 50
          pool (Executors/newFixedThreadPool 8)]
      (with-training-dir dir
        (try
          (let [futures (mapv (fn [i]
                                (.submit pool
                                         ^Callable
                                         (fn [] (tl/record-tool-call!
                                                  "clojure_eval"
                                                  {:code (str "(inc " i ")")}
                                                  [(str (inc i))] false))))
                              (range n))]
            (doseq [f futures] (.get f)))
          (finally
            (.shutdown pool)
            (.awaitTermination pool 5 TimeUnit/SECONDS)))
        (drain!)
        (let [lines (list-turn-lines dir)
              indexes (mapv (fn [line]
                              (Long/parseLong
                                (second (re-find #"\"turn-index\":(\d+)" line))))
                            lines)]
          (is (= n (count lines))
              "one line per submitted call")
          (is (= n (count (distinct indexes)))
              "no duplicate turn-indexes under concurrent submission")
          (is (= (set (range n)) (set indexes))
              "the full 0..N-1 range is present"))))))

(deftest emit-never-throws-on-io-failure-test
  (testing (str "the emit MUST swallow every expected exception. Portable "
                "fixture: point CLOJURE_MCP_TRAINING_DIR at a REGULAR FILE "
                "(not a directory) — subsequent io/make-parents + spit will "
                "fail with an IOException that the emit path must catch.")
    (let [tmp (fresh-dir)
          not-a-dir (str tmp "/regular-file-not-a-dir")
          _ (spit not-a-dir "seed")]
      (with-training-dir not-a-dir
        (is (nil? (tl/record-tool-call! "clojure_eval" {} ["ok"] false)))
        (drain!)
        (is (pos? @tl/failure-count)
            "failure counter must have been bumped by the swallowed exception")
        (is (empty? (list-turn-lines not-a-dir))
            "no JSONL file created (write path failed as expected)")))))
