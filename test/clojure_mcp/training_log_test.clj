(ns clojure-mcp.training-log-test
  "Tests for the opt-in training-log emit. All hermetic — write to a
   fresh tmp dir per test, assert JSONL round-trips, then delete.
   The `record-tool-call!` fn must NEVER throw (a bug there could
   otherwise break every tool call), so error-path tests are here too."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [clojure-mcp.training-log :as tl])
  (:import [java.util UUID]))

(defn- fresh-dir []
  (let [d (str (System/getProperty "java.io.tmpdir")
               "/clojure-mcp-tl-test-" (UUID/randomUUID))]
    (io/make-parents (str d "/x"))
    d))

(defn- with-training-dir* [dir f]
  ;; The env-var read at ns-load can't be rebindt from here — instead
  ;; we rebind the private var directly for the duration of the test.
  (let [original @#'tl/training-dir]
    (try
      (alter-var-root #'tl/training-dir (constantly dir))
      (alter-var-root #'tl/enabled? (constantly (some? dir)))
      ;; Reset the session-atom so each test starts clean.
      (reset! @#'tl/session-state
              {:session-id nil :turn-index 0 :tools-used #{}
               :started-at nil :turn-count 0})
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
      (let [lines (list-turn-lines dir)]
        (is (= 1 (count lines)) "one JSONL line per tool call")
        (is (string/includes? (first lines) "\"clojure_eval\""))
        (is (string/includes? (first lines) "\"(+ 1 2)\""))
        (is (string/includes? (first lines) "\"turn-index\":0"))))))

(deftest no-op-when-disabled-test
  (with-training-dir nil
    (tl/record-tool-call! "clojure_eval" {:code "(+ 1 2)"} ["3"] false)
    ;; No file to check for — the emit path short-circuits on
    ;; `enabled?` false. The assertion is simply that the call returned
    ;; without throwing.
    (is (false? tl/enabled?))))

(deftest error-flag-sets-outcome-error-test
  (let [dir (fresh-dir)]
    (with-training-dir dir
      (tl/record-tool-call! "clojure_eval" {:code "(/ 1 0)"} ["ArithmeticException"] true)
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
      (let [lines (list-turn-lines dir)]
        (is (= 5 (count lines)))
        (is (= [0 1 2 3 4]
               (mapv (fn [line]
                       (let [m (re-find #"\"turn-index\":(\d+)" line)]
                         (Long/parseLong (second m))))
                     lines)))))))

(deftest emit-never-throws-on-io-failure-test
  (testing (str "the emit MUST swallow every exception — a bug in the emit "
                "path can't be allowed to break the tool call it's observing")
    (with-training-dir "/proc/1/definitely-not-writable"
      ;; No exception should propagate.
      (is (nil? (tl/record-tool-call! "clojure_eval" {} ["ok"] false))))))
