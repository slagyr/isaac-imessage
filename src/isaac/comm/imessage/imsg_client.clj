(ns isaac.comm.imessage.imsg-client
  "JSON-RPC client for the imsg CLI (https://github.com/openclaw/imsg).
   Spawns `imsg rpc` as a persistent subprocess and talks newline-delimited
   JSON-RPC 2.0 over its stdin/stdout. Requests are correlated by id and
   resolved on a background reader thread; pushed notifications (e.g. from
   imsg's watch.subscribe) are dispatched to an :on-notification callback.

   Generic JSON-RPC message construction and predicates come from
   isaac.util.jsonrpc."
  (:require
    [babashka.process :as process]
    [isaac.logger :as log]
    [isaac.util.jsonrpc :as jrpc])
  (:import
    (java.io BufferedReader InputStreamReader OutputStreamWriter Writer)
    (java.nio.charset StandardCharsets)))

(defprotocol Subprocess
  "Seam over a live OS subprocess. The default impl wraps a
   babashka.process Process; tests provide a fake."
  (-stdout-reader [proc])
  (-stdin-writer  [proc])
  (-alive? [proc])
  (-destroy [proc]))

(defn- bb-subprocess [bb-proc]
  (reify Subprocess
    (-stdout-reader [_]
      (BufferedReader. (InputStreamReader. (:out bb-proc) StandardCharsets/UTF_8)))
    (-stdin-writer [_]
      (OutputStreamWriter. (:in bb-proc) StandardCharsets/UTF_8))
    (-alive? [_]
      (.isAlive (:proc bb-proc)))
    (-destroy [_]
      (.destroy (:proc bb-proc)))))

(defn- spawn-imsg!
  "Spawn `imsg rpc` with the given options. Returns a Subprocess."
  [{:keys [bin db-path]}]
  (let [args (cond-> [(or bin "imsg") "rpc"]
               db-path (into ["--db" db-path]))
        bb   (process/process args {:in :pipe :out :pipe :err :pipe})]
    (bb-subprocess bb)))

(defn- handle-message
  "Route a parsed JSON-RPC message: result/error → resolve the
   matching pending future; notification → on-notification callback."
  [{:keys [pending on-notification]} message]
  (cond
    (jrpc/result? message)
    (when-let [pending-entry (get @pending (:id message))]
      (swap! pending dissoc (:id message))
      (deliver (:promise pending-entry) (:result message)))

    (jrpc/error? message)
    (when-let [pending-entry (get @pending (:id message))]
      (swap! pending dissoc (:id message))
      (deliver (:promise pending-entry)
               (ex-info (or (get-in message [:error :message]) "JSON-RPC error")
                        {:type :imsg/error :rpc-error (:error message)})))

    (jrpc/notification? message)
    (when on-notification
      (try
        (on-notification {:method (:method message)
                          :params (:params message)})
        (catch Exception e
          (log/warn :imsg.notification/handler-failed :error (.getMessage e)))))))

(defn- read-loop! [client ^BufferedReader reader]
  (try
    (loop []
      (when-let [line (.readLine reader)]
        (let [message (jrpc/parse-message line)]
          (if (jrpc/parse-error? message)
            (log/warn :imsg.read/parse-error :line line)
            (handle-message client message)))
        (recur)))
    (catch Exception e
      (log/warn :imsg.read/loop-failed :error (.getMessage e)))
    (finally
      ;; Subprocess closed or read failed — fail all pending requests so
      ;; their futures can deref without hanging callers forever.
      (doseq [[_id {:keys [promise]}] @(:pending client)]
        (deliver promise (ex-info "imsg subprocess closed"
                                  {:type :imsg/closed})))
      (reset! (:pending client) {}))))

(defn start!
  "Spawn the imsg subprocess and return a client.

   opts:
     :bin             - imsg binary path (defaults to 'imsg' on PATH)
     :db-path         - optional --db argument
     :process         - inject a Subprocess directly (for tests)
     :on-notification - (fn [{:method :params}]) for push notifications"
  [{:keys [process on-notification] :as opts}]
  (let [proc            (or process (spawn-imsg! opts))
        writer          (-stdin-writer proc)
        reader          (-stdout-reader proc)
        pending         (atom {})
        next-id         (atom 0)
        client          {:proc            proc
                         :writer          writer
                         :pending         pending
                         :next-id         next-id
                         :on-notification on-notification}
        reader-thread   (doto (Thread. ^Runnable #(read-loop! client reader)
                                       "imsg-client-reader")
                          (.setDaemon true)
                          (.start))]
    (assoc client :reader-thread reader-thread)))

(defn alive? [client]
  (and client (-alive? (:proc client))))

(defn- write-line! [^Writer writer line]
  (locking writer
    (.write writer ^String line)
    (.flush writer)))

(defn request!
  "Send a JSON-RPC request and return a future-like promise that derefs
   to the :result (or an ex-info for errors / closed subprocesses)."
  [client method params]
  (let [id     (swap! (:next-id client) inc)
        line   (jrpc/request-line id method params)
        p      (promise)]
    (swap! (:pending client) assoc id {:promise p :method method})
    (try
      (write-line! (:writer client) line)
      (catch Exception e
        (swap! (:pending client) dissoc id)
        (deliver p (ex-info "failed to write imsg request"
                            {:type :imsg/write-failed :method method}
                            e))))
    p))

(defn notify!
  "Send a JSON-RPC notification (no response expected)."
  [client method params]
  (write-line! (:writer client) (jrpc/notification-line method params)))

(defn stop! [client]
  (when client
    (try
      (when-let [w (:writer client)]
        (.close ^Writer w))
      (catch Exception _ nil))
    (try
      (-destroy (:proc client))
      (catch Exception _ nil))
    (when-let [t (:reader-thread client)]
      (try (.join ^Thread t 1000) (catch Exception _ nil)))))
