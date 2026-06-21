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

(defprotocol Client
  "imsg-client API. Real impl wraps a long-lived `imsg rpc`
   subprocess; tests reify a stub that captures calls."
  (-request! [client method params])
  (-notify! [client method params])
  (-stop! [client])
  (-alive?-client [client]))

(defprotocol Subprocess
  "Seam over a live OS subprocess. The default impl wraps a
   babashka.process Process; tests provide a fake."
  (-stdout-reader [proc])
  (-stdin-writer [proc])
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

(defn- drain-stderr! [^java.io.InputStream err]
  (doto (Thread. ^Runnable
           (fn []
             (try
               (let [reader (BufferedReader. (InputStreamReader. err StandardCharsets/UTF_8))]
                 (loop []
                   (when-let [line (.readLine reader)]
                     (log/debug :imsg.subprocess/stderr :line line)
                     (recur))))
               (catch Exception _ nil)))
         "imsg-client-stderr")
    (.setDaemon true)
    (.start)))

(defn spawn-argv
  "Build argv for the long-lived `imsg rpc` subprocess.

   :command — full launch prefix (e.g. [\"ssh\" \"-T\" \"host\"
              \"/usr/local/bin/imsg\"]); takes precedence over :bin.
   :bin     — single executable when :command is absent (default \"imsg\").
   :db-path — optional; appended as --db <path> on the machine where
              imsg runs (local path or remote path for ssh wrappers)."
  [{:keys [bin command db-path]}]
  (let [base (cond
               (seq command) (vec command)
               (seq bin)     [bin]
               :else         ["imsg"])]
    (into base (cond-> ["rpc"]
                  db-path (into ["--db" db-path])))))

(defn- spawn-imsg!
  "Spawn `imsg rpc` with the given options. Returns a Subprocess."
  [opts]
  (let [args (spawn-argv opts)
        bb   (process/process args {:in :pipe :out :pipe :err :pipe})]
    (when-let [err (:err bb)]
      (drain-stderr! err))
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
      (reset! (:pending client) {})
      ;; Notify the comm impl unless this is an intentional stop!.
      (when (and (:on-disconnect client)
                 (not @(:stopping? client)))
        (log/error :imsg.client/disconnected)
        (try ((:on-disconnect client))
             (catch Exception e
               (log/warn :imsg.client/disconnect-handler-failed :error (.getMessage e))))))))

(defn- write-line! [^Writer writer line]
  (locking writer
    (.write writer ^String line)
    (.flush writer)))

(defrecord SubprocessClient [proc writer pending next-id on-notification on-disconnect stopping? reader-thread]
  Client
  (-request! [_ method params]
    (let [id   (swap! next-id inc)
          line (jrpc/request-line id method params)
          p    (promise)]
      (swap! pending assoc id {:promise p :method method})
      (try
        (write-line! writer line)
        (catch Exception e
          (swap! pending dissoc id)
          (deliver p (ex-info "failed to write imsg request"
                              {:type :imsg/write-failed :method method}
                              e))))
      p))
  (-notify! [_ method params]
    (write-line! writer (jrpc/notification-line method params)))
  (-alive?-client [_]
    (-alive? proc))
  (-stop! [_]
    (reset! stopping? true)
    (try (.close ^Writer writer) (catch Exception _ nil))
    (try (-destroy proc) (catch Exception _ nil))
    (when reader-thread
      (try (.join ^Thread reader-thread 1000) (catch Exception _ nil)))))

(defn start!
  "Spawn the imsg subprocess and return a client.

   opts:
     :command         - full launch argv prefix before rpc/--db (wrapper mode)
     :bin             - imsg binary path when :command absent (defaults to 'imsg')
     :db-path         - optional --db argument (path on the host where imsg runs)
     :process         - inject a Subprocess directly (for tests)
     :on-notification - (fn [{:method :params}]) for push notifications
     :on-disconnect   - (fn []) called once when the subprocess exits
                        unexpectedly (not via stop!). Used by the comm
                        impl to mark the client dead and trigger
                        reconnect."
  [{:keys [process on-notification on-disconnect] :as opts}]
  (let [proc          (or process (spawn-imsg! opts))
        writer        (-stdin-writer proc)
        reader        (-stdout-reader proc)
        client        (->SubprocessClient proc writer (atom {}) (atom 0)
                                          on-notification on-disconnect (atom false) nil)
        reader-thread (doto (Thread. ^Runnable #(read-loop! client reader)
                                     "imsg-client-reader")
                        (.setDaemon true)
                        (.start))]
    (assoc client :reader-thread reader-thread)))

(defn request! [client method params] (-request! client method params))
(defn notify! [client method params] (-notify! client method params))
(defn stop! [client] (when client (-stop! client)))
(defn alive? [client] (and client (-alive?-client client)))
