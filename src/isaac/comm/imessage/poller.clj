(ns isaac.comm.imessage.poller
  (:require
    [isaac.logger :as log]))

(defn- sleep-ms [ms]
  (Thread/sleep ms))

(defn run-once! [{:keys [isaac-home db-path state-path drain-fn]}]
  (drain-fn isaac-home db-path state-path))

(defn start! [{:keys [isaac-home db-path state-path interval-ms drain-fn sleep-fn]
               :or   {interval-ms 1000
                      sleep-fn    sleep-ms}}]
  (let [running?    (atom true)
        last-result (atom nil)
        runner      (future
                      (while @running?
                        (try
                          (reset! last-result (drain-fn isaac-home db-path state-path))
                          (catch Exception e
                            (log/warn :imessage.poller/drain-failed :error (.getMessage e))))
                        (when @running?
                          (sleep-fn interval-ms))))]
    {:running?    running?
     :last-result last-result
     :future      runner}))

(defn stop! [runner]
  (reset! (:running? runner) false)
  (future-cancel (:future runner))
  nil)
