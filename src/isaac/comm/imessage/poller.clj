(ns isaac.comm.imessage.poller
  (:require
    [isaac.comm.imessage :as imessage]))

(defn- sleep-ms [ms]
  (Thread/sleep ms))

(defn run-once! [{:keys [isaac-home db-path state-path drain-fn]
                  :or   {drain-fn imessage/drain-once!}}]
  (drain-fn isaac-home db-path state-path))

(defn start! [{:keys [isaac-home db-path state-path interval-ms drain-fn sleep-fn]
               :or   {interval-ms 1000
                      drain-fn    imessage/drain-once!
                      sleep-fn    sleep-ms}}]
  (let [running?    (atom true)
        last-result (atom nil)
        runner      (future
                      (while @running?
                        (reset! last-result (drain-fn isaac-home db-path state-path))
                        (when @running?
                          (sleep-fn interval-ms))))]
    {:running?    running?
     :last-result last-result
     :future      runner}))

(defn stop! [runner]
  (reset! (:running? runner) false)
  (future-cancel (:future runner))
  nil)
