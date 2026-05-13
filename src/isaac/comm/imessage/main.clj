(ns isaac.comm.imessage.main
  (:require
    [isaac.comm.imessage.poller :as poller]))

(defn- parse-long-safe [value]
  (when value
    (parse-long value)))

(defn parse-args [args]
  (let [[first-arg & rest-args] args
        [mode remaining] (if (contains? #{"once" "loop"} first-arg)
                           [(keyword first-arg) rest-args]
                           [:once args])]
    (loop [opts {:mode :once :isaac-home nil :db-path nil :state-path nil :interval-ms 1000}
           remaining remaining]
      (if (empty? remaining)
        (assoc opts :mode mode)
        (let [[flag value & more] remaining]
          (case flag
            "--isaac-home"  (recur (assoc opts :isaac-home value) more)
            "--db-path"     (recur (assoc opts :db-path value) more)
            "--state-path"  (recur (assoc opts :state-path value) more)
            "--interval-ms" (recur (assoc opts :interval-ms (or (parse-long-safe value) 1000)) more)
            (recur opts more)))))))

(defn run-poller! [{:keys [mode isaac-home db-path state-path interval-ms]}]
  (let [poller-opts {:isaac-home isaac-home
                     :db-path db-path
                     :state-path state-path
                     :interval-ms interval-ms}]
    (case mode
      :loop (poller/start! poller-opts)
      (poller/run-once! poller-opts))))

(defn -main [& args]
  (run-poller! (parse-args args)))
