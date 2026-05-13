(ns isaac.comm.imessage.routing
  (:require
    [isaac.comm.imessage.state :as state]))

(defn session-key-for-thread [thread-id]
  (str "imessage:" thread-id))

(defn ensure-session [current-state thread-id handle]
  (if-let [session-key (get-in current-state [:threads thread-id :session-key])]
    {:session-key session-key
     :state       current-state}
    (let [session-key (session-key-for-thread thread-id)]
      {:session-key session-key
       :state       (state/assoc-thread-session current-state thread-id handle session-key)})))
