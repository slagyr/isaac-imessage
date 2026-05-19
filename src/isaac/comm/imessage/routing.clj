(ns isaac.comm.imessage.routing
  (:require
    [isaac.comm.imessage.state :as state]))

(defn session-key-for-chat [chat-guid]
  (str "imessage:" chat-guid))

(defn ensure-session [current-state chat-guid handle]
  (if-let [session-key (get-in current-state [:chats chat-guid :session-key])]
    {:session-key session-key
     :state       current-state}
    (let [session-key (session-key-for-chat chat-guid)]
      {:session-key session-key
       :state       (state/assoc-chat-session current-state chat-guid handle session-key)})))
