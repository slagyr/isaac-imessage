(ns isaac.comm.imessage.inbox)

(defprotocol MessageSource
  (-messages-since [source watermark]))

(defn messages-since [source watermark]
  (-messages-since source watermark))

(defn- max-rowid [messages]
  (when-let [rowids (seq (keep :message-rowid messages))]
    {:message-rowid (apply max rowids)}))

(defn poll! [source state]
  (let [messages      (vec (messages-since source (:watermark state)))
        inbound-only  (->> messages
                           (remove :from-me?)
                           (sort-by :message-rowid)
                           vec)
        new-watermark (or (max-rowid messages)
                          (:watermark state))]
    {:messages inbound-only
     :state    (assoc state :watermark new-watermark)}))
