(ns isaac.comm.imessage.chat-db
  (:require
    [isaac.comm.imessage.inbox :as inbox]))

(defprotocol RawMessageStore
  (-rows-since [store watermark]))

(defn rows-since [store watermark]
  (-rows-since store watermark))

(defn normalize-row [row]
  {:message-rowid (:rowid row)
   :thread-id      (:chat_guid row)
   :handle         (:handle_id row)
   :from-me?       (boolean (pos? (long (or (:is_from_me row) 0))))
   :text           (:text row)
   :sent-at        (:date row)})

(defn fetch-messages [store watermark]
  (->> (rows-since store watermark)
       (map normalize-row)
       (sort-by :message-rowid)
       vec))

(deftype ChatDbSource [store]
  inbox/MessageSource
  (-messages-since [_ watermark]
    (fetch-messages store watermark)))

(defn message-source [store]
  (->ChatDbSource store))
