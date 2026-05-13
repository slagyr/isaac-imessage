(ns isaac.comm.imessage.chat-db-spec
  (:require
    [isaac.comm.imessage.chat-db :as sut]
    [isaac.comm.imessage.inbox :as inbox]
    [speclj.core :refer :all]))

(deftype FakeStore [rows]
  sut/RawMessageStore
  (-rows-since [_ _]
    rows))

(describe "chat db adapter"

  (it "normalizes a raw Messages row into the inbox message shape"
    (should= {:message-rowid 42
              :thread-id      "chat-guid-1"
              :handle         "+15551234567"
              :from-me?       false
              :text           "hello"
              :sent-at        1234567890}
             (sut/normalize-row {:rowid       42
                                 :chat_guid   "chat-guid-1"
                                 :handle_id   "+15551234567"
                                 :is_from_me  0
                                 :text        "hello"
                                 :date        1234567890})))

  (it "fetches normalized rows in message-rowid order"
    (let [store (->FakeStore [{:rowid       43
                               :chat_guid   "chat-guid-1"
                               :handle_id   "+15551234567"
                               :is_from_me  0
                               :text        "second"
                               :date        2}
                              {:rowid       42
                               :chat_guid   "chat-guid-1"
                               :handle_id   "+15551234567"
                               :is_from_me  0
                               :text        "first"
                               :date        1}])]
      (should= [42 43]
               (mapv :message-rowid
                     (sut/fetch-messages store {:message-rowid 41})))))

  (it "adapts a raw store into the inbox MessageSource protocol"
    (let [store  (->FakeStore [{:rowid       42
                                :chat_guid   "chat-guid-1"
                                :handle_id   "+15551234567"
                                :is_from_me  0
                                :text        "hello"
                                :date        1}])
          source (sut/message-source store)]
      (should= [{:message-rowid 42
                 :thread-id      "chat-guid-1"
                 :handle         "+15551234567"
                  :from-me?       false
                  :text           "hello"
                  :sent-at        1}]
               (inbox/messages-since source {:message-rowid 41})))))
