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

  (it "builds a sqlite3 command for rows after the watermark"
    (let [command (sut/sqlite3-command "/Users/micah/Library/Messages/chat.db"
                                       {:message-rowid 41})]
      (should= "sqlite3" (first command))
      (should= "/Users/micah/Library/Messages/chat.db" (second command))
      (should (re-find #"m\.ROWID > 41" (nth command 2)))
      (should (re-find #"replace\(ifnull\(m\.text, ''\)" (nth command 2)))))

  (it "parses a sqlite line into a raw row map"
    (should= {:rowid 42
              :chat_guid "chat-guid-1"
              :handle_id "+15551234567"
              :is_from_me 0
              :text "hello\nthere"
              :date 1234567890}
             (sut/parse-row-line "42\u001Fchat-guid-1\u001F+15551234567\u001F0\u001Fhello\\nthere\u001F1234567890")))

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

  (it "reads rows from sqlite3 output through a shell-backed store"
    (let [store (sut/shell-store "/Users/micah/Library/Messages/chat.db")]
      (with-redefs [sut/run-command (fn [_]
                                      {:exit 0
                                       :out  (str "42\u001Fchat-guid-1\u001F+15551234567\u001F0\u001Fhello\u001F1\n"
                                                  "43\u001Fchat-guid-1\u001F+15551234567\u001F1\u001Freply\u001F2\n")
                                       :err  ""})]
        (should= [{:rowid 42
                   :chat_guid "chat-guid-1"
                   :handle_id "+15551234567"
                   :is_from_me 0
                   :text "hello"
                   :date 1}
                  {:rowid 43
                   :chat_guid "chat-guid-1"
                   :handle_id "+15551234567"
                   :is_from_me 1
                   :text "reply"
                   :date 2}]
                 (sut/rows-since store {:message-rowid 41})))))
