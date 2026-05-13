(ns isaac.comm.imessage.inbox-spec
  (:require
    [isaac.comm.imessage.inbox :as sut]
    [speclj.core :refer :all]))

(deftype FakeSource [messages]
  sut/MessageSource
  (-messages-since [_ _]
    messages))

(describe "iMessage inbox"

  (it "returns only inbound messages"
    (let [source (->FakeSource [{:message-rowid 41 :from-me? true  :text "outbound"}
                                {:message-rowid 42 :from-me? false :text "hello"}])]
      (should= [{:message-rowid 42 :from-me? false :text "hello"}]
               (:messages (sut/poll! source {:threads {} :watermark nil})))))

  (it "advances the watermark to the highest seen rowid"
    (let [source (->FakeSource [{:message-rowid 41 :from-me? true  :text "outbound"}
                                {:message-rowid 42 :from-me? false :text "hello"}])]
      (should= {:message-rowid 42}
               (get-in (sut/poll! source {:threads {} :watermark nil}) [:state :watermark]))))

  (it "keeps the prior watermark when no messages are returned"
    (let [source (->FakeSource [])]
      (should= {:message-rowid 42}
               (get-in (sut/poll! source {:threads {} :watermark {:message-rowid 42}})
                       [:state :watermark])))))
