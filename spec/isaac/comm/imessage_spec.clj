(ns isaac.comm.imessage-spec
  (:require
    [isaac.comm.imessage :as sut]
    [isaac.comm.imessage.apple-script]
    [isaac.comm.imessage.inbox]
    [isaac.comm.imessage.state]
    [speclj.core :refer :all]))

(describe "iMessage scaffold"

  (it "loads the top-level namespace"
    (should-not-be-nil (find-ns 'isaac.comm.imessage)))

  (it "exposes the child namespaces"
    (should-not-be-nil (find-ns 'isaac.comm.imessage.apple-script))
    (should-not-be-nil (find-ns 'isaac.comm.imessage.inbox))
    (should-not-be-nil (find-ns 'isaac.comm.imessage.state)))

  (it "normalizes a delivery record before sending"
    (with-redefs [isaac.comm.imessage.apple-script/send-message!
                  (fn [request]
                    (should= {:message "hello"
                              :service "E:me"
                              :target "+15551234567"}
                             request)
                    {:ok true})]
      (should= {:ok true}
               (sut/send! {:content "hello"
                           :service "E:me"
                           :target "+15551234567"}))))

  (it "polls inbound messages and persists the updated state"
    (let [path    (str (System/getProperty "java.io.tmpdir") "/isaac-imessage-top-level-state.edn")
          source  (reify isaac.comm.imessage.inbox/MessageSource
                    (-messages-since [_ _]
                      [{:message-rowid 42 :from-me? false :text "hello"}]))
          result  (sut/poll-inbound! source path)]
      (should= [{:message-rowid 42 :from-me? false :text "hello"}] (:messages result))
      (should= {:message-rowid 42} (get-in result [:state :watermark]))
      (should= {:message-rowid 42} (:watermark (sut/read-state path))))))
