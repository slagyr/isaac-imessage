(ns isaac.comm.imessage-spec
  (:require
    [isaac.comm.imessage :as sut]
    [isaac.comm.imessage.apple-script]
    [isaac.comm.imessage.chat-db]
    [isaac.comm.imessage.inbox]
    [isaac.comm.imessage.routing]
    [isaac.comm.imessage.state]
    [speclj.core :refer :all]))

(defn- temp-path [name]
  (str (System/getProperty "java.io.tmpdir") "/" name "-" (random-uuid) ".edn"))

(describe "iMessage scaffold"

  (it "loads the top-level namespace"
    (should-not-be-nil (find-ns 'isaac.comm.imessage)))

  (it "exposes the child namespaces"
    (should-not-be-nil (find-ns 'isaac.comm.imessage.apple-script))
    (should-not-be-nil (find-ns 'isaac.comm.imessage.chat-db))
    (should-not-be-nil (find-ns 'isaac.comm.imessage.inbox))
    (should-not-be-nil (find-ns 'isaac.comm.imessage.routing))
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
    (let [path    (temp-path "isaac-imessage-top-level-state")
          source  (reify isaac.comm.imessage.inbox/MessageSource
                    (-messages-since [_ _]
                      [{:message-rowid 42 :from-me? false :text "hello"}]))
          result  (sut/poll-inbound! source path)]
      (should= [{:message-rowid 42 :from-me? false :text "hello"}] (:messages result))
      (should= {:message-rowid 42} (get-in result [:state :watermark]))
      (should= {:message-rowid 42} (:watermark (sut/read-state path)))))

  (it "routes inbound messages onto persisted session keys"
    (let [path   (temp-path "isaac-imessage-route-state")
          source (reify isaac.comm.imessage.inbox/MessageSource
                   (-messages-since [_ _]
                     [{:message-rowid 42
                       :thread-id      "chat-guid-1"
                       :handle         "+15551234567"
                       :from-me?       false
                       :text           "hello"}]))
          result (sut/poll-routed! source path)]
      (should= [{:message-rowid 42
                 :thread-id      "chat-guid-1"
                 :handle         "+15551234567"
                 :from-me?       false
                 :text           "hello"
                 :session-key    "imessage:chat-guid-1"}]
               (:messages result))
      (should= "imessage:chat-guid-1"
               (get-in (sut/read-state path) [:threads "chat-guid-1" :session-key]))))

  (it "does not replay already-watermarked messages on a second poll"
    (let [path   (temp-path "isaac-imessage-watermark-state")
          source (reify isaac.comm.imessage.inbox/MessageSource
                   (-messages-since [_ watermark]
                     (let [all [{:message-rowid 42
                                 :thread-id      "chat-guid-1"
                                 :handle         "+15551234567"
                                 :from-me?       false
                                 :text           "hello"}]
                           cutoff (get watermark :message-rowid 0)]
                       (filter #(> (:message-rowid %) cutoff) all))))]
      (should= 1 (count (:messages (sut/poll-routed! source path))))
      (should= [] (:messages (sut/poll-routed! source path))))))
