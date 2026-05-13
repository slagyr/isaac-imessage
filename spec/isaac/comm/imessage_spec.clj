(ns isaac.comm.imessage-spec
  (:require
    [isaac.api]
    [isaac.comm.imessage :as sut]
    [isaac.comm.imessage.apple-script]
    [isaac.comm.imessage.chat-db]
    [isaac.comm.imessage.inbox]
    [isaac.comm.imessage.main]
    [isaac.comm.imessage.poller]
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
    (should-not-be-nil (find-ns 'isaac.comm.imessage.main))
    (should-not-be-nil (find-ns 'isaac.comm.imessage.poller))
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
      (should= [] (:messages (sut/poll-routed! source path)))))

  (it "builds dispatch-ready work items from routed messages"
    (let [path   (temp-path "isaac-imessage-work-items")
          source (reify isaac.comm.imessage.inbox/MessageSource
                   (-messages-since [_ _]
                     [{:message-rowid 42
                       :thread-id      "chat-guid-1"
                       :handle         "+15551234567"
                       :from-me?       false
                       :text           "hello"
                       :sent-at        1234567890}]))
          result (sut/poll-work-items! source path)]
      (should= [{:session-key "imessage:chat-guid-1"
                 :input       "hello"
                 :origin      {:kind          :imessage
                               :thread-id     "chat-guid-1"
                               :handle        "+15551234567"
                               :message-rowid 42
                               :sent-at       1234567890}}]
               (:work-items result))
      (should= {:message-rowid 42}
               (get-in result [:state :watermark]))))

  (it "returns no work items when a second poll sees nothing new"
    (let [path   (temp-path "isaac-imessage-work-items-watermark")
          source (reify isaac.comm.imessage.inbox/MessageSource
                   (-messages-since [_ watermark]
                     (let [all [{:message-rowid 42
                                 :thread-id      "chat-guid-1"
                                 :handle         "+15551234567"
                                 :from-me?       false
                                 :text           "hello"
                                 :sent-at        1234567890}]
                           cutoff (get watermark :message-rowid 0)]
                        (filter #(> (:message-rowid %) cutoff) all))))]
      (should= 1 (count (:work-items (sut/poll-work-items! source path))))
      (should= [] (:work-items (sut/poll-work-items! source path)))))

  (it "builds an Isaac dispatch request from a work item"
    (should= {:session-key "imessage:chat-guid-1"
              :input       "hello"
              :origin      {:kind          :imessage
                            :thread-id     "chat-guid-1"
                            :handle        "+15551234567"
                            :message-rowid 42
                            :sent-at       1234567890}}
             (sut/dispatch-request {:session-key "imessage:chat-guid-1"
                                    :input       "hello"
                                    :origin      {:kind          :imessage
                                                  :thread-id     "chat-guid-1"
                                                  :handle        "+15551234567"
                                                  :message-rowid 42
                                                  :sent-at       1234567890}})))

  (it "creates a session before dispatch when one does not exist"
    (let [calls (atom [])
          work-item {:session-key "imessage:chat-guid-1"
                     :input       "hello"
                     :origin      {:kind :imessage :thread-id "chat-guid-1" :handle "+15551234567"}}]
      (with-redefs [isaac.api/get-session (fn [_state-dir _session-key] nil)
                    isaac.api/create-session! (fn [state-dir session-key opts]
                                                (swap! calls conj [:create state-dir session-key opts])
                                                {:id session-key})
                    isaac.api/dispatch! (fn [state-dir request]
                                          (swap! calls conj [:dispatch state-dir request])
                                          {:ok true})]
        (should= {:ok true}
                 (sut/dispatch-work-item! "/tmp/isaac-home" work-item))
        (should= [[:create "/tmp/isaac-home"
                   "imessage:chat-guid-1"
                   {:origin {:kind :imessage :thread-id "chat-guid-1" :handle "+15551234567"}
                    :chatType "direct"
                    :channel "imessage"}]
                  [:dispatch "/tmp/isaac-home"
                   {:session-key "imessage:chat-guid-1"
                    :input "hello"
                    :origin {:kind :imessage :thread-id "chat-guid-1" :handle "+15551234567"}}]]
                 @calls))))

  (it "dispatches without creating a session when one already exists"
    (let [calls (atom [])
          work-item {:session-key "imessage:chat-guid-1"
                     :input       "hello"
                     :origin      {:kind :imessage :thread-id "chat-guid-1" :handle "+15551234567"}}]
      (with-redefs [isaac.api/get-session (fn [_state-dir _session-key] {:id "imessage:chat-guid-1"})
                    isaac.api/create-session! (fn [& _]
                                                (swap! calls conj :create)
                                                nil)
                    isaac.api/dispatch! (fn [state-dir request]
                                          (swap! calls conj [:dispatch state-dir request])
                                          {:ok true})]
        (should= {:ok true}
                 (sut/dispatch-work-item! "/tmp/isaac-home" work-item))
        (should= [[:dispatch "/tmp/isaac-home"
                   {:session-key "imessage:chat-guid-1"
                    :input "hello"
                    :origin {:kind :imessage :thread-id "chat-guid-1" :handle "+15551234567"}}]]
                 @calls))))

  (it "dispatches a batch of work items in order"
    (let [calls (atom [])
          items [{:session-key "imessage:chat-1" :input "hello" :origin {:kind :imessage}}
                 {:session-key "imessage:chat-2" :input "there" :origin {:kind :imessage}}]]
      (with-redefs [sut/dispatch-work-item! (fn [state-dir item]
                                              (swap! calls conj [state-dir (:session-key item)])
                                              {:session-key (:session-key item) :ok true})]
        (should= [{:session-key "imessage:chat-1" :ok true}
                  {:session-key "imessage:chat-2" :ok true}]
                 (sut/dispatch-work-items! "/tmp/isaac-home" items))
        (should= [["/tmp/isaac-home" "imessage:chat-1"]
                  ["/tmp/isaac-home" "imessage:chat-2"]]
                 @calls))))

  (it "formats a successful assistant message result as reply text"
    (should= "hello back"
             (sut/result->reply-text {:message {:content "hello back"}})))

  (it "formats a bridge command result as reply text"
    (should= "effort set to 5"
             (sut/result->reply-text {:message "effort set to 5"})))

  (it "formats an error result as reply text"
    (should= "boom"
             (sut/result->reply-text {:error :exception :message "boom"})))

  (it "builds an outbound reply record from a work item and result"
    (should= {:content "hello back"
              :service "E:me"
              :target "+15551234567"}
             (sut/reply-record {:session-key "imessage:chat-1"
                                :origin {:kind :imessage :handle "+15551234567"}}
                               {:message {:content "hello back"}}
                               "E:me")))

  (it "does not chunk short reply text"
    (should= ["hello back"]
             (sut/chunk-reply-text "hello back" 20)))

  (it "chunks long reply text on whitespace boundaries when possible"
    (should= ["one two" "three" "four five"]
             (sut/chunk-reply-text "one two three four five" 10)))

  (it "hard-splits a long token when no whitespace boundary fits"
    (should= ["abcdefghij" "klm"]
             (sut/chunk-reply-text "abcdefghijklm" 10)))

  (it "dispatches a work item and sends the formatted reply"
    (let [calls (atom [])
          item  {:session-key "imessage:chat-1"
                 :input "hello"
                 :origin {:kind :imessage :handle "+15551234567"}}]
      (with-redefs [sut/dispatch-work-item! (fn [state-dir work-item]
                                              (swap! calls conj [:dispatch state-dir work-item])
                                              {:message {:content "hello back"}})
                    sut/send!               (fn [record]
                                              (swap! calls conj [:send record])
                                              {:ok true})]
        (should= {:dispatch-result {:message {:content "hello back"}}
                  :delivery-results [{:ok true}]}
                 (sut/dispatch-and-reply-work-item! "/tmp/isaac-home" item "E:me"))
        (should= [[:dispatch "/tmp/isaac-home" item]
                  [:send {:content "hello back"
                          :service "E:me"
                          :target "+15551234567"}]]
                 @calls))))

  (it "dispatches a work item and sends multiple reply chunks when needed"
    (let [calls (atom [])
          item  {:session-key "imessage:chat-1"
                 :input "hello"
                 :origin {:kind :imessage :handle "+15551234567"}}]
      (with-redefs [sut/dispatch-work-item! (fn [_state-dir _work-item]
                                              {:message {:content "one two three four five"}})
                    sut/send!               (fn [record]
                                              (swap! calls conj record)
                                              {:ok true})]
        (should= {:dispatch-result {:message {:content "one two three four five"}}
                  :delivery-results [{:ok true} {:ok true} {:ok true}]}
                 (sut/dispatch-and-reply-work-item! "/tmp/isaac-home" item "E:me" 10))
        (should= [{:content "one two"
                   :service "E:me"
                   :target "+15551234567"}
                  {:content "three"
                   :service "E:me"
                   :target "+15551234567"}
                  {:content "four five"
                   :service "E:me"
                   :target "+15551234567"}]
                 @calls))))

  (it "drains, dispatches, and replies for a full cycle"
    (let [item {:session-key "imessage:chat-1"
                :input "hello"
                :origin {:kind :imessage :handle "+15551234567"}}]
      (with-redefs [sut/poll-work-items-from-db! (fn [db-path state-path]
                                                   {:db-path db-path
                                                    :state-path state-path
                                                    :state {:watermark {:message-rowid 42}}
                                                    :work-items [item]})
                    sut/dispatch-and-reply-work-item! (fn [state-dir work-item service]
                                                        (should= "/tmp/isaac-home" state-dir)
                                                        (should= item work-item)
                                                        (should= "E:me" service)
                                                        {:dispatch-result {:message {:content "hello back"}}
                                                         :delivery-results [{:ok true}]})]
        (should= {:db-path "/tmp/chat.db"
                  :state-path "/tmp/state.edn"
                  :state {:watermark {:message-rowid 42}}
                  :work-items [item]
                  :results [{:dispatch-result {:message {:content "hello back"}}
                             :delivery-results [{:ok true}]}]}
                 (sut/drain-once-and-reply! "/tmp/isaac-home" "/tmp/chat.db" "/tmp/state.edn" "E:me")))))

  (it "drains one polling cycle from chat db into dispatched Isaac turns"
    (with-redefs [sut/poll-work-items-from-db! (fn [db-path state-path]
                                                 {:db-path db-path
                                                  :state-path state-path
                                                  :work-items [{:session-key "imessage:chat-1"
                                                                :input "hello"
                                                                :origin {:kind :imessage}}]
                                                  :state {:watermark {:message-rowid 42}}})
                  sut/dispatch-work-items! (fn [state-dir items]
                                             (should= "/tmp/isaac-home" state-dir)
                                             (should= [{:session-key "imessage:chat-1"
                                                        :input "hello"
                                                        :origin {:kind :imessage}}]
                                                      items)
                                             [{:session-key "imessage:chat-1" :ok true}])]
      (should= {:db-path "/tmp/chat.db"
                :state-path "/tmp/state.edn"
                :state {:watermark {:message-rowid 42}}
                :work-items [{:session-key "imessage:chat-1"
                              :input "hello"
                              :origin {:kind :imessage}}]
                :results [{:session-key "imessage:chat-1" :ok true}]}
               (sut/drain-once! "/tmp/isaac-home" "/tmp/chat.db" "/tmp/state.edn"))))

  (it "drains one cycle with default db and state paths"
    (with-redefs [sut/default-chat-db-path (fn [] "/Users/micah/Library/Messages/chat.db")
                  sut/default-state-path   (fn [] "/Users/micah/.isaac/imessage/state.edn")
                  sut/poll-work-items-from-db! (fn [db-path state-path]
                                                 {:db-path db-path
                                                  :state-path state-path
                                                  :work-items []
                                                  :state {:watermark nil}})
                  sut/dispatch-work-items! (fn [state-dir items]
                                             (should= "/tmp/isaac-home" state-dir)
                                             (should= [] items)
                                             [])]
      (should= {:db-path "/Users/micah/Library/Messages/chat.db"
                :state-path "/Users/micah/.isaac/imessage/state.edn"
                :state {:watermark nil}
                :work-items []
                :results []}
               (sut/drain-once! "/tmp/isaac-home")))))
