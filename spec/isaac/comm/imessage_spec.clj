(ns isaac.comm.imessage-spec
  (:require
    [clojure.string :as str]
    [isaac.api]
    [isaac.comm.imessage :as sut]
    [isaac.comm.imessage.imsg-client]
    [speclj.core :refer :all]))

(defn- fake-client [calls]
  (reify isaac.comm.imessage.imsg-client/Client
    (-request! [_ method params]
      (swap! calls conj {:method method :params params})
      (doto (promise) (deliver {:ok true})))
    (-notify! [_ _ _] nil)
    (-stop!   [_] nil)
    (-alive?-client [_] true)))

(describe "iMessage outbound translation"

  (it "translates a delivery record into imsg send params (lowercased service)"
    (let [calls (atom [])]
      (should= {:ok true}
               (sut/send! (fake-client calls)
                          {:content "hello"
                           :service "iMessage"
                           :target  "+15551234567"}))
      (should= [{:method "send"
                 :params {:to "+15551234567" :text "hello" :service "imessage"}}]
               @calls)))

  (it "omits :service when the record has none"
    (let [calls (atom [])]
      (sut/send! (fake-client calls)
                 {:content "hi" :target "+15551234567"})
      (should= {:to "+15551234567" :text "hi"}
               (:params (first @calls))))))

(describe "iMessage inbound filter (notification->work-item)"

  (let [allow ["+15551234567" "friend@icloud.com"]
        slice (delay {:allow-from allow})
        notif (fn [overrides]
                {:method "message"
                 :params {:subscription 1
                          :message (merge {:id 1
                                           :chat_guid "T1"
                                           :sender "+15551234567"
                                           :text "hi"
                                           :is_from_me false
                                           :created_at "2026-05-20T00:00:00Z"}
                                          overrides)}})]

    (it "produces a work-item for an allowed inbound message"
      (let [item (sut/notification->work-item @slice (notif {}))]
        (should= "imessage:T1" (:session-key item))
        (should= "hi" (:input item))
        (should= {:kind          :imessage
                  :chat-guid     "T1"
                  :handle        "+15551234567"
                  :message-rowid 1
                  :sent-at       "2026-05-20T00:00:00Z"}
                 (:origin item))))

    (it "drops self-sent messages"
      (should= nil (sut/notification->work-item @slice (notif {:is_from_me true}))))

    (it "drops senders not in allow-from"
      (should= nil (sut/notification->work-item @slice (notif {:sender "+15559999999"}))))

    (it "drops everything when allow-from is empty (fail-closed)"
      (should= nil (sut/notification->work-item {:allow-from []} (notif {}))))

    (it "passes everything when allow-from is missing (no filter)"
      (let [item (sut/notification->work-item {} (notif {:sender "+15559999999"}))]
        (should= "+15559999999" (get-in item [:origin :handle]))))

    (it "ignores notifications with a method other than \"message\""
      (should= nil (sut/notification->work-item @slice {:method "error" :params {}})))

    (it "drops messages with no chat identity"
      (should= nil (sut/notification->work-item @slice (notif {:chat_guid nil :chat_identifier nil}))))))

(describe "iMessage dispatch-request"

  (it "embeds a trusted inbound_meta block in :soul-prepend"
    (let [req (sut/dispatch-request
                {:session-key "imessage:T1"
                 :input       "hi"
                 :origin      {:kind :imessage :chat-guid "T1" :handle "+15551234567"}})]
      (should (string? (:soul-prepend req)))
      (should (str/includes? (:soul-prepend req) "isaac.inbound_meta.v1"))
      (should (str/includes? (:soul-prepend req) "\"chat_guid\":\"T1\""))
      (should (str/includes? (:soul-prepend req) "\"handle\":\"+15551234567\""))
      (should (str/includes? (:soul-prepend req) "\"provider\":\"imessage\""))
      (should (str/includes? (:soul-prepend req) "\"was_mentioned\":false"))))

  (it "instructs the LLM to keep iMessage replies brief"
    (let [req (sut/dispatch-request
                {:session-key "imessage:T1"
                 :input       "hi"
                 :origin      {:kind :imessage :chat-guid "T1" :handle "+15551234567"}})]
      (should (str/includes? (:soul-prepend req) "iMessage"))
      (should (str/includes? (:soul-prepend req) "brief")))))

(describe "iMessage cap-chunks"

  (it "passes through when chunks ≤ max-chunks"
    (should= ["a" "b" "c"] (sut/cap-chunks ["a" "b" "c"] 3))
    (should= ["a" "b"]     (sut/cap-chunks ["a" "b"] 3)))

  (it "passes through when max-chunks is nil"
    (should= ["a" "b" "c" "d"] (sut/cap-chunks ["a" "b" "c" "d"] nil)))

  (it "drops the tail and replaces with a notice when over the cap"
    (let [result (sut/cap-chunks ["a" "b" "c" "d" "e"] 3)]
      (should= 3 (count result))
      (should= ["a" "b"] (vec (take 2 result)))
      (should (str/includes? (nth result 2) "3 more chunk"))
      (should (str/includes? (nth result 2) "truncated"))))

  (it "returns empty when max-chunks is 0"
    (should= [] (sut/cap-chunks ["a" "b"] 0))))

(describe "iMessage reply text"

  (it "reads the assistant content from the dispatch result"
    (should= "hello back"
             (sut/result->reply-text {:response {:message {:content "hello back"}}})))

  (it "falls back to :message :content if no :response wrapper"
    (should= "hello back"
             (sut/result->reply-text {:message {:content "hello back"}})))

  (it "returns empty string when nothing matches"
    (should= "" (sut/result->reply-text {:other :shape}))))

(describe "iMessage chunking"

  (it "leaves short text alone"
    (should= ["hello back"] (sut/chunk-reply-text "hello back" 20)))

  (it "splits on whitespace boundaries when possible"
    (should= ["one two" "three" "four five"]
             (sut/chunk-reply-text "one two three four five" 10)))

  (it "hard-splits a long token when no whitespace boundary fits"
    (should= ["abcdefghij" "klm"]
             (sut/chunk-reply-text "abcdefghijklm" 10))))
