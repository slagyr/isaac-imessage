(ns isaac.comm.imessage-spec
  (:require
    [clojure.edn :as edn]
    [clojure.java.io :as io]
    [clojure.string :as str]
    [isaac.api]
    [isaac.comm.delivery.queue :as queue]
    [isaac.comm.imessage :as sut]
    [isaac.comm.imessage.imsg-client :as imsg-client]
    [isaac.comm.protocol :as comm]
    [isaac.logger :as log]
    [isaac.reconfigurable :as reconfigurable]
    [speclj.core :refer :all]))

(defn- fake-client [calls]
  (reify isaac.comm.imessage.imsg-client/Client
    (-request! [_ method params]
      (swap! calls conj {:method method :params params})
      (doto (promise) (deliver {:ok true})))
    (-notify! [_ _ _] nil)
    (-stop!   [_] nil)
    (-alive?-client [_] true)))

(describe "imsg RPC error detail"

  (it "prefers rpc-error :data over the generic Internal error message"
    (let [err (ex-info "Internal error"
                         {:type      :imsg/error
                          :rpc-error {:code    -32603
                                      :message "Internal error"
                                      :data    "Permission Error: cannot open /Users/zane/Library/Messages/chat.db — grant Full Disk Access"}})]
      (should (str/includes? (sut/-imsg-error-message err) "Full Disk Access"))
      (should (str/includes? (sut/-imsg-error-message err) "chat.db"))))

  (it "falls back to rpc-error :message when :data is absent"
    (let [err (ex-info "Method not found"
                         {:type      :imsg/error
                          :rpc-error {:code -32601 :message "Method not found"}})]
      (should= "Method not found" (sut/-imsg-error-message err))))

  (it "reads :detail out of a structured rpc-error :data map"
    (let [err (ex-info "Delivery outcome unknown"
                         {:type      :imsg/error
                          :rpc-error {:code    -32001
                                      :message "Delivery outcome unknown"
                                      :data    {:operation    "send"
                                                :disposition  "may_have_completed"
                                                :transport    "applescript"
                                                :retry_safe   false
                                                :detail       "Messages automation returned success, but no matching outgoing text row was observed within 8 seconds."}}})]
      (should= "Messages automation returned success, but no matching outgoing text row was observed within 8 seconds."
               (sut/-imsg-error-message err))))

  (it "falls back to the rpc-error :message when structured :data carries no :detail"
    (let [err (ex-info "Delivery outcome unknown"
                         {:type      :imsg/error
                          :rpc-error {:code    -32001
                                      :message "Delivery outcome unknown"
                                      :data    {:retry_safe false}}})]
      (should= "Delivery outcome unknown" (sut/-imsg-error-message err))))

  (it "logs subscribe failure with rpc detail and slice context"
    (let [calls  (atom [])
          client (reify isaac.comm.imessage.imsg-client/Client
                   (-request! [_ method params]
                     (swap! calls conj {:method method :params params})
                     (if (= "watch.subscribe" method)
                       (doto (promise)
                         (deliver (ex-info "Internal error"
                                           {:type      :imsg/error
                                            :rpc-error {:code    -32603
                                                        :message "Internal error"
                                                        :data    "Permission Error: grant Full Disk Access"}})))
                       (doto (promise) (deliver {:ok true}))))
                   (-notify! [_ _ _] nil)
                   (-stop!   [_] nil)
                   (-alive?-client [_] true))
          slice  {:imessage/service "iMessage"
                  :imessage/db-path "/Users/zane/Library/Messages/chat.db"
                  :imessage/bin     "/usr/local/bin/imsg"}]
      (log/capture-logs
        (reconfigurable/on-load (sut/make {:name "imessage" :imsg-client client}) slice))
      (let [entry (first (filter #(= :imsg.watch/subscribe-failed (:event %)) @log/captured-logs))]
        (should (some? entry))
        (should (str/includes? (:error entry) "Full Disk Access"))
        (should= -32603 (:rpc-code entry))
        (should= "/Users/zane/Library/Messages/chat.db" (:imessage/db-path entry))
        (should= "/usr/local/bin/imsg" (:imessage/bin entry)))))

  (it "skips imsg spawn when db-path is missing on disk"
    (let [started (atom false)]
      (with-redefs [imsg-client/start! (fn [_] (reset! started true) ::client)]
        (log/capture-logs
          (reconfigurable/on-load
            (sut/make {:name "imessage"})
            {:imessage/service "iMessage"
             :imessage/db-path "/no/such/chat.db"})))
      (should= false @started)
      (should (some #(= :imsg.client/db-path-unavailable (:event %)) @log/captured-logs)))

  (it "spawns through a wrapper command without checking db-path on the local disk"
    (let [started (atom nil)]
      (with-redefs [imsg-client/start! (fn [opts] (reset! started opts) ::client)
                    imsg-client/request! (fn [_ _ _] (doto (promise) (deliver {:subscription 1})))]
        (reconfigurable/on-load
          (sut/make {:name "imessage"})
          {:imessage/service  "iMessage"
           :imessage/db-path  "/Users/zane/Library/Messages/chat.db"
           :imessage/command ["ssh" "-T" "zane@mac" "/usr/local/bin/imsg"]}))
      (should= ["ssh" "-T" "zane@mac" "/usr/local/bin/imsg"]
               (:command @started))
      (should= "/Users/zane/Library/Messages/chat.db" (:db-path @started)))))

  (it "logs subscribe failure with command context when a wrapper is configured"
    (let [calls  (atom [])
          client (reify isaac.comm.imessage.imsg-client/Client
                   (-request! [_ method params]
                     (swap! calls conj {:method method :params params})
                     (if (= "watch.subscribe" method)
                       (doto (promise)
                         (deliver (ex-info "boom" {:type :imsg/error :rpc-error {:message "boom"}})))
                       (doto (promise) (deliver {:ok true}))))
                   (-notify! [_ _ _] nil)
                   (-stop!   [_] nil)
                   (-alive?-client [_] true))
          slice  {:imessage/service  "iMessage"
                  :imessage/db-path  "/Users/zane/Library/Messages/chat.db"
                  :imessage/command ["ssh" "-T" "zane@mac" "/usr/local/bin/imsg"]}]
      (log/capture-logs
        (reconfigurable/on-load (sut/make {:name "imessage" :imsg-client client}) slice))
      (let [entry (first (filter #(= :imsg.watch/subscribe-failed (:event %)) @log/captured-logs))]
        (should= ["ssh" "-T" "zane@mac" "/usr/local/bin/imsg"] (:imessage/command entry))))))

(describe "iMessage delivery record shape"

  (it "declares namespaced :send-schema on the manifest"
    (let [manifest (edn/read-string (slurp (io/resource "isaac-manifest.edn")))
          schema   (get-in manifest [:isaac.agent/comm :imessage :send-schema])]
      (should= #{:imessage/target :imessage/service} (set (keys schema)))))

  (it "offers auto/sms/imessage as the send-record :service values"
    (let [manifest (edn/read-string (slurp (io/resource "isaac-manifest.edn")))
          field    (get-in manifest [:isaac.agent/comm :imessage :send-schema :imessage/service])]
      (should= [[:one-of? "auto" "sms" "imessage"]] (:validations field))))

  (it "documents auto — not iMessage — as the working service default"
    (let [manifest    (edn/read-string (slurp (io/resource "isaac-manifest.edn")))
          description (get-in manifest [:isaac.agent/comm :imessage :extra-schema
                                        :imessage/service :description])]
      (should (str/includes? description "auto"))
      (should-not (str/includes? description "Defaults to iMessage"))))

  (it "enqueues reply chunks with :imessage/target"
    (let [captured (atom [])]
      (with-redefs [sut/dispatch-work-item! (fn [_ _ _]
                                              {:response {:message {:content "hello back"}}})
                    queue/enqueue!           (fn [record]
                                               (swap! captured conj record)
                                               record)]
        (sut/dispatch-and-enqueue-reply!
          "/test"
          {:origin {:handle "+15551234567"}}
          nil))
      (should= 1 (count @captured))
      (should= {:comm "imessage" :imessage/target "+15551234567" :content "hello back"}
               (select-keys (first @captured) [:comm :imessage/target :content])))))

(describe "iMessage outbound translation"

  (it "honours :retry_safe false in the error :data as a permanent failure"
    (let [err (ex-info "Delivery outcome unknown"
                         {:type      :imsg/error
                          :rpc-error {:code    -32001
                                      :message "Delivery outcome unknown"
                                      :data    {:operation   "send"
                                                :disposition "may_have_completed"
                                                :transport   "applescript"
                                                :retry_safe  false
                                                :detail      "Messages automation returned success, but no matching outgoing text row was observed within 8 seconds."}}})]
      (should= {:ok false :transient? false :error (sut/-imsg-error-message err)}
               (sut/-classify-imsg-error err))))

  (it "never retries a \"may_have_completed\" disposition"
    (let [err (ex-info "Delivery outcome unknown"
                         {:type      :imsg/error
                          :rpc-error {:code -32001
                                      :message "Delivery outcome unknown"
                                      :data {:disposition "may_have_completed"}}})]
      (should= false (:transient? (sut/-classify-imsg-error err)))))

  (it "retries when the error :data says the send is retry-safe"
    (let [err (ex-info "Messages is not running"
                         {:type      :imsg/error
                          :rpc-error {:code -32001
                                      :message "Messages is not running"
                                      :data {:disposition "did_not_send" :retry_safe true}}})]
      (should= true (:transient? (sut/-classify-imsg-error err)))))

  (it "falls back to the message regex when the error carries no structured :data"
    (let [permanent (ex-info "Internal error"
                             {:type      :imsg/error
                              :rpc-error {:code -32603 :message "invalid handle: +1555"}})
          transient* (ex-info "Internal error"
                              {:type      :imsg/error
                               :rpc-error {:code -32603 :message "connection reset"}})]
      (should= false (:transient? (sut/-classify-imsg-error permanent)))
      (should= true  (:transient? (sut/-classify-imsg-error transient*)))))

  (it "classifies permission errors surfaced in rpc-error :data as permanent"
    (let [err (ex-info "Internal error"
                         {:type      :imsg/error
                          :rpc-error {:code    -32603
                                      :message "Internal error"
                                      :data    "Permission Error: grant Full Disk Access to read chat.db"}})]
      (should= {:ok false :transient? false :error (sut/-imsg-error-message err)}
               (sut/-classify-imsg-error err))))

  (it "translates a delivery record into imsg send params (lowercased service)"
    (let [calls (atom [])]
      (should= {:ok true}
               (sut/send! (fake-client calls)
                          {:content           "hello"
                           :imessage/service  "iMessage"
                           :imessage/target   "+15551234567"}))
      (should= [{:method "send"
                 :params {:to "+15551234567" :text "hello" :service "imessage"}}]
               @calls)))

  (it "omits :service when the record has none"
    (let [calls (atom [])]
      (sut/send! (fake-client calls)
                 {:content "hi" :imessage/target "+15551234567"})
      (should= {:to "+15551234567" :text "hi"}
               (:params (first @calls))))))

;; Shared fixtures for the inbound filter. Top-level, not a `let` inside
;; `describe`: a let body yields only its last form, so every `it` but the
;; last silently vanishes from the suite.
(def ^:private inbound-slice {:imessage/allow-from ["+15551234567" "friend@icloud.com"]})

(defn- notif [overrides]
  {:method "message"
   :params {:subscription 1
            :message      (merge {:id         1
                                  :chat_guid  "T1"
                                  :sender     "+15551234567"
                                  :text       "hi"
                                  :is_from_me false
                                  :created_at "2026-05-20T00:00:00Z"}
                                 overrides)}})

(describe "iMessage inbound filter (notification->work-item)"

  (it "produces a work-item for an allowed inbound message"
    (let [item (sut/notification->work-item inbound-slice (notif {}))]
      (should= "imessage:T1" (:session-key item))
      (should= "hi" (:input item))
      (should= {:kind          :imessage
                :chat-guid     "T1"
                :handle        "+15551234567"
                :message-rowid 1
                :sent-at       "2026-05-20T00:00:00Z"}
               (:origin item))))

  (it "drops self-sent messages"
    (should= nil (sut/notification->work-item inbound-slice (notif {:is_from_me true}))))

  (it "drops senders not in allow-from"
    (should= nil (sut/notification->work-item inbound-slice (notif {:sender "+15559999999"}))))

  (it "drops everything when allow-from is empty (fail-closed)"
    (should= nil (sut/notification->work-item {:imessage/allow-from []} (notif {}))))

  (it "passes everything when allow-from is missing (no filter)"
    (let [item (sut/notification->work-item {} (notif {:sender "+15559999999"}))]
      (should= "+15559999999" (get-in item [:origin :handle]))))

  (it "ignores notifications with a method other than \"message\""
    (should= nil (sut/notification->work-item inbound-slice {:method "error" :params {}})))

  (it "drops messages with no chat identity"
    (should= nil (sut/notification->work-item inbound-slice (notif {:chat_guid nil :chat_identifier nil}))))

  (it "drops every inbound message on a send-only comm"
    (should= nil (sut/notification->work-item
                   (assoc inbound-slice :imessage/inbound? false)
                   (notif {}))))

  (it "keeps producing work-items when inbound? is explicitly true"
    (should= "imessage:T1"
             (:session-key (sut/notification->work-item
                             (assoc inbound-slice :imessage/inbound? true)
                             (notif {}))))))

(defn- subscribed? [calls]
  (boolean (some #(= "watch.subscribe" (:method %)) @calls)))

(describe "send-only iMessage comm (isaac-k00m)"

  (it "does not subscribe to the watch when inbound? is false"
    (let [calls  (atom [])
          client (fake-client calls)]
      (log/capture-logs
        (reconfigurable/on-load (sut/make {:name "imessage" :imsg-client client})
                                {:imessage/inbound? false}))
      (should= false (subscribed? calls))
      (should (some #(= :imsg.watch/send-only (:event %)) @log/captured-logs))))

  (it "subscribes to the watch when inbound? is absent"
    (let [calls  (atom [])
          client (fake-client calls)]
      (reconfigurable/on-load (sut/make {:name "imessage" :imsg-client client}) {})
      (should= true (subscribed? calls))))

  (it "subscribes to the watch when inbound? is true"
    (let [calls  (atom [])
          client (fake-client calls)]
      (reconfigurable/on-load (sut/make {:name "imessage" :imsg-client client})
                              {:imessage/inbound? true})
      (should= true (subscribed? calls))))

  (it "still sends outbound records when inbound? is false"
    (let [calls  (atom [])
          client (fake-client calls)
          comm   (sut/make {:name "imessage" :imsg-client client})]
      (reconfigurable/on-load comm {:imessage/inbound? false})
      (should= {:ok true} (comm/send! comm {:imessage/target "friend@icloud.com"
                                            :content         "Still sending."}))
      (should= [{:to "friend@icloud.com" :text "Still sending."}]
               (->> @calls (filter #(= "send" (:method %))) (mapv :params)))))

  (it "declares :imessage/inbound? on the comm's config schema"
    (let [manifest (edn/read-string (slurp (io/resource "isaac-manifest.edn")))
          schema   (get-in manifest [:isaac.agent/comm :imessage :extra-schema :imessage/inbound?])]
      (should= :boolean (:type schema))
      (should (string? (:description schema))))))

(describe "iMessage dispatch-input"

  (it "embeds a trusted inbound_meta block in :soul-prepend"
    (let [req (sut/dispatch-input
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
    (let [req (sut/dispatch-input
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
