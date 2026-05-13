(ns isaac.comm.imessage.main-spec
  (:require
    [isaac.comm.imessage.main :as sut]
    [isaac.comm.imessage.config]
    [speclj.core :refer :all]))

(describe "iMessage main"

  (it "parses once mode with explicit paths"
    (should= {:mode :once
              :isaac-home "/tmp/isaac-home"
              :db-path "/tmp/chat.db"
              :state-path "/tmp/state.edn"
              :config-path nil
              :service nil
              :interval-ms 1000}
             (sut/parse-args ["once"
                              "--isaac-home" "/tmp/isaac-home"
                              "--db-path" "/tmp/chat.db"
                              "--state-path" "/tmp/state.edn"])))

  (it "parses loop mode with custom interval"
    (should= {:mode :loop
              :isaac-home "/tmp/isaac-home"
              :db-path nil
              :state-path nil
              :config-path nil
              :service nil
              :interval-ms 2500}
             (sut/parse-args ["loop"
                              "--isaac-home" "/tmp/isaac-home"
                              "--interval-ms" "2500"])))

  (it "parses an explicit outbound service"
    (should= {:mode :once
              :isaac-home "/tmp/isaac-home"
              :db-path nil
              :state-path nil
              :config-path nil
              :service "E:me"
              :interval-ms 1000}
             (sut/parse-args ["--isaac-home" "/tmp/isaac-home"
                              "--service" "E:me"])))

  (it "parses inspect mode"
    (should= {:mode :inspect
              :isaac-home "/tmp/isaac-home"
              :db-path nil
              :state-path nil
              :config-path nil
              :service nil
              :interval-ms 1000}
             (sut/parse-args ["inspect"
                              "--isaac-home" "/tmp/isaac-home"])))

  (it "parses an explicit config path"
    (should= "/tmp/imessage.edn"
             (:config-path (sut/parse-args ["--config-path" "/tmp/imessage.edn"]))))

  (it "defaults to once mode when no mode is given"
    (should= :once (:mode (sut/parse-args ["--isaac-home" "/tmp/isaac-home"]))))

  (it "runs one cycle through the poller"
    (with-redefs [isaac.comm.imessage.config/load-config (fn [_] {:service "E:file" :interval-ms 2500})
                  isaac.comm.imessage.config/default-config-path (fn [_] "/tmp/imessage.edn")
                  isaac.comm.imessage/drain-once-and-reply! (fn [isaac-home db-path state-path service]
                                                               (should= ["/tmp/isaac-home" "/tmp/chat.db" "/tmp/state.edn" "E:me"]
                                                                        [isaac-home db-path state-path service])
                                                               {:ok true})
                  isaac.comm.imessage.poller/run-once! (fn [opts]
                                                          (should= "/tmp/isaac-home" (:isaac-home opts))
                                                          (should= "/tmp/chat.db" (:db-path opts))
                                                          (should= "/tmp/state.edn" (:state-path opts))
                                                          (should= 1000 (:interval-ms opts))
                                                          (should= {:ok true} ((:drain-fn opts) "/tmp/isaac-home" "/tmp/chat.db" "/tmp/state.edn"))
                                                          {:ok true})]
      (should= {:ok true}
                (sut/run-poller! {:mode :once
                                  :isaac-home "/tmp/isaac-home"
                                  :db-path "/tmp/chat.db"
                                  :state-path "/tmp/state.edn"
                                  :config-path nil
                                  :service "E:me"
                                  :interval-ms 1000}))))

  (it "starts the loop through the poller"
    (with-redefs [isaac.comm.imessage.config/load-config (fn [_] {:service "E:file" :interval-ms 2500})
                  isaac.comm.imessage.config/default-config-path (fn [_] "/tmp/imessage.edn")
                  isaac.comm.imessage/drain-once-and-reply! (fn [isaac-home db-path state-path service]
                                                               (should= ["/tmp/isaac-home" "/tmp/chat.db" "/tmp/state.edn" "E:me"]
                                                                        [isaac-home db-path state-path service])
                                                               {:ok true})
                  isaac.comm.imessage.poller/start! (fn [opts]
                                                      (should= "/tmp/isaac-home" (:isaac-home opts))
                                                      (should= "/tmp/chat.db" (:db-path opts))
                                                      (should= "/tmp/state.edn" (:state-path opts))
                                                      (should= 2500 (:interval-ms opts))
                                                      (should= {:ok true} ((:drain-fn opts) "/tmp/isaac-home" "/tmp/chat.db" "/tmp/state.edn"))
                                                      {:running? (atom true)})]
      (should-not-be-nil
        (sut/run-poller! {:mode :loop
                          :isaac-home "/tmp/isaac-home"
                          :db-path "/tmp/chat.db"
                          :state-path "/tmp/state.edn"
                          :config-path nil
                          :service "E:me"
                          :interval-ms 2500})))))

  (it "uses config file defaults when CLI values are omitted"
    (with-redefs [isaac.comm.imessage.config/default-config-path (fn [_] "/tmp/imessage.edn")
                  isaac.comm.imessage.config/load-config (fn [_] {:service "E:file" :interval-ms 2500})
                  isaac.comm.imessage.poller/run-once! (fn [opts]
                                                         (should= "E:file" (:service opts))
                                                         (should= 2500 (:interval-ms opts))
                                                         {:ok true})]
       (should= {:ok true}
                (sut/run-poller! {:mode :once
                                  :isaac-home "/tmp/isaac-home"
                                  :db-path "/tmp/chat.db"
                                  :state-path "/tmp/state.edn"
                                  :config-path nil
                                  :service nil
                                  :interval-ms nil}))))

  (it "uses default db and state paths when they are omitted"
    (with-redefs [isaac.comm.imessage.config/default-config-path (fn [_] "/tmp/imessage.edn")
                  isaac.comm.imessage.config/load-config (fn [_] {:service "E:file" :interval-ms 2500})
                  isaac.comm.imessage/default-chat-db-path (fn [] "/Users/micah/Library/Messages/chat.db")
                  isaac.comm.imessage/default-state-path (fn [] "/Users/micah/.isaac/imessage/state.edn")
                  isaac.comm.imessage/drain-once-and-reply! (fn [isaac-home db-path state-path service]
                                                              (should= ["/tmp/isaac-home"
                                                                         "/Users/micah/Library/Messages/chat.db"
                                                                         "/Users/micah/.isaac/imessage/state.edn"
                                                                         "E:file"]
                                                                       [isaac-home db-path state-path service])
                                                              {:ok true})
                  isaac.comm.imessage.poller/run-once! (fn [opts]
                                                         (should= "/Users/micah/Library/Messages/chat.db" (:db-path opts))
                                                         (should= "/Users/micah/.isaac/imessage/state.edn" (:state-path opts))
                                                         (should= {:ok true} ((:drain-fn opts) "/tmp/isaac-home" (:db-path opts) (:state-path opts)))
                                                         {:ok true})]
      (should= {:ok true}
               (sut/run-poller! {:mode :once
                                 :isaac-home "/tmp/isaac-home"
                                 :db-path nil
                                 :state-path nil
                                 :config-path nil
                                 :service nil
                                 :interval-ms nil}))))

  (it "runs inspect mode without dispatching or sending"
    (with-redefs [isaac.comm.imessage.config/default-config-path (fn [_] "/tmp/imessage.edn")
                  isaac.comm.imessage.config/load-config (fn [_] {:service "E:file" :interval-ms 2500})
                  isaac.comm.imessage/default-chat-db-path (fn [] "/Users/micah/Library/Messages/chat.db")
                  isaac.comm.imessage/default-state-path (fn [] "/Users/micah/.isaac/imessage/state.edn")
                  isaac.comm.imessage/inspect-work-items-from-db! (fn [db-path state-path service]
                                                                    (should= ["/Users/micah/Library/Messages/chat.db"
                                                                               "/Users/micah/.isaac/imessage/state.edn"
                                                                               "E:file"]
                                                                             [db-path state-path service])
                                                                    {:work-items [{:session-key "imessage:chat-1"}]
                                                                     :reply-preview [{:content "hello"}]})]
      (should= {:work-items [{:session-key "imessage:chat-1"}]
                :reply-preview [{:content "hello"}]}
               (sut/run-poller! {:mode :inspect
                                 :isaac-home "/tmp/isaac-home"
                                 :db-path nil
                                 :state-path nil
                                 :config-path nil
                                 :service nil
                                 :interval-ms nil}))))
