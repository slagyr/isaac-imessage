(ns isaac.comm.imessage.main-spec
  (:require
    [isaac.comm.imessage.main :as sut]
    [speclj.core :refer :all]))

(describe "iMessage main"

  (it "parses once mode with explicit paths"
    (should= {:mode :once
              :isaac-home "/tmp/isaac-home"
              :db-path "/tmp/chat.db"
              :state-path "/tmp/state.edn"
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
              :service "E:me"
              :interval-ms 1000}
             (sut/parse-args ["--isaac-home" "/tmp/isaac-home"
                              "--service" "E:me"])))

  (it "defaults to once mode when no mode is given"
    (should= :once (:mode (sut/parse-args ["--isaac-home" "/tmp/isaac-home"]))))

  (it "runs one cycle through the poller"
    (with-redefs [isaac.comm.imessage/drain-once-and-reply! (fn [isaac-home db-path state-path service]
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
                                  :service "E:me"
                                  :interval-ms 1000}))))

  (it "starts the loop through the poller"
    (with-redefs [isaac.comm.imessage/drain-once-and-reply! (fn [isaac-home db-path state-path service]
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
                          :service "E:me"
                          :interval-ms 2500})))))
