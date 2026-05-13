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
              :interval-ms 2500}
             (sut/parse-args ["loop"
                              "--isaac-home" "/tmp/isaac-home"
                              "--interval-ms" "2500"])))

  (it "defaults to once mode when no mode is given"
    (should= :once (:mode (sut/parse-args ["--isaac-home" "/tmp/isaac-home"]))))

  (it "runs one cycle through the poller"
    (with-redefs [isaac.comm.imessage.poller/run-once! (fn [opts]
                                                         (should= {:isaac-home "/tmp/isaac-home"
                                                                   :db-path "/tmp/chat.db"
                                                                   :state-path "/tmp/state.edn"
                                                                   :interval-ms 1000}
                                                                  opts)
                                                         {:ok true})]
      (should= {:ok true}
               (sut/run-poller! {:mode :once
                                 :isaac-home "/tmp/isaac-home"
                                 :db-path "/tmp/chat.db"
                                 :state-path "/tmp/state.edn"
                                 :interval-ms 1000}))))

  (it "starts the loop through the poller"
    (with-redefs [isaac.comm.imessage.poller/start! (fn [opts]
                                                      (should= {:isaac-home "/tmp/isaac-home"
                                                                :db-path "/tmp/chat.db"
                                                                :state-path "/tmp/state.edn"
                                                                :interval-ms 2500}
                                                               opts)
                                                      {:running? (atom true)})]
      (should-not-be-nil
        (sut/run-poller! {:mode :loop
                          :isaac-home "/tmp/isaac-home"
                          :db-path "/tmp/chat.db"
                          :state-path "/tmp/state.edn"
                          :interval-ms 2500})))))
