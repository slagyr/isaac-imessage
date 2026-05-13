(ns isaac.comm.imessage.poller-spec
  (:require
    [isaac.comm.imessage :as sut]
    [speclj.core :refer :all]))

(describe "iMessage poller"

  (it "defaults the Messages chat db path under the user home"
    (binding [*assert* false]
      (let [home "/Users/micah"]
        (should= "/Users/micah/Library/Messages/chat.db"
                 (sut/default-chat-db-path home)))))

  (it "defaults the module state path under .isaac"
    (binding [*assert* false]
      (let [home "/Users/micah"]
        (should= "/Users/micah/.isaac/imessage/state.edn"
                 (sut/default-state-path home)))))

  (it "polls work items from a chat db path and persists state"
    (let [captured (atom nil)]
      (with-redefs [isaac.comm.imessage.chat-db/shell-store (fn [db-path]
                                                              (reset! captured {:db-path db-path})
                                                              :store)
                    isaac.comm.imessage.chat-db/message-source (fn [store]
                                                                 (assoc @captured :store store)
                                                                 :source)
                    isaac.comm.imessage/poll-work-items! (fn [source state-path]
                                                           {:work-items [{:session-key "imessage:chat-guid-1"
                                                                          :input "hello"
                                                                          :origin {:kind :imessage}}]
                                                            :state {:watermark {:message-rowid 42}}
                                                            :source source
                                                            :state-path state-path})]
        (let [result (sut/poll-work-items-from-db! "/tmp/chat.db" "/tmp/state.edn")]
          (should= [{:session-key "imessage:chat-guid-1"
                     :input "hello"
                     :origin {:kind :imessage}}]
                   (:work-items result))
          (should= {:message-rowid 42} (get-in result [:state :watermark]))
          (should= {:db-path "/tmp/chat.db"} @captured)
          (should= :source (:source result))
          (should= "/tmp/state.edn" (:state-path result)))))))
