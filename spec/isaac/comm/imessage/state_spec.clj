(ns isaac.comm.imessage.state-spec
  (:require
    [isaac.comm.imessage.state :as sut]
    [speclj.core :refer :all]))

(describe "iMessage state"

  (it "returns the default state when the file is missing"
    (should= {:chats {} :watermark nil}
             (sut/read-state "/tmp/isaac-imessage-missing-state.edn")))

  (it "stores a chat to session mapping"
    (should= {:chats {"chat-1" {:handle "+15551234567"
                                 :session-key "imessage:chat-1"}}
              :watermark nil}
             (sut/assoc-chat-session {:chats {} :watermark nil}
                                     "chat-1"
                                     "+15551234567"
                                     "imessage:chat-1")))

  (it "stores a watermark"
    (should= {:chats {} :watermark {:message-rowid 42}}
             (sut/assoc-watermark {:chats {} :watermark nil}
                                  {:message-rowid 42})))

  (it "writes state and reads it back"
    (let [path  (str (System/getProperty "java.io.tmpdir") "/isaac-imessage-state-spec.edn")
          state {:chats {"chat-1" {:handle "+15551234567"
                                    :session-key "imessage:chat-1"}}
                 :watermark {:message-rowid 42}}]
      (sut/write-state! path state)
      (should= state (sut/read-state path)))))
