(ns isaac.comm.imessage.routing-spec
  (:require
    [isaac.comm.imessage.routing :as sut]
    [speclj.core :refer :all]))

(describe "iMessage routing"

  (it "builds a deterministic session key from a chat guid"
    (should= "imessage:chat-guid-1"
             (sut/session-key-for-chat "chat-guid-1")))

  (it "reuses an existing mapped session for a known chat"
    (let [state {:chats {"chat-guid-1" {:handle "+15551234567"
                                        :session-key "existing-session"}}
                 :watermark nil}]
      (should= {:session-key "existing-session"
                :state       state}
               (sut/ensure-session state "chat-guid-1" "+15551234567"))))

  (it "creates and stores a session key for an unknown chat"
    (should= {:session-key "imessage:chat-guid-1"
              :state       {:chats {"chat-guid-1" {:handle "+15551234567"
                                                   :session-key "imessage:chat-guid-1"}}
                            :watermark nil}}
             (sut/ensure-session {:chats {} :watermark nil}
                                 "chat-guid-1"
                                 "+15551234567"))))
