(ns isaac.comm.imessage.apple-script-spec
  (:require
    [isaac.comm.imessage.apple-script :as sut]
    [speclj.core :refer :all]))

(describe "AppleScript sender"

  (it "escapes quotes and backslashes in message text"
    (should= "hello \\\"Micah\\\" \\\\o/"
             (#'sut/escape-applescript-string "hello \"Micah\" \\o/")))

  (it "builds a Messages send script for a target and service"
    (should= (str "tell application \"Messages\"\n"
                  "  send \"hello\" to buddy \"+15551234567\" of service \"E:me\"\n"
                  "end tell")
             (sut/build-script {:message "hello"
                                :service "E:me"
                                :target "+15551234567"})))

  (it "returns ok true when osascript exits successfully"
    (with-redefs [sut/run-command (fn [_] {:exit 0 :out "" :err ""})]
      (should= {:ok true}
               (sut/send-message! {:message "hello"
                                   :service "E:me"
                                   :target "+15551234567"}))))

  (it "treats automation permission failures as permanent"
    (with-redefs [sut/run-command (fn [_] {:exit 1 :out "" :err "Not authorized to send Apple events to Messages."})]
      (should= {:ok false :transient? false :error :not-authorized}
               (sut/send-message! {:message "hello"
                                   :service "E:me"
                                   :target "+15551234567"}))))

  (it "treats Messages-not-running failures as transient"
    (with-redefs [sut/run-command (fn [_] {:exit 1 :out "" :err "Application isn’t running."})]
      (should= {:ok false :transient? true :error :messages-unavailable}
               (sut/send-message! {:message "hello"
                                   :service "E:me"
                                   :target "+15551234567"})))))
