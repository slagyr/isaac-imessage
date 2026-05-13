(ns isaac.comm.imessage-integration-spec
  (:require
    [isaac.comm :as comm]
    [isaac.comm.imessage :as sut]
    [speclj.core :refer :all]))

(describe "iMessage Isaac integration"

  (it "builds a Comm/Reconfigurable instance"
    (let [instance (sut/make {:name "imessage-slot" :service "E:me"})]
      (should (sut/imessage? instance))
      (should (satisfies? comm/Comm instance))))

  (it "uses configured defaults when delivering a record"
    (let [instance (sut/make {:name "imessage-slot" :service "E:me" :default-target "+15551234567"})]
      (with-redefs [isaac.comm.imessage.apple-script/send-message!
                    (fn [request]
                      (should= {:message "hello"
                                :service "E:me"
                                :target "+15551234567"}
                               request)
                      {:ok true})]
        (should= {:ok true}
                 (comm/send! instance {:content "hello"})))))

  (it "prefers per-record target and service over defaults"
    (let [instance (sut/make {:name "imessage-slot" :service "E:me" :default-target "+15551234567"})]
      (with-redefs [isaac.comm.imessage.apple-script/send-message!
                    (fn [request]
                      (should= {:message "hello"
                                :service "E:other"
                                :target "+15550000000"}
                               request)
                      {:ok true})]
        (should= {:ok true}
                 (comm/send! instance {:content "hello"
                                       :service "E:other"
                                       :target "+15550000000"}))))))
