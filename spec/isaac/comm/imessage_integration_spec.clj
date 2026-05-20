(ns isaac.comm.imessage-integration-spec
  (:require
    [isaac.comm :as comm]
    [isaac.comm.imessage :as sut]
    [isaac.comm.imessage.imsg-client :as imsg-client]
    [isaac.configurator :as configurator]
    [speclj.core :refer :all]))

(defn- fake-client+calls []
  (let [calls (atom [])
        client (reify imsg-client/Client
                 (-request! [_ method params]
                   (swap! calls conj {:method method :params params})
                   (doto (promise) (deliver {:ok true})))
                 (-notify! [_ _ _] nil)
                 (-stop!   [_] nil)
                 (-alive?-client [_] true))]
    [client calls]))

(describe "iMessage Isaac integration"

  (it "builds a Comm/Reconfigurable instance"
    (let [[client _] (fake-client+calls)
          instance   (sut/make {:name "imessage-slot" :service "E:me" :imsg-client client})]
      (should (sut/imessage? instance))
      (should (satisfies? comm/Comm instance))))

  (it "uses configured defaults when delivering a record"
    (let [[client calls] (fake-client+calls)
          instance       (sut/make {:name "imessage-slot"
                                    :service "E:me"
                                    :default-target "+15551234567"
                                    :imsg-client client})]
      (configurator/on-startup! instance {:service "E:me"})
      (should= {:ok true} (comm/send! instance {:content "hello"}))
      (should= [{:method "send"
                 :params {:to "+15551234567" :text "hello" :service "e:me"}}]
               (filterv #(= "send" (:method %)) @calls))))

  (it "prefers per-record target and service over defaults"
    (let [[client calls] (fake-client+calls)
          instance       (sut/make {:name "imessage-slot"
                                    :service "E:me"
                                    :default-target "+15551234567"
                                    :imsg-client client})]
      (configurator/on-startup! instance {:service "E:me"})
      (should= {:ok true} (comm/send! instance {:content "hello"
                                                :service "E:other"
                                                :target "+15550000000"}))
      (should= [{:method "send"
                 :params {:to "+15550000000" :text "hello" :service "e:other"}}]
               (filterv #(= "send" (:method %)) @calls)))))
