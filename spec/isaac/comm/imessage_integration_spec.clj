(ns isaac.comm.imessage-integration-spec
  (:require
    [isaac.comm.imessage :as sut]
    [isaac.comm.imessage.imsg-client :as imsg-client]
    [isaac.comm.protocol :as comm]
    [isaac.reconfigurable :as reconfigurable]
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
          instance   (sut/make {:name "imessage-slot" :imsg-client client})]
      (should (sut/imessage? instance))
      (should (satisfies? comm/Comm instance))))

  (it "uses slice service when the record has none"
    (let [[client calls] (fake-client+calls)
          instance       (sut/make {:name "imessage-slot" :imsg-client client})]
      (reconfigurable/on-load instance {:imessage/service "E:me"})
      (should= {:ok true} (comm/send! instance {:content          "hello"
                                                 :imessage/target  "+15551234567"}))
      (should= [{:method "send"
                 :params {:to "+15551234567" :text "hello" :service "e:me"}}]
               (filterv #(= "send" (:method %)) @calls))))

  (it "prefers per-record service over slice service"
    (let [[client calls] (fake-client+calls)
          instance       (sut/make {:name "imessage-slot" :imsg-client client})]
      (reconfigurable/on-load instance {:imessage/service "E:me"})
      (should= {:ok true} (comm/send! instance {:content          "hello"
                                                :imessage/service "E:other"
                                                :imessage/target  "+15550000000"}))
      (should= [{:method "send"
                 :params {:to "+15550000000" :text "hello" :service "e:other"}}]
               (filterv #(= "send" (:method %)) @calls))))

  (it "fails loudly with permanent classification when :imessage/target is missing"
    (let [[client calls] (fake-client+calls)
          instance       (sut/make {:name "imessage-slot" :imsg-client client})]
      (reconfigurable/on-load instance {:imessage/service "E:me"})
      (let [result (comm/send! instance {:content "no target here"})]
        (should= false (:ok result))
        (should= false (:transient? result)))
      (should= [] (filterv #(= "send" (:method %)) @calls)))))
