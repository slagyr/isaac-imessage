(ns isaac.comm.imessage.poller-runtime-spec
  (:require
    [isaac.comm.imessage.poller :as sut]
    [speclj.core :refer :all]))

(describe "iMessage poller runtime"

  (it "runs one drain cycle through the configured drain function"
    (let [calls (atom [])]
      (should= {:ok true}
               (sut/run-once! {:isaac-home "/tmp/isaac-home"
                               :db-path "/tmp/chat.db"
                               :state-path "/tmp/state.edn"
                               :drain-fn (fn [isaac-home db-path state-path]
                                           (swap! calls conj [isaac-home db-path state-path])
                                           {:ok true})}))
      (should= [["/tmp/isaac-home" "/tmp/chat.db" "/tmp/state.edn"]]
               @calls)))

  (it "starts a loop that repeatedly drains until stopped"
    (let [calls     (atom 0)
          release*  (promise)
          runner    (sut/start! {:isaac-home "/tmp/isaac-home"
                                 :db-path "/tmp/chat.db"
                                 :state-path "/tmp/state.edn"
                                 :drain-fn (fn [_ _ _]
                                             (let [n (swap! calls inc)]
                                               (when (= 2 n)
                                                 (deliver release* true))
                                               {:iteration n}))
                                 :sleep-fn (fn [_] nil)})]
      (should= true (deref release* 1000 nil))
      (sut/stop! runner)
      (should (<= 2 @calls))))

  (it "returns the last drain result"
    (let [runner (sut/start! {:isaac-home "/tmp/isaac-home"
                              :db-path "/tmp/chat.db"
                              :state-path "/tmp/state.edn"
                              :drain-fn (fn [_ _ _] {:ok true :count 1})
                              :sleep-fn (fn [_] nil)})]
      (Thread/sleep 10)
      (sut/stop! runner)
      (should= {:ok true :count 1} @(-> runner :last-result)))))
