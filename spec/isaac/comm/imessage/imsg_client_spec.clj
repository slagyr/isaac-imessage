(ns isaac.comm.imessage.imsg-client-spec
  (:require
    [cheshire.core :as json]
    [clojure.string :as str]
    [isaac.comm.imessage.imsg-client :as sut]
    [isaac.util.jsonrpc :as jrpc]
    [speclj.core :refer :all])
  (:import
    (java.io ByteArrayOutputStream PipedInputStream PipedOutputStream
             OutputStreamWriter BufferedReader InputStreamReader)
    (java.nio.charset StandardCharsets)))

(defn- fake-process
  "Build a fake process with piped streams. Returns
   {:proc <as expected by sut/start!>
    :feed-line! (fn [line]) - feed a line of stdout to the client
    :written  - StringBuilder accumulating what the client wrote to stdin}"
  []
  (let [out->client     (PipedOutputStream.)
        client-stdout   (PipedInputStream. out->client 4096)
        client-stdin    (ByteArrayOutputStream.)
        written-lines   (atom [])
        proc            (reify sut/Subprocess
                          (-stdout-reader [_] (BufferedReader. (InputStreamReader. client-stdout StandardCharsets/UTF_8)))
                          (-stdin-writer  [_] (OutputStreamWriter. client-stdin StandardCharsets/UTF_8))
                          (-alive? [_] true)
                          (-destroy [_] (.close out->client)))
        feed!           (fn [line]
                          (let [bytes (.getBytes (str line "\n") StandardCharsets/UTF_8)]
                            (.write out->client bytes)
                            (.flush out->client)))
        snapshot-written (fn []
                            (let [s (.toString client-stdin StandardCharsets/UTF_8)]
                              (->> (str/split s #"\n")
                                   (remove str/blank?)
                                   (mapv #(json/parse-string % true)))))]
    {:proc proc
     :feed! feed!
     :written snapshot-written}))

(describe "imsg-client"

  (context "request!"

    (it "returns a future that resolves when a matching result arrives"
      (let [{:keys [proc feed!]} (fake-process)
            client                (sut/start! {:process proc})
            f                     (sut/request! client "chats.list" {})]
        (feed! (json/generate-string {:jsonrpc "2.0" :id 1 :result {:ok true}}))
        (should= {:ok true} (deref f 1000 ::timeout))
        (sut/stop! client)))

    (it "correlates multiple in-flight requests by id"
      (let [{:keys [proc feed!]} (fake-process)
            client                (sut/start! {:process proc})
            f1                    (sut/request! client "a" {})
            f2                    (sut/request! client "b" {})]
        (feed! (json/generate-string {:jsonrpc "2.0" :id 2 :result "two"}))
        (feed! (json/generate-string {:jsonrpc "2.0" :id 1 :result "one"}))
        (should= "two" (deref f2 1000 ::timeout))
        (should= "one" (deref f1 1000 ::timeout))
        (sut/stop! client)))

    (it "writes a JSON-RPC request line on the subprocess stdin"
      (let [{:keys [proc written]} (fake-process)
            client                  (sut/start! {:process proc})]
        (sut/request! client "send" {:to "x" :text "y"})
        (Thread/sleep 50)
        (let [[msg] (written)]
          (should= "2.0" (:jsonrpc msg))
          (should= "send" (:method msg))
          (should= {:to "x" :text "y"} (:params msg))
          (should (some? (:id msg))))
        (sut/stop! client)))

    (it "rejects the future on subprocess close"
      (let [{:keys [proc]} (fake-process)
            client          (sut/start! {:process proc})
            f               (sut/request! client "send" {})]
        (sut/stop! client)
        (let [v (deref f 1000 ::timeout)]
          (should-not= ::timeout v)
          (should (instance? Throwable v))))))

  (context "disconnect"

    (it "fires the on-disconnect callback when the subprocess closes unexpectedly"
      (let [fired (atom false)
            {:keys [proc]} (fake-process)
            client (sut/start! {:process proc
                                 :on-disconnect #(reset! fired true)})]
        ;; Close the stdout side from the test's perspective — reader sees EOF.
        (sut/-destroy proc)
        (.join ^Thread (:reader-thread client) 1000)
        (should @fired)))

    (it "does NOT fire on-disconnect when stop! is called explicitly"
      (let [fired (atom false)
            {:keys [proc]} (fake-process)
            client (sut/start! {:process proc
                                 :on-disconnect #(reset! fired true)})]
        (sut/stop! client)
        (should-not @fired))))

  (context "notifications"

    (it "routes :method-only messages to the notification handler"
      (let [received (atom [])
            {:keys [proc feed!]} (fake-process)
            client (sut/start! {:process proc
                                 :on-notification #(swap! received conj %)})]
        (feed! (json/generate-string {:jsonrpc "2.0"
                                       :method "message"
                                       :params {:text "hi"}}))
        (Thread/sleep 50)
        (should= [{:method "message" :params {:text "hi"}}] @received)
        (sut/stop! client)))

    (it "ignores notifications when no handler is configured"
      (let [{:keys [proc feed!]} (fake-process)
            client (sut/start! {:process proc})]
        (feed! (json/generate-string {:jsonrpc "2.0" :method "message" :params {}}))
        (Thread/sleep 50)
        (should (sut/alive? client))
        (sut/stop! client)))))
