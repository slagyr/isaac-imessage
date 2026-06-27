(ns isaac.server.imessage-app-spec
  (:require
    [clojure.edn :as edn]
    [clojure.java.io :as io]
    [isaac.comm.imessage :as imessage]
    [isaac.comm.imessage.imsg-client :as imsg-client]
    [isaac.config.change-source :as change-source]
    [isaac.fs :as fs]
    [isaac.logger :as log]
    [isaac.nexus :as nexus]
    [isaac.server.app :as sut]
    [isaac.spec-helper :as helper]
    [speclj.core :refer :all]))

(defn- imessage-module-index []
  (when-let [manifest (some-> (io/resource "isaac-manifest.edn") slurp edn/read-string)]
    {:isaac.comm.imessage {:coord {} :manifest manifest :path nil}}))

(defn- imessage-modules []
  {:isaac.comm.imessage {:local/root (System/getProperty "user.dir")}})

(defn- cfg-with-imessage [cfg]
  (assoc cfg :module-index (imessage-module-index)))

(defn- config-edn [body]
  (pr-str (merge {:modules (imessage-modules)} body)))

(defn- live-imessage []
  (nexus/get-in [:comms :imessage]))

(defn- stub-request! [response]
  (fn [_ _ _]
    (doto (promise) (deliver response))))

(describe "Server app — iMessage integration"

  (helper/with-captured-logs)

  (after (sut/stop!))

  (it "activates imessage comm on startup when comms.imessage config is present"
    (with-redefs [imsg-client/start! (fn [_] (throw (ex-info "should not spawn without db-path" {})))]
      (sut/start! {:port               0
                   :root               "/tmp/isaac-imessage"
                   :cfg                (cfg-with-imessage {:comms {:imessage {:imessage/service "iMessage"}}})
                   :start-http-server? false})
      (helper/await-condition #(some? (live-imessage)) 6000)
      (should (imessage/imessage? (live-imessage)))
      (should (some #(and (= :comm/activated (:event %))
                          (= "imessage" (:comm %)))
                    @log/captured-logs))
      (sut/stop!)))

  (it "does not spawn imsg client on startup when db-path is absent"
    (let [started (atom false)]
      (with-redefs [imsg-client/start! (fn [_] (reset! started true) ::client)]
        (sut/start! {:port               0
                     :root               "/tmp/isaac-imessage"
                     :cfg                (cfg-with-imessage {:comms {:imessage {:imessage/service "iMessage"}}})
                     :start-http-server? false})
        (helper/await-condition #(some? (live-imessage)) 6000)
        (sut/stop!))
      (should= false @started)))

  (it "spawns imsg client on startup when db-path is configured"
    (let [started (atom nil)
          db-path "/tmp/isaac-imessage-chat.db"]
      (.createNewFile (java.io.File. db-path))
      (with-redefs [imsg-client/start!  (fn [opts] (reset! started opts) ::client)
                    imsg-client/request! (stub-request! {:subscription 1})]
        (sut/start! {:port               0
                     :root               "/tmp/isaac-imessage-db"
                     :cfg                (cfg-with-imessage
                                         {:comms {:imessage {:imessage/service "iMessage"
                                                             :imessage/db-path db-path}}})
                     :start-http-server? false})
        (helper/await-condition #(some? @started) 6000)
        (should= db-path (:db-path @started))
        (sut/stop!))))

  (it "spawns imsg through a configured wrapper command without a local db-path file"
    (let [started (atom nil)
          command ["ssh" "-T" "zane@mac" "/usr/local/bin/imsg"]]
      (with-redefs [imsg-client/start!  (fn [opts] (reset! started opts) ::client)
                    imsg-client/request! (stub-request! {:subscription 1})]
        (sut/start! {:port               0
                     :root               "/tmp/isaac-imessage-wrap"
                     :cfg                (cfg-with-imessage
                                         {:comms {:imessage {:imessage/service  "iMessage"
                                                             :imessage/db-path  "/Users/zane/Library/Messages/chat.db"
                                                             :imessage/command command}}})
                     :start-http-server? false})
        (helper/await-condition #(some? @started) 6000)
        (should= command (:command @started))
        (sut/stop!))))

  (it "updates the live comm slice when config changes via hot-reload"
    (let [source (change-source/memory-source "/tmp/isaac-imessage-reload/.isaac")
          mem    (fs/mem-fs)]
      (nexus/-with-nexus {:fs mem}
        (fs/mkdirs mem "/tmp/isaac-imessage-reload/.isaac/config")
        (fs/spit mem "/tmp/isaac-imessage-reload/.isaac/config/isaac.edn"
                 (config-edn {:comms {:imessage {:imessage/service     "iMessage"
                                                 :imessage/message-cap 2000}}}))
        (with-redefs [imsg-client/start! (fn [_] nil)]
          (sut/start! {:cfg                  (cfg-with-imessage {:comms {:imessage {:imessage/service     "iMessage"
                                                                                   :imessage/message-cap 2000}}})
                       :config-change-source source
                       :fs                   mem
                       :root                 "/tmp/isaac-imessage-reload/.isaac"
                       :port                 0
                       :start-http-server?   false})
          (fs/spit mem "/tmp/isaac-imessage-reload/.isaac/config/isaac.edn"
                   (config-edn {:comms {:imessage {:imessage/service     "iMessage"
                                                   :imessage/message-cap 500}}}))
          (change-source/notify-path! source "/tmp/isaac-imessage-reload/.isaac/config/isaac.edn")
          (helper/await-condition
            #(= 500 (get-in (imessage/state (live-imessage)) [:slice :imessage/message-cap]))
            6000)
          (helper/await-condition
            #(= 500 (get-in (sut/current-config) [:comms :imessage :imessage/message-cap]))
            6000)
          (sut/stop!)))))

  (it "stops imsg client and removes comm when slot is deleted via config hot-reload"
    (let [source  (change-source/memory-source "/tmp/isaac-imessage-remove/.isaac")
          stopped (atom nil)
          db-path "/tmp/isaac-imessage-remove-chat.db"]
      (.createNewFile (java.io.File. db-path))
      (let [mem (fs/mem-fs)]
       (nexus/-with-nexus {:fs mem}
        (fs/mkdirs mem "/tmp/isaac-imessage-remove/.isaac/config")
        (fs/spit mem "/tmp/isaac-imessage-remove/.isaac/config/isaac.edn"
                 (config-edn {:comms {:imessage {:imessage/service "iMessage"
                                                 :imessage/db-path db-path}}}))
        (with-redefs [imsg-client/start!  (fn [_] ::client)
                      imsg-client/stop!   (fn [client] (reset! stopped client))
                      imsg-client/request! (stub-request! {:subscription 1})]
          (sut/start! {:cfg                  (cfg-with-imessage {:comms {:imessage {:imessage/service "iMessage"
                                                                                   :imessage/db-path db-path}}})
                       :config-change-source source
                       :fs                   mem
                       :root                 "/tmp/isaac-imessage-remove/.isaac"
                       :port                 0
                       :start-http-server?   false})
          (helper/await-condition #(some? (live-imessage)) 6000)
          (fs/spit mem "/tmp/isaac-imessage-remove/.isaac/config/isaac.edn"
                   (config-edn {:comms {}}))
          (change-source/notify-path! source "/tmp/isaac-imessage-remove/.isaac/config/isaac.edn")
          (helper/await-condition #(nil? (live-imessage)) 6000)
          (sut/stop!))))
      (should= ::client @stopped))))