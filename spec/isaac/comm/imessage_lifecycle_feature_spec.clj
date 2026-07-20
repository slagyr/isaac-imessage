(ns isaac.comm.imessage-lifecycle-feature-spec
  (:require
    [isaac.comm.imessage :as imessage]
    [isaac.comm.imessage.imessage-steps :as steps]
    [isaac.comm.registry :as comm-registry]
    [isaac.configurator-steps :as cfg-steps]
    [isaac.nexus :as nexus]
    [isaac.spec-helper :as helper]
    [speclj.core :refer :all]))

(helper/with-captured-logs)

(defn- apply-config! [rows]
  ;; Prefer server-config-applied (current), fall back to configure (legacy pin).
  (let [apply-fn (or (requiring-resolve 'isaac.server.server-steps/server-config-applied)
                     (requiring-resolve 'isaac.server.server-steps/configure))]
    (apply-fn {:headers ["key" "value"]
               :rows    rows})))

(describe "lifecycle feature wiring"
  (after ((requiring-resolve 'isaac.server.app/stop!)))

  (it "registers the comm in comm-registry for delivery on server start"
    (steps/imessage-lifecycle-setup)
    (apply-config! [["comms.imessage.imessage/service" "iMessage"]])
    (steps/imessage-isaac-server-started)
    (should (some? (nexus/get-in [:comms :imessage])))
    (should (some? (comm-registry/comm-for "imessage"))))

  (it "hot-reload removes comm when slot deleted"
    (steps/imessage-lifecycle-setup)
    (apply-config! [["comms.imessage.imessage/service" "iMessage"]])
    (steps/imessage-isaac-server-started)
    (should (some? (nexus/get-in [:comms :imessage])))
    (cfg-steps/config-updated
      {:headers ["path" "value"]
       :rows [["comms.imessage" "#delete"]]})
    (should-be-nil (nexus/get-in [:comms :imessage]))
    (should-be-nil (comm-registry/comm-for "imessage")))

  (it "hot-reload updates message-cap in place"
    (steps/imessage-lifecycle-setup)
    (apply-config! [["comms.imessage.imessage/service" "iMessage"]
                    ["comms.imessage.imessage/message-cap" "2000"]])
    (steps/imessage-isaac-server-started)
    (cfg-steps/config-updated
      {:headers ["path" "value"]
       :rows [["comms.imessage.imessage/message-cap" "500"]]})
    (should= 500 (get-in (imessage/state (nexus/get-in [:comms :imessage])) [:slice :imessage/message-cap]))))
