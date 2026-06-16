(ns isaac.comm.imessage-lifecycle-feature-spec
  (:require
    [isaac.comm.imessage :as imessage]
    [isaac.comm.imessage.imessage-steps :as steps]
    [isaac.configurator-steps :as cfg-steps]
    [isaac.nexus :as nexus]
    [isaac.spec-helper :as helper]
    [speclj.core :refer :all]))

(helper/with-captured-logs)

(describe "lifecycle feature wiring"
  (after ((requiring-resolve 'isaac.server.app/stop!)))

  (it "hot-reload removes comm when slot deleted"
    (steps/imessage-lifecycle-setup)
    ((requiring-resolve 'isaac.server.server-steps/configure)
      {:headers ["path" "value"]
       :rows [["comms.imessage.imessage/service" "iMessage"]]})
    (steps/imessage-isaac-server-started)
    (should (some? (nexus/get-in [:comms :imessage])))
    (cfg-steps/config-updated
      {:headers ["path" "value"]
       :rows [["comms.imessage" "#delete"]]})
    (should-be-nil (nexus/get-in [:comms :imessage])))

  (it "hot-reload updates message-cap in place"
    (steps/imessage-lifecycle-setup)
    ((requiring-resolve 'isaac.server.server-steps/configure)
      {:headers ["path" "value"]
       :rows [["comms.imessage.imessage/service" "iMessage"]
              ["comms.imessage.imessage/message-cap" "2000"]]})
    (steps/imessage-isaac-server-started)
    (cfg-steps/config-updated
      {:headers ["path" "value"]
       :rows [["comms.imessage.imessage/message-cap" "500"]]})
    (should= 500 (get-in (imessage/state (nexus/get-in [:comms :imessage])) [:slice :imessage/message-cap]))))