(ns isaac.comm.imessage
  (:require
    [isaac.comm :as comm]
    [isaac.comm.imessage.apple-script :as apple-script]
    [isaac.comm.imessage.inbox :as inbox]
    [isaac.comm.imessage.state :as state]
    [isaac.configurator :as configurator]))

(defn- default-target [host slice record]
  (or (:target record)
      (:default-target slice)
      (:default-target host)))

(defn- delivery-request [record]
  {:message (:content record)
   :service (:service record)
   :target  (:target record)})

(defn send! [record]
  (apple-script/send-message! (delivery-request record)))

(defn send-message! [request]
  (apple-script/send-message! request))

(defn read-state [path]
  (state/read-state path))

(defn write-state! [path data]
  (state/write-state! path data))

(defn poll-inbound! [source path]
  (let [current (read-state path)
        result  (inbox/poll! source current)]
    (write-state! path (:state result))
    result))

(deftype ImessageComm [host state*]
  comm/Comm
  (on-turn-start [_ _ _] nil)
  (on-text-chunk [_ _ _] nil)
  (on-tool-call [_ _ _] nil)
  (on-tool-cancel [_ _ _] nil)
  (on-tool-result [_ _ _ _] nil)
  (on-compaction-start [_ _ _] nil)
  (on-compaction-success [_ _ _] nil)
  (on-compaction-failure [_ _ _] nil)
  (on-compaction-disabled [_ _ _] nil)
  (on-turn-end [_ _ _] nil)
  (send! [_ record]
    (let [slice   (:slice @state*)
          target  (default-target host slice record)
          service (or (:service record)
                      (:service slice)
                      (:service host))]
      (send! {:content (:content record)
              :service service
              :target  target})))

  configurator/Reconfigurable
  (on-startup! [_ slice]
    (reset! state* {:host host :slice slice :status :started}))
  (on-config-change! [_ old-slice new-slice]
    (if (nil? new-slice)
      (reset! state* {:host host :slice nil :status :stopped :prior old-slice})
      (swap! state* assoc :slice new-slice :status :changed :prior old-slice))))

(defn make [host]
  (->ImessageComm host (atom {:host host :slice nil :status :new})))

(defn imessage? [x]
  (instance? ImessageComm x))

(defn state [^ImessageComm comm]
  @(.-state* comm))
