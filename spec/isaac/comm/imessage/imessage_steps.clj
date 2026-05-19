(ns isaac.comm.imessage.imessage-steps
  (:require
    [gherclj.core :as g :refer [defgiven defwhen defthen helper!]]
    [isaac.comm.delivery.worker :as worker]
    [isaac.comm.imessage :as imessage]
    [isaac.comm.imessage.apple-script :as apple-script]
    [isaac.comm.registry :as comm-registry]
    [isaac.configurator :as configurator]
    [isaac.fs :as fs]
    [isaac.session.session-steps :as session-steps]
    [isaac.step-tables :as match]
    [isaac.system :as system]))

(helper! isaac.comm.imessage.imessage-steps)

(def ^:private captured-runner-calls (atom []))

(defn- capturing-send-message! [request]
  (swap! captured-runner-calls conj request)
  {:ok true})

(defn default-imessage-setup []
  (session-steps/in-memory-state "target/test-state")
  (reset! captured-runner-calls [])
  (let [host     {:name "imessage" :service "iMessage"}
        instance (imessage/make host)]
    (configurator/on-startup! instance {:service "iMessage"})
    (comm-registry/register-instance! "imessage" instance)
    (g/assoc! :imessage-instance instance)))

(defn imessage-delivery-worker-ticks []
  (g/assoc! :isaac-file-phase :assert)
  (let [runtime-state-dir (str (g/get :state-dir) "/.isaac")]
    (g/assoc! :runtime-state-dir runtime-state-dir)
    (binding [fs/*fs* (or (g/get :mem-fs) fs/*fs*)]
      (with-redefs [apple-script/send-message! capturing-send-message!]
        (system/with-system {:state-dir runtime-state-dir}
          (worker/tick! {}))))))

(defn runner-was-invoked-with [table]
  (let [calls  (mapv (fn [call]
                       {:service (:service call)
                        :buddy   (:target call)
                        :body    (:message call)})
                     @captured-runner-calls)
        result (match/match-entries table calls)]
    (g/should= [] (:failures result))))

(defwhen "the imessage delivery worker ticks" isaac.comm.imessage.imessage-steps/imessage-delivery-worker-ticks
  "Runs worker/tick! against the iMessage-registered comm-registry,
   bypassing the stub-only comm rebinding used by the generic
   'delivery worker ticks' step.")

(defgiven "default iMessage setup" isaac.comm.imessage.imessage-steps/default-imessage-setup
  "Sets up an in-memory state dir, registers the imessage Comm impl
   under the 'imessage' name, and stubs apple-script/send-message! to
   capture invocations instead of shelling out to osascript.")

(defthen "the imessage runner was invoked with:" isaac.comm.imessage.imessage-steps/runner-was-invoked-with
  "Asserts that at least one captured apple-script invocation matches
   the table. Captured request keys :message and :target are aliased
   to :body and :buddy so the table can read in iMessage vocabulary.")
