(ns isaac.comm.imessage.imessage-steps
  (:require
    [gherclj.core :as g :refer [defgiven defwhen defthen helper!]]
    [isaac.comm.delivery.worker :as worker]
    [isaac.comm.imessage :as imessage]
    [isaac.comm.imessage.apple-script :as apple-script]
    [isaac.comm.imessage.inbox :as inbox]
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

(defn- imessage-state-path []
  (str (g/get :state-dir) "/.isaac/imessage/state.edn"))

(defn- parse-row [headers row]
  (let [m (zipmap headers row)]
    {:message-rowid (parse-long (get m "rowid"))
     :thread-id     (get m "chat-guid")
     :handle        (get m "handle")
     :text          (get m "text")
     :from-me?      (pos? (or (parse-long (get m "from-me")) 0))
     :sent-at       (or (some-> (get m "sent-at") parse-long) 0)}))

(defn imessage-source-has-rows [table]
  (let [rows (mapv #(parse-row (:headers table) %) (:rows table))]
    (g/assoc! :imessage-rows rows)))

(defn- imessage-source []
  (let [rows (or (g/get :imessage-rows) [])]
    (reify inbox/MessageSource
      (-messages-since [_ watermark]
        (let [floor (:message-rowid watermark)]
          (if floor
            (vec (filter #(> (:message-rowid %) floor) rows))
            rows))))))

(defn imessage-inbox-is-polled []
  (binding [fs/*fs* (or (g/get :mem-fs) fs/*fs*)]
    (let [source (imessage-source)
          result (imessage/poll-work-items! source (imessage-state-path))]
      (g/assoc! :imessage-work-items (:work-items result))
      (g/assoc! :imessage-state      (:state result)))))

(defn polled-work-items-are [table]
  (let [items  (vec (g/get :imessage-work-items))
        result (match/match-entries table items)]
    (g/should= [] (:failures result))))

(defn imessage-watermark-is [n]
  (let [state (g/get :imessage-state)]
    (g/should= n (get-in state [:watermark :message-rowid]))))

(defn runner-was-invoked-with [table]
  (let [calls  (mapv (fn [call]
                       {:service (:service call)
                        :buddy   (:target call)
                        :body    (:message call)})
                     @captured-runner-calls)
        result (match/match-entries table calls)]
    (g/should= [] (:failures result))))

(defgiven "the imessage source has rows:" isaac.comm.imessage.imessage-steps/imessage-source-has-rows
  "Installs an in-memory inbox/MessageSource fed by the table. Row
   columns: rowid, chat-guid, handle, text, from-me (0/1), sent-at
   (optional, defaults to 0).")

(defwhen "the imessage inbox is polled" isaac.comm.imessage.imessage-steps/imessage-inbox-is-polled
  "Calls imessage/poll-work-items! against the source installed by
   'the imessage source has rows:' and the in-memory state path under
   <state-dir>/.isaac/imessage/state.edn. Captures :work-items and
   :state for subsequent Then assertions.")

(defthen "the polled work items are:" isaac.comm.imessage.imessage-steps/polled-work-items-are
  "Matches the captured :work-items against the table. Supports dotted
   header paths like origin.handle for nested keys.")

(defthen "the imessage watermark is {n:int}" isaac.comm.imessage.imessage-steps/imessage-watermark-is
  "Asserts the watermark message-rowid in the captured :state.")

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
