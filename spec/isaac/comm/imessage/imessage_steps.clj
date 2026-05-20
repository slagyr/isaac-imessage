(ns isaac.comm.imessage.imessage-steps
  (:require
    [clojure.string :as str]
    [gherclj.core :as g :refer [defgiven defwhen defthen helper!]]
    [isaac.comm.delivery.worker :as worker]
    [isaac.comm.imessage :as imessage]
    [isaac.comm.imessage.apple-script :as apple-script]
    [isaac.comm.imessage.chat-db :as chat-db]
    [isaac.comm.imessage.inbox :as inbox]
    [isaac.comm.imessage.poller :as poller]
    [isaac.comm.imessage.state :as state]
    [isaac.comm.registry :as comm-registry]
    [isaac.configurator :as configurator]
    [isaac.fs :as fs]
    [isaac.session.session-steps :as session-steps]
    [isaac.step-tables :as match]
    [isaac.system :as system]))

(helper! isaac.comm.imessage.imessage-steps)

(def ^:private captured-runner-calls (atom []))
(def ^:private source-poll-count    (atom 0))
(def ^:private source-raise-budget  (atom 0))

(defn- capturing-send-message! [request]
  (swap! captured-runner-calls conj request)
  {:ok true})

(defn default-imessage-setup []
  (session-steps/in-memory-state "target/test-state")
  (reset! captured-runner-calls [])
  (reset! source-poll-count 0)
  (reset! source-raise-budget 0)
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
  (str (g/get :state-dir) "/.isaac/comms/imessage/state.edn"))

(defn- parse-row [headers row]
  (let [m (zipmap headers row)]
    {:message-rowid (parse-long (get m "rowid"))
     :chat-guid     (get m "chat-guid")
     :handle        (get m "handle")
     :text          (get m "text")
     :from-me?      (pos? (or (parse-long (get m "from-me")) 0))
     :sent-at       (or (some-> (get m "sent-at") parse-long) 0)}))

(defn imessage-source-has-rows [table]
  (let [rows (mapv #(parse-row (:headers table) %) (:rows table))]
    (g/assoc! :imessage-rows rows)))

(defn- imessage-source []
  (reify inbox/MessageSource
    (-messages-since [_ watermark]
      (swap! source-poll-count inc)
      (when (pos? @source-raise-budget)
        (swap! source-raise-budget dec)
        (throw (ex-info "source raised" {:reason :test})))
      (let [rows  (or (g/get :imessage-rows) [])
            floor (:message-rowid watermark)]
        (if floor
          (vec (filter #(> (:message-rowid %) floor) rows))
          rows)))))

(defn- imessage-slice []
  (or (some-> (g/get :imessage-instance) imessage/state :slice) {}))

(defn imessage-inbox-is-polled []
  (binding [fs/*fs* (or (g/get :mem-fs) fs/*fs*)]
    (let [source (imessage-source)
          opts   (select-keys (imessage-slice) [:allow-from])
          result (imessage/poll-work-items! source (imessage-state-path) opts)]
      (g/assoc! :imessage-work-items (:work-items result))
      (g/assoc! :imessage-state      (:state result)))))

(defn- update-imessage-slice! [updater]
  (when-let [instance (g/get :imessage-instance)]
    (configurator/on-config-change! instance
                                    (:slice (imessage/state instance))
                                    (updater (:slice (imessage/state instance))))))

(defn imessage-source-raises-then-succeeds [n]
  (reset! source-raise-budget n))

(defn imessage-poller-is-ticked-n-times [n]
  (binding [fs/*fs* (or (g/get :mem-fs) fs/*fs*)]
    (let [opts {:isaac-home "/fake/home"
                :db-path    "/fake/chat.db"
                :state-path (imessage-state-path)
                :drain-fn   (fn [_ _ state-path]
                              (let [source (imessage-source)
                                    result (imessage/poll-work-items! source state-path)]
                                (g/assoc! :imessage-work-items (:work-items result))
                                (g/assoc! :imessage-state      (:state result))
                                result))}]
      (dotimes [_ n]
        (try (poller/run-once! opts)
             (catch Exception _ nil))))))

(defn imessage-source-was-polled-n-times [n]
  (g/should= n @source-poll-count))

(defn imessage-module-is-declared []
  ;; isaac's discover! only picks up modules listed in config :modules or
  ;; {cwd}/modules/. Pre-seed :modules so the manifest at src/isaac-manifest.edn
  ;; activates when the server starts. Mirrors isaac-discord's
  ;; ensure-discord-module-declared!.
  (g/update! :server-config
             #(update (or % {}) :modules
                      (fn [m] (merge {:isaac.comm.imessage {:local/root "."}} m)))))

(defn imessage-comm-has-state [table]
  (let [instance (or (g/get :imessage-instance)
                     (comm-registry/comm-for "imessage"))]
    (g/should-not-be-nil instance)
    (let [state (imessage/state instance)]
      (doseq [row (:rows table)]
        (let [row-map (zipmap (:headers table) row)
              path    (get row-map "path")
              keys    (mapv keyword (str/split path #"\."))
              actual  (get-in state keys)
              raw     (get row-map "value")
              expected (cond
                         (re-matches #"-?\d+" raw) (parse-long raw)
                         (= "true" raw)            true
                         (= "false" raw)           false
                         (re-matches #":\S+" raw)  (keyword (subs raw 1))
                         :else                     raw)]
          (g/should= expected actual))))))

(defn imessage-allow-from-is [value]
  (let [parts (->> (str/split (or value "") #",")
                   (map str/trim)
                   (remove str/blank?)
                   vec)]
    (update-imessage-slice! #(assoc % :allow-from parts))))

(defn- parse-long-or-zero [s]
  (or (when (string? s) (parse-long s)) 0))

(defn- parse-raw-row [headers row]
  (let [m (zipmap headers row)]
    {:rowid      (parse-long-or-zero (get m "rowid"))
     :chat_guid  (get m "chat_guid")
     :handle_id  (get m "handle_id")
     :is_from_me (parse-long-or-zero (get m "is_from_me"))
     :text       (or (get m "text") "")
     :date       (parse-long-or-zero (get m "date"))}))

(defn imessage-chat-db-responds-with-rows [table]
  (let [rows (mapv #(parse-raw-row (:headers table) %) (:rows table))]
    (g/assoc! :imessage-chat-db-rows rows)))

(defn- fake-shell-store [_db-path]
  (let [rows (or (g/get :imessage-chat-db-rows) [])]
    (reify chat-db/RawMessageStore
      (-rows-since [_ watermark]
        (let [floor (:message-rowid watermark)]
          (if floor
            (vec (filter #(> (:rowid %) floor) rows))
            rows))))))

(defn imessage-inbox-is-polled-from-chat-db []
  (binding [fs/*fs* (or (g/get :mem-fs) fs/*fs*)]
    (with-redefs [chat-db/shell-store fake-shell-store]
      (let [result (imessage/poll-work-items-from-db! "/fake/chat.db"
                                                      (imessage-state-path))]
        (g/assoc! :imessage-work-items (:work-items result))
        (g/assoc! :imessage-state      (:state result))))))

(defn no-polled-work-items []
  (g/should= [] (vec (g/get :imessage-work-items))))

(defn polled-work-items-are [table]
  (let [items  (vec (g/get :imessage-work-items))
        result (match/match-entries table items)]
    (g/should= [] (:failures result))))

(defn imessage-state-has-chats [table]
  (binding [fs/*fs* (or (g/get :mem-fs) fs/*fs*)]
    (let [headers (:headers table)
          chats   (reduce (fn [acc row]
                            (let [m (zipmap headers row)]
                              (assoc acc (get m "chat-guid")
                                     {:handle      (get m "handle")
                                      :session-key (get m "session-key")})))
                          {}
                          (:rows table))]
      (state/write-state! (imessage-state-path)
                          (assoc state/default-state :chats chats)))))

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

(defgiven "the imessage state has chats:" isaac.comm.imessage.imessage-steps/imessage-state-has-chats
  "Writes <state-dir>/.isaac/comms/imessage/state.edn with the rows
   keyed by chat-guid (string). Each row maps chat-guid → {:handle
   :session-key}. Used to seed routing scenarios that need a known
   chat to already be mapped before the poll.")

(defgiven "the imessage source has rows:" isaac.comm.imessage.imessage-steps/imessage-source-has-rows
  "Installs an in-memory inbox/MessageSource fed by the table. Row
   columns: rowid, chat-guid, handle, text, from-me (0/1), sent-at
   (optional, defaults to 0).")

(defgiven "the imessage chat.db responds with rows:" isaac.comm.imessage.imessage-steps/imessage-chat-db-responds-with-rows
  "Installs a fake RawMessageStore returned by chat-db/shell-store
   so 'the imessage inbox is polled from chat.db' exercises the
   real normalize + fetch path without a real DB. Row columns are
   the raw SQLite shape: rowid, chat_guid, handle_id, is_from_me,
   text, date.")

(defgiven "the imessage source raises on the next {n:int} polls then succeeds" isaac.comm.imessage.imessage-steps/imessage-source-raises-then-succeeds
  "Sets a budget of N raise-on-poll responses. Once exhausted, the
   source behaves normally (returning rows from g state). Used by
   the poller resilience scenario.")

(defwhen "the imessage poller is ticked {n:int} times" isaac.comm.imessage.imessage-steps/imessage-poller-is-ticked-n-times
  "Invokes poller/run-once! N times with a drain-fn that uses the
   in-memory source. Wraps each tick in try/catch so a raise on one
   tick does not abort the loop (mirroring start!'s production
   resilience). Captures the final tick's :work-items and :state.")

(defthen "the imessage source was polled {n:int} times" isaac.comm.imessage.imessage-steps/imessage-source-was-polled-n-times
  "Asserts the recorded source-poll count.")

(defgiven "the imessage module is declared" isaac.comm.imessage.imessage-steps/imessage-module-is-declared
  "Adds {:isaac.comm.imessage {:local/root \".\"}} into the test's
   :server-config :modules so isaac.module.loader/discover! activates
   the manifest at src/isaac-manifest.edn when the Isaac server starts.
   Used in lifecycle scenarios that exercise the configurator path
   (not used by manual-registration scenarios like send.feature).")

(defthen "the imessage comm has state:" isaac.comm.imessage.imessage-steps/imessage-comm-has-state
  "Asserts the iMessage Comm's internal state map matches each row
   (dotted path -> value). Reads via imessage/state. Existing
   'the comm X exists with state:' step only reads telly comms.")

(defgiven "comms.imessage.allow-from is {value:string}" isaac.comm.imessage.imessage-steps/imessage-allow-from-is
  "Updates the registered imessage comm's slice with :allow-from
   parsed from a comma-separated string. Subsequent 'the imessage
   inbox is polled' reads this slice and passes :allow-from down
   to poll-work-items!. Empty value parses to [] (fail-closed).")

(defwhen "the imessage inbox is polled from chat.db" isaac.comm.imessage.imessage-steps/imessage-inbox-is-polled-from-chat-db
  "Calls imessage/poll-work-items-from-db! with chat-db/shell-store
   redefined to return the fake store from 'the imessage chat.db
   responds with rows:'.")

(defthen "there are no polled work items" isaac.comm.imessage.imessage-steps/no-polled-work-items
  "Asserts the captured :work-items collection is empty.")

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
