(ns isaac.comm.imessage.imessage-steps
  (:require
    [clojure.edn :as edn]
    [clojure.string :as str]
    [gherclj.core :as g :refer [defgiven defwhen defthen helper!]]
    [isaac.comm.delivery.worker :as worker]
    [isaac.comm.imessage :as imessage]
    [isaac.comm.imessage.imsg-client :as imsg-client]
    [isaac.comm.registry :as comm-registry]
    [isaac.config.api :as config]
    [isaac.config.loader :as loader]
    [isaac.foundation.root-steps :as root-steps]
    [isaac.fs :as fs]
    [isaac.llm.api.grover :as grover]
    [isaac.reconfigurable :as reconfigurable]
    [isaac.step-tables :as match]
    [isaac.nexus :as nexus]))

(helper! isaac.comm.imessage.imessage-steps)

(defrecord FakeImsgClient [calls]
  imsg-client/Client
  (-request! [_ method params]
    (swap! calls conj {:method method :params params})
    (doto (promise) (deliver {:ok true})))
  (-notify! [_ method params]
    (swap! calls conj {:method method :params params}))
  (-stop!    [_] nil)
  (-alive?-client [_] true))

(defn- fake-imsg-client []
  (->FakeImsgClient (atom [])))

(defn default-imessage-setup []
  ;; Don't blow away an already-initialized state dir (e.g. default Grover
  ;; setup ran first to install LLM defaults). Otherwise initialize fresh.
  (when-not (g/get :root)
    (root-steps/in-memory-state "target/test-state"))
  (let [client   (fake-imsg-client)
        host     {:name      "imessage"
                  :imsg-client client
                  ;; Host's :state-dir is the Isaac root where the
                  ;; delivery queue and other per-comm files live.
                  :state-dir (g/get :root)}
        instance (imessage/make host)]
    (reconfigurable/on-load instance {:imessage/service "iMessage"})
    (comm-registry/register-instance! "imessage" instance)
    (g/assoc! :imessage-instance instance)
    (g/assoc! :imessage-fake-client client)
    (g/assoc! :imessage-work-items [])))

(defn imessage-delivery-worker-ticks []
  (let [runtime-state-dir (g/get :root)]
    (g/assoc! :runtime-state-dir runtime-state-dir)
    (binding [fs/*fs* (or (g/get :mem-fs) fs/*fs*)]
      (nexus/-with-nexus {:root runtime-state-dir}
                         (worker/tick! {})))))

(defn- imessage-slice []
  (or (some-> (g/get :imessage-instance) imessage/state :slice) {}))

(defn- row->notification
  "Translate a scenario row (test-friendly column names) into the
   imsg notification shape that notification->work-item consumes:
   {:method \"message\" :params {:message {...payload...} :subscription N}}."
  [headers row]
  (let [m (zipmap headers row)]
    {:method "message"
     :params {:subscription 1
              :message      {:id          (some-> (get m "rowid") parse-long)
                             :chat_guid   (get m "chat-guid")
                             :sender      (get m "handle")
                             :text        (get m "text")
                             :is_from_me  (pos? (or (some-> (get m "from-me") parse-long) 0))
                             :created_at  (or (get m "sent-at") "1970-01-01T00:00:00Z")}}}))

(defn- update-imessage-slice! [updater]
  (when-let [instance (g/get :imessage-instance)]
    (reconfigurable/on-config-change! instance
                                    (:slice (imessage/state instance))
                                    (updater (:slice (imessage/state instance))))))

(defn- push-notifications! [headers rows dispatch?]
  (let [comm-impl (comm-registry/comm-for "imessage")
        slice     (imessage-slice)]
    (->> rows
         (map #(row->notification headers %))
         (keep (fn [notification]
                 (when-let [work-item (imessage/notification->work-item slice notification)]
                   (when dispatch?
                     (imessage/on-imsg-notification! comm-impl notification))
                   work-item)))
         vec)))

(defn imessage-source-has-rows
  "Stashes the rows for a later 'is polled' / 'is polled and
   dispatched' step. Each row will become an imsg notification."
  [table]
  (g/assoc! :imessage-test-rows table))

(defn imessage-inbox-is-polled []
  (let [table (g/get :imessage-test-rows)
        items (push-notifications! (:headers table) (:rows table) false)]
    (g/assoc! :imessage-work-items items)))

(defn imessage-inbox-is-polled-and-dispatched []
  (grover/clear-provider-requests!)
  (binding [fs/*fs* (or (g/get :mem-fs) fs/*fs*)]
    (let [cfg   (:config (loader/load-config-result {:root (g/get :root)}))
          _     (config/dangerously-install-config! cfg "imessage feature")
          table (g/get :imessage-test-rows)
          items (push-notifications! (:headers table) (:rows table) true)]
      (g/assoc! :imessage-work-items items)
      (g/assoc! :llm-request (grover/last-request)))))

(defn- live-imessage-instance []
  (or (g/get :imessage-instance)
      (nexus/get-in [:comms :imessage])
      (comm-registry/comm-for "imessage")))

(defn imessage-comm-has-state [table]
  (let [instance (live-imessage-instance)]
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

(defn imessage-lifecycle-setup []
  (root-steps/in-memory-state "target/test-state")
  (let [coord {:local/root (System/getProperty "user.dir")}
        path  (str (g/get :root) "/config/isaac.edn")
        fs*   (or (g/get :mem-fs) (nexus/get :fs) (fs/real-fs))
        cfg   {:server     {:hot-reload true}
               :modules    {:isaac.comm.imessage coord}
               :defaults   {:crew "main" :model "grover"}
               :models     {:grover {:model "echo" :provider :grover :context-window 32768}}
               :providers  {:grover {}}
               :crew       {:main {:model :grover :soul "You are Atticus."}}}]
    (fs/mkdirs fs* (fs/parent path))
    (fs/spit fs* path (pr-str cfg))
    (g/assoc! :server-config cfg)))

(defn- persist-imessage-module! [coord]
  (when-let [root (g/get :root)]
    (let [path (str root "/config/isaac.edn")
          fs*  (or (g/get :mem-fs) (nexus/get :fs) (fs/real-fs))
          cfg  (if (fs/exists? fs* path)
                 (edn/read-string (fs/slurp fs* path))
                 {})]
      (fs/mkdirs fs* (fs/parent path))
      (fs/spit fs* path (pr-str (assoc-in cfg [:modules :isaac.comm.imessage] coord))))))

(defn imessage-module-is-declared []
  (let [coord {:local/root (System/getProperty "user.dir")}]
    (g/update! :server-config
               #(update (or % {}) :modules
                        (fn [m] (merge {:isaac.comm.imessage coord} m))))
    (persist-imessage-module! coord)))

(defn imessage-isaac-server-started []
  ;; Lazy: server-steps only exists on the :features classpath.
  ((requiring-resolve 'isaac.server.server-steps/server-running)))

(defn imessage-message-cap-is [n]
  (update-imessage-slice! #(assoc % :imessage/message-cap n)))

(defn imessage-allow-from-is [value]
  (let [parts (->> (str/split (or value "") #",")
                   (map str/trim)
                   (remove str/blank?)
                   vec)]
    (update-imessage-slice! #(assoc % :imessage/allow-from parts))))

(defn no-polled-work-items []
  (g/should= [] (vec (g/get :imessage-work-items))))

(defn polled-work-items-are [table]
  (let [items  (vec (g/get :imessage-work-items))
        result (match/match-entries table items)]
    (g/should= [] (:failures result))))

(defn runner-was-invoked-with [table]
  (let [fake-client (g/get :imessage-fake-client)
        calls       (->> @(:calls fake-client)
                         (filter #(= "send" (:method %)))
                         (mapv (fn [call]
                                 (let [params (:params call)]
                                   {:service (or (:service params) "imessage")
                                    :buddy   (:to params)
                                    :body    (:text params)}))))
        result      (match/match-entries table calls)]
    (g/should= [] (:failures result))))

(defgiven "an in-memory Isaac state directory {path:string}" isaac.foundation.root-steps/in-memory-state
  "Compatibility route for features that still say 'in-memory Isaac state
   directory'. The harness stores the path as :root.")

(defgiven "default iMessage setup" isaac.comm.imessage.imessage-steps/default-imessage-setup
  "Sets up an in-memory state dir, registers a FakeImsgClient under
   the 'imessage' name, calls on-load so the comm wires its own
   notification handler. Subsequent steps push imsg notifications
   through that handler.")

(defgiven "the imessage source has rows:" isaac.comm.imessage.imessage-steps/imessage-source-has-rows
  "Stashes the row table for a later 'is polled' or 'is polled and
   dispatched' step. Each row becomes one imsg `message` notification
   when pushed.")

(defwhen "the imessage inbox is polled" isaac.comm.imessage.imessage-steps/imessage-inbox-is-polled
  "Pushes the stashed rows as imsg notifications through the comm's
   notification->work-item filter. Allowed messages are captured in
   :imessage-work-items; no dispatch.")

(defwhen "the imessage inbox is polled and dispatched" isaac.comm.imessage.imessage-steps/imessage-inbox-is-polled-and-dispatched
  "Pushes the deferred rows (from 'has these rows:') through the
   full notification handler, dispatching each work-item into Isaac's
   turn machinery and enqueuing replies. Captures grover/last-request
   into :llm-request.")

(defgiven "iMessage lifecycle setup" isaac.comm.imessage.imessage-steps/imessage-lifecycle-setup
  "In-memory Isaac root with inline Grover config (no per-entity files),
   hot-reload enabled, and the imessage module declared for discover!.")

(defgiven "the imessage module is declared" isaac.comm.imessage.imessage-steps/imessage-module-is-declared
  "Adds the imessage module to :server-config :modules so the
   discover! step activates the manifest when the Isaac server starts.")

(defgiven "the imessage Isaac server is started" isaac.comm.imessage.imessage-steps/imessage-isaac-server-started
  "Boots the Isaac server against the scenario state dir with the
   declared imessage module. Distinct from the generic server step so
   features don't collide with isaac.agent.module-steps.")

(defthen "the imessage comm has state:" isaac.comm.imessage.imessage-steps/imessage-comm-has-state
  "Asserts the iMessage Comm's internal state map matches each row
   (dotted path -> value).")

(defgiven "comms.imessage.message-cap is {n:int}" isaac.comm.imessage.imessage-steps/imessage-message-cap-is
  "Updates the registered comm's slice with :message-cap.")

(defgiven "comms.imessage.allow-from is {value:string}" isaac.comm.imessage.imessage-steps/imessage-allow-from-is
  "Updates the registered imessage comm's slice with :allow-from
   parsed from a comma-separated string. Empty value parses to []
   (fail-closed).")

(defthen "there are no polled work items" isaac.comm.imessage.imessage-steps/no-polled-work-items
  "Asserts the captured :work-items collection is empty.")

(defthen "the polled work items are:" isaac.comm.imessage.imessage-steps/polled-work-items-are
  "Matches the captured :work-items against the table.")

(defwhen "the imessage delivery worker ticks" isaac.comm.imessage.imessage-steps/imessage-delivery-worker-ticks
  "Runs worker/tick! against the iMessage-registered comm-registry.")

(defthen "the imessage runner was invoked with:" isaac.comm.imessage.imessage-steps/runner-was-invoked-with
  "Asserts captured imsg `send` calls match the table (buddy = :to,
   body = :text).")
