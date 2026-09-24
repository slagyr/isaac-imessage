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
    [isaac.spec-helper :as helper]
    [isaac.llm.api.grover :as grover]
    [isaac.reconfigurable :as reconfigurable]
    [isaac.session.store.memory :as memory-store]
    [isaac.session.store.spi :as session-store]
    [isaac.step-tables :as match]
    [isaac.nexus :as nexus]))

(helper! isaac.comm.imessage.imessage-steps)

(defrecord FakeImsgClient [calls send-response]
  imsg-client/Client
  (-request! [_ method params]
    (swap! calls conj {:method method :params params})
    (doto (promise)
      (deliver (or (when (= "send" method) @send-response) {:ok true}))))
  (-notify! [_ method params]
    (swap! calls conj {:method method :params params}))
  (-stop!    [_] nil)
  (-alive?-client [_] true))

(defn- fake-imsg-client []
  (->FakeImsgClient (atom []) (atom nil)))

;; The server-start scenarios need the comm the *server* builds to talk to the
;; scenario's FakeImsgClient, so imsg-client/start! is stubbed for the duration
;; of the scenario (the same seam the unit specs redef). :opts records that the
;; spawn really happened, so "the watch was not subscribed" cannot pass just
;; because no client was ever spawned.
(defonce ^:private real-imsg-start! imsg-client/start!)
(defonce ^:private imsg-spawn* (atom nil))

(defn- stub-imsg-spawn! [client]
  (reset! imsg-spawn* nil)
  (alter-var-root #'imsg-client/start!
                  (constantly (fn [opts] (reset! imsg-spawn* opts) client))))

(g/after-scenario
  (fn []
    (alter-var-root #'imsg-client/start! (constantly real-imsg-start!))
    (reset! imsg-spawn* nil)))

(defn- feature-fs []
  (or (g/get :mem-fs) (nexus/get :fs) (fs/real-fs)))

(defn- ensure-session-store!
  "Register an in-memory SessionStore when the scenario harness has not
   already installed one (e.g. default Grover setup ran without the
   agent session-steps root hook on the classpath)."
  []
  (when-not (session-store/registered-store)
    (when-let [root (g/get :root)]
      (session-store/register-store! (memory-store/create-store root)))))

(defn default-imessage-setup []
  ;; Grover is a config-driven test provider now; the removed
  ;; install-test-fixture! only reset the response queue.
  (grover/reset-queue!)
  ;; HTTP's feature hook may seed a real :root before the Background runs.
  ;; Preserve only a root that already has an in-memory feature filesystem
  ;; (e.g. default Grover setup); otherwise replace the seed with our fixture.
  (when-not (g/get :mem-fs)
    (root-steps/in-memory-state "target/test-state"))
  (ensure-session-store!)
  (let [client   (fake-imsg-client)
        host     {:name      "imessage"
                  :imsg-client client
                  ;; Host's :state-dir is the Isaac root where the
                  ;; delivery queue and other per-comm files live.
                  :state-dir (g/get :root)}
        instance (imessage/make host)]
    ;; No :imessage/service — the shipping default. An explicit service is
    ;; an operator decision, and configuring "iMessage" is the broken path
    ;; through imsg's AppleScript transport (isaac-2zs0).
    (reconfigurable/on-load instance {})
    (comm-registry/register-instance! "imessage" instance)
    (g/assoc! :imessage-instance instance)
    (g/assoc! :imessage-fake-client client)
    (g/assoc! :imessage-work-items [])))

(defn imessage-delivery-worker-ticks []
  (let [runtime-state-dir (g/get :root)]
    (g/assoc! :runtime-state-dir runtime-state-dir)
    (nexus/-with-nested-nexus {:fs   (feature-fs)
                               :root runtime-state-dir}
      (worker/tick! {}))))

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
        slice     (imessage-slice)
        state-dir (g/get :root)
        max-chars (or (:imessage/message-cap slice) 2000)
        max-chunks (or (:imessage/max-chunks slice) 3)]
    (->> rows
         (map #(row->notification headers %))
         (keep (fn [notification]
                 (when-let [work-item (imessage/notification->work-item slice notification)]
                   (when dispatch?
                     ;; Call the dispatch pipeline directly so feature
                     ;; failures surface (on-imsg-notification! swallows).
                     (imessage/dispatch-and-enqueue-reply!
                       state-dir work-item comm-impl max-chars max-chunks))
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
  (ensure-session-store!)
  (let [fs*   (feature-fs)
        root  (g/get :root)
        table (g/get :imessage-test-rows)]
    ;; Nested — preserve :sessions/:config already installed by setup.
    (nexus/-with-nested-nexus {:fs fs* :root root}
      (let [cfg   (:config (loader/load-config-result {:root root :fs fs*}))
            _     (config/dangerously-install-config! cfg "imessage feature")
            items (push-notifications! (:headers table) (:rows table) true)]
        (g/assoc! :imessage-work-items items)
        (g/assoc! :llm-request (grover/last-request))))))

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
        cfg   {:hot-reload true
               :modules    {:isaac.comm.imessage coord}
               ;; :defaults is entity templates (isaac-ruom). The default crew
               ;; id lives at :defaults :frequencies :crew and the default model
               ;; alias at :defaults :crew :model; the flat :defaults :crew /
               ;; :defaults :model keys are retired and now fail validation,
               ;; which would sink the whole config — and with it the comms
               ;; slice this fixture exists to exercise.
               :defaults   {:frequencies {:crew "main"}
                            :crew        {:model "grover"}}
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

(defn- feature-comm-slice []
  ;; db-path + command together are what make the comm spawn a client at all
  ;; (a wrapped command skips the local-file check); the stubbed spawn hands
  ;; back the scenario's fake instead of a real imsg subprocess.
  (cond-> {:type             :imessage
           :imessage/db-path "/tmp/isaac-imessage-feature-chat.db"
           :imessage/command ["imsg-feature-stub"]}
          (some? (g/get :imessage-inbound?))
          (assoc :imessage/inbound? (g/get :imessage-inbound?))))

(defn- seed-imessage-server-config! [client]
  (stub-imsg-spawn! client)
  ;; Boot is the observation window for the watch: drop the calls the
  ;; Background's own on-load made.
  (reset! (:calls client) [])
  (g/update! :server-config
             (fn [cfg]
               (-> (or cfg {})
                   (update :modules #(merge {:isaac.comm.imessage {:local/root (System/getProperty "user.dir")}} %))
                   (update-in [:comms :imessage] #(merge (feature-comm-slice) %))))))

(defn imessage-isaac-http-started []
  ;; Scenarios that run on the 'default iMessage setup' background carry a
  ;; FakeImsgClient; give the server-built comm that client and a comm slice
  ;; to activate. Lifecycle scenarios bring their own config and no fake.
  (when-let [client (g/get :imessage-fake-client)]
    (seed-imessage-server-config! client))
  ;; Lazy: server-steps only exists on the :features classpath.
  ((requiring-resolve 'isaac.http.server-steps/server-running)))

(defn- live-imessage-comm []
  (nexus/get-in [:comms :imessage]))

(defn- watch-subscribe-calls []
  (->> @(:calls (g/get :imessage-fake-client))
       (filterv #(= "watch.subscribe" (:method %)))))

(defn- await-comm-spawned! []
  (helper/await-condition #(and (some? (live-imessage-comm)) (some? @imsg-spawn*)) 6000)
  (g/should-not-be-nil (live-imessage-comm))
  (g/should-not-be-nil @imsg-spawn*))

(defn imessage-watch-was-subscribed []
  (await-comm-spawned!)
  (helper/await-condition #(seq (watch-subscribe-calls)) 5000)
  (g/should= 1 (count (watch-subscribe-calls))))

(defn imessage-watch-was-not-subscribed []
  (await-comm-spawned!)
  (g/should= [] (watch-subscribe-calls)))

(defn comm-registered-for-delivery [name]
  (helper/await-condition #(some? (comm-registry/comm-for name)) 5000)
  (g/should-not-be-nil (comm-registry/comm-for name)))

(defn comm-not-registered-for-delivery [name]
  (helper/await-condition #(nil? (comm-registry/comm-for name)) 5000)
  (g/should-be-nil (comm-registry/comm-for name)))

(defn imessage-message-cap-is [n]
  (update-imessage-slice! #(assoc % :imessage/message-cap n)))

(defn imessage-service-is [value]
  (update-imessage-slice! #(assoc % :imessage/service value)))

(defn- blank->nil [s]
  (when-not (str/blank? s) s))

(defn imsg-send-fails-with
  "Arms the FakeImsgClient so the next `send` resolves to an imsg
   JSON-RPC error. Columns: code, message, and the structured :data
   fields imsg answers with — disposition, retry_safe, detail."
  [table]
  (let [row   (zipmap (:headers table) (first (:rows table)))
        data  (cond-> {}
                (blank->nil (get row "disposition")) (assoc :disposition (get row "disposition"))
                (blank->nil (get row "retry_safe"))  (assoc :retry_safe (= "true" (get row "retry_safe")))
                (blank->nil (get row "detail"))      (assoc :detail (get row "detail")))
        rpc   (cond-> {:message (or (blank->nil (get row "message")) "JSON-RPC error")}
                (blank->nil (get row "code")) (assoc :code (parse-long (get row "code")))
                (seq data)                    (assoc :data data))]
    (reset! (:send-response (g/get :imessage-fake-client))
            (ex-info (:message rpc) {:type :imsg/error :rpc-error rpc}))))

(defn imessage-runner-send-count [n]
  (let [calls (->> @(:calls (g/get :imessage-fake-client))
                   (filter #(= "send" (:method %))))]
    (g/should= n (count calls))))

(defn imessage-allow-from-is [value]
  (let [parts (->> (str/split (or value "") #",")
                   (map str/trim)
                   (remove str/blank?)
                   vec)]
    (update-imessage-slice! #(assoc % :imessage/allow-from parts))))

(defn imessage-inbound-flag-is [value]
  (let [flag (= "true" (str/trim (or value "")))]
    ;; Remembered for a later server start, and applied to the comm the
    ;; Background already loaded so inbox/delivery steps see it too.
    (g/assoc! :imessage-inbound? flag)
    (update-imessage-slice! #(assoc % :imessage/inbound? flag))))

(defn no-polled-work-items []
  (g/should= [] (vec (g/get :imessage-work-items))))

(defn polled-work-items-are [table]
  (let [items  (vec (g/get :imessage-work-items))
        result (match/match-entries table items)]
    (g/should= [] (:failures result))
    ;; One row per work item, so a table with no rows asserts "nothing was
    ;; polled" instead of passing vacuously.
    (g/should= (count (:rows table)) (count items))))

(defn runner-was-invoked-with [table]
  (let [fake-client (g/get :imessage-fake-client)
        calls       (->> @(:calls fake-client)
                         (filter #(= "send" (:method %)))
                         (mapv (fn [call]
                                 (let [params (:params call)]
                                   ;; :service reported raw — a blank cell in
                                   ;; the table asserts imsg was left to pick
                                   ;; the transport itself (isaac-2zs0).
                                   {:service (:service params)
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

(defgiven "the imessage Isaac server is started" isaac.comm.imessage.imessage-steps/imessage-isaac-http-started
  "Boots the Isaac server against the scenario state dir with the
   declared imessage module. Distinct from the generic server step so
   features don't collide with isaac.agent.module-steps.")

(defthen "the imessage comm has state:" isaac.comm.imessage.imessage-steps/imessage-comm-has-state
  "Asserts the iMessage Comm's internal state map matches each row
   (dotted path -> value).")

(defthen "the comm {name:string} is registered for delivery" isaac.comm.imessage.imessage-steps/comm-registered-for-delivery
  "Asserts the live comm slot is registered in comm-registry for the
   delivery worker (berth :register-fn on create).")

(defthen "the comm {name:string} is not registered for delivery" isaac.comm.imessage.imessage-steps/comm-not-registered-for-delivery
  "Asserts the comm slot was deregistered from comm-registry on teardown.")

(defgiven "comms.imessage.message-cap is {n:int}" isaac.comm.imessage.imessage-steps/imessage-message-cap-is
  "Updates the registered comm's slice with :message-cap.")

(defgiven "comms.imessage.service is {value:string}" isaac.comm.imessage.imessage-steps/imessage-service-is
  "Updates the registered comm's slice with :imessage/service — the
   deliberate operator choice. Omit the step for the default (no service;
   imsg picks the transport).")

(defgiven "the imsg send fails with:" isaac.comm.imessage.imessage-steps/imsg-send-fails-with
  "Arms the fake imsg client so `send` resolves to a JSON-RPC error.
   Columns: code, message, disposition, retry_safe, detail — the last
   three become the structured :data map imsg answers with.")

(defthen "the imessage runner send count is {n:int}" isaac.comm.imessage.imessage-steps/imessage-runner-send-count
  "Asserts how many `send` calls reached imsg — the retry count.")

(defgiven "comms.imessage.allow-from is {value:string}" isaac.comm.imessage.imessage-steps/imessage-allow-from-is
  "Updates the registered imessage comm's slice with :allow-from
   parsed from a comma-separated string. Empty value parses to []
   (fail-closed).")

(defgiven "comms.imessage.inbound? is {value:string}" isaac.comm.imessage.imessage-steps/imessage-inbound-flag-is
  "Sets :imessage/inbound? on the registered imessage comm's slice, and on
   the comm config a later 'imessage Isaac server is started' activates.
   \"false\" declares the comm send-only.")

(defthen "the imessage watch was subscribed" isaac.comm.imessage.imessage-steps/imessage-watch-was-subscribed
  "Asserts the activated comm spawned an imsg client and called
   watch.subscribe on it exactly once.")

(defthen "the imessage watch was not subscribed" isaac.comm.imessage.imessage-steps/imessage-watch-was-not-subscribed
  "Asserts the activated comm spawned an imsg client but never called
   watch.subscribe — the send-only path. Fails if no client was spawned,
   so it cannot pass by the comm never starting.")

(defthen "there are no polled work items" isaac.comm.imessage.imessage-steps/no-polled-work-items
  "Asserts the captured :work-items collection is empty.")

(defthen "the polled work items are:" isaac.comm.imessage.imessage-steps/polled-work-items-are
  "Matches the captured :work-items against the table.")

(defwhen "the imessage delivery worker ticks" isaac.comm.imessage.imessage-steps/imessage-delivery-worker-ticks
  "Runs worker/tick! against the iMessage-registered comm-registry.")

(defthen "the imessage runner was invoked with:" isaac.comm.imessage.imessage-steps/runner-was-invoked-with
  "Asserts captured imsg `send` calls match the table (buddy = :to,
   body = :text).")
