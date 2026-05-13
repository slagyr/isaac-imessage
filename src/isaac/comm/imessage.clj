(ns isaac.comm.imessage
  (:require
    [clojure.string :as str]
    [isaac.api :as api]
    [isaac.comm :as comm]
    [isaac.comm.imessage.apple-script :as apple-script]
    [isaac.comm.imessage.chat-db :as chat-db]
    [isaac.comm.imessage.inbox :as inbox]
    [isaac.comm.imessage.routing :as routing]
    [isaac.comm.imessage.state :as state]
    [isaac.configurator :as configurator]))

(defn default-chat-db-path
  ([] (default-chat-db-path (System/getProperty "user.home")))
  ([home]
   (str home "/Library/Messages/chat.db")))

(defn default-state-path
  ([] (default-state-path (System/getProperty "user.home")))
  ([home]
   (str home "/.isaac/imessage/state.edn")))

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

(defn poll-routed! [source path]
  (let [current (read-state path)
        polled  (inbox/poll! source current)
        routed  (reduce (fn [{:keys [state messages]} message]
                          (let [{:keys [session-key state]} (routing/ensure-session state (:thread-id message) (:handle message))]
                            {:state    state
                             :messages (conj messages (assoc message :session-key session-key))}))
                        {:state (:state polled) :messages []}
                        (:messages polled))]
    (write-state! path (:state routed))
    routed))

(defn- ->work-item [message]
  {:session-key (:session-key message)
   :input       (:text message)
   :origin      {:kind          :imessage
                 :thread-id     (:thread-id message)
                 :handle        (:handle message)
                 :message-rowid (:message-rowid message)
                 :sent-at       (:sent-at message)}})

(defn poll-work-items! [source path]
  (let [routed (poll-routed! source path)]
    {:work-items (mapv ->work-item (:messages routed))
     :state      (:state routed)}))

(defn poll-work-items-from-db!
  ([db-path state-path]
   (let [store  (chat-db/shell-store db-path)
         source (chat-db/message-source store)
         result (poll-work-items! source state-path)]
     (assoc result :db-path db-path :state-path state-path)))
  ([]
   (poll-work-items-from-db! (default-chat-db-path)
                             (default-state-path))))

(defn dispatch-request [work-item]
  {:session-key (:session-key work-item)
   :input       (:input work-item)
   :origin      (:origin work-item)})

(defn- ensure-session! [state-dir work-item]
  (or (api/get-session state-dir (:session-key work-item))
      (api/create-session! state-dir
                           (:session-key work-item)
                           {:origin   (:origin work-item)
                            :chatType "direct"
                            :channel  "imessage"})))

(defn dispatch-work-item! [state-dir work-item]
  (ensure-session! state-dir work-item)
  (api/dispatch! state-dir (dispatch-request work-item)))

(defn dispatch-work-items! [state-dir work-items]
  (mapv #(dispatch-work-item! state-dir %) work-items))

(defn result->reply-text [result]
  (or (get-in result [:message :content])
      (:content result)
      (:message result)
      (:message (:error result))
      (:message result)
      (when-let [error (:error result)]
        (if (keyword? error) (name error) (str error)))
      ""))

(defn chunk-reply-text
  ([text] (chunk-reply-text text 2000))
  ([text max-chars]
   (let [text (or text "")]
     (loop [remaining (str/trim text)
            chunks    []]
       (cond
         (empty? remaining)
         (if (seq chunks) chunks [""])

         (<= (count remaining) max-chars)
         (conj chunks remaining)

         :else
         (let [candidate (subs remaining 0 max-chars)
               split-at  (or (some->> (re-find #"(?s)^.*\s" candidate) count)
                             max-chars)
               chunk     (str/trim (subs remaining 0 split-at))
               next-text (str/trim (subs remaining split-at))]
           (recur next-text (conj chunks chunk))))))))

(defn reply-record [work-item result service]
  {:content (result->reply-text result)
   :service service
   :target  (get-in work-item [:origin :handle])})

(defn preview-reply-records [work-item result service]
  (mapv (fn [content]
          {:content content
           :service service
           :target  (get-in work-item [:origin :handle])})
        (chunk-reply-text (result->reply-text result))))

(defn dispatch-and-reply-work-item!
  ([state-dir work-item service]
   (dispatch-and-reply-work-item! state-dir work-item service 2000))
  ([state-dir work-item service max-chars]
   (let [dispatch-result   (dispatch-work-item! state-dir work-item)
         reply-text        (result->reply-text dispatch-result)
         delivery-results  (mapv #(send! {:content %
                                          :service service
                                          :target  (get-in work-item [:origin :handle])})
                                 (chunk-reply-text reply-text max-chars))]
     {:dispatch-result dispatch-result
      :delivery-results delivery-results})))

(defn drain-once!
  ([isaac-home db-path state-path]
   (let [{:keys [work-items state] :as polled} (poll-work-items-from-db! db-path state-path)
         results (dispatch-work-items! isaac-home work-items)]
     (assoc polled :results results :state state)))
  ([isaac-home]
   (drain-once! isaac-home
                (default-chat-db-path)
                (default-state-path))))

(defn drain-once-and-reply!
  ([isaac-home db-path state-path service]
   (let [{:keys [work-items state] :as polled} (poll-work-items-from-db! db-path state-path)
         results (mapv #(dispatch-and-reply-work-item! isaac-home % service) work-items)]
     (assoc polled :results results :state state)))
  ([isaac-home service]
   (drain-once-and-reply! isaac-home
                          (default-chat-db-path)
                          (default-state-path)
                          service)))

(defn inspect-work-items-from-db! [db-path state-path service]
  (let [{:keys [work-items] :as result} (poll-work-items-from-db! db-path state-path)]
    (assoc result :reply-preview
                  (mapv (fn [item]
                          {:session-key (:session-key item)
                           :records     (preview-reply-records item {:message {:content (:input item)}} service)})
                        work-items))))

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
