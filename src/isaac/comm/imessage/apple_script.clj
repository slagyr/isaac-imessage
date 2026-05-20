(ns isaac.comm.imessage.apple-script
  (:require
    [clojure.java.shell :as shell]
    [clojure.string :as str]))

(defn- escape-applescript-string [s]
  (-> (or s "")
      (str/replace "\\" "\\\\")
      (str/replace "\"" "\\\"")))

(defn- service-keyword [service]
  ;; AppleScript service-type values are literal keywords (iMessage,
  ;; SMS, etc.), not strings. Default to iMessage.
  (or (when (string? service)
        (let [lc (str/lower-case service)]
          (cond
            (= lc "imessage") "iMessage"
            (= lc "sms")      "SMS"
            :else             nil)))
      "iMessage"))

(defn build-script [{:keys [message service target]}]
  ;; Modern Messages.app rejects `buddy "X" of service "iMessage"`
  ;; with -10002 "Invalid key form". Resolve the service object
  ;; first via `service type =`, then ask for the buddy on it.
  (str "tell application \"Messages\"\n"
       "  set targetService to 1st service whose service type = "
       (service-keyword service) "\n"
       "  set targetBuddy to buddy \""
       (escape-applescript-string target) "\" of targetService\n"
       "  send \"" (escape-applescript-string message)
       "\" to targetBuddy\n"
       "end tell"))

(defn run-command [args]
  (apply shell/sh args))

(defn- classify-failure [{:keys [err]}]
  (let [err (or err "")]
    (cond
      (str/includes? err "Not authorized to send Apple events")
      {:ok false :transient? false :error :not-authorized}

      (or (str/includes? err "Application isn")
          (str/includes? err "Messages got an error"))
      {:ok false :transient? true :error :messages-unavailable}

      :else
      {:ok false :transient? true :error :send-failed})))

(defn send-message! [request]
  (let [result (run-command ["osascript" "-e" (build-script request)])]
    (if (zero? (:exit result))
      {:ok true}
      (classify-failure result))))
