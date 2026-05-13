(ns isaac.comm.imessage.apple-script
  (:require
    [clojure.java.shell :as shell]
    [clojure.string :as str]))

(defn- escape-applescript-string [s]
  (-> (or s "")
      (str/replace "\\" "\\\\")
      (str/replace "\"" "\\\"")))

(defn build-script [{:keys [message service target]}]
  (str "tell application \"Messages\"\n"
       "  send \"" (escape-applescript-string message) "\" to buddy \""
       (escape-applescript-string target) "\" of service \""
       (escape-applescript-string service) "\"\n"
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
