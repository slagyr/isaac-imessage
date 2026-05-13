(ns isaac.comm.imessage.state
  (:require
    [clojure.edn :as edn]
    [clojure.java.io :as io]))

(def default-state {:threads {} :watermark nil})

(defn- normalize-state [state]
  (merge default-state state))

(defn read-state [path]
  (let [file (io/file path)]
    (if (.exists file)
      (normalize-state (edn/read-string (slurp file)))
      default-state)))

(defn write-state! [path state]
  (let [file (io/file path)]
    (.mkdirs (.getParentFile file))
    (spit file (pr-str (normalize-state state)))
    state))

(defn assoc-thread-session [state thread-id handle session-key]
  (assoc-in (normalize-state state)
            [:threads thread-id]
            {:handle handle :session-key session-key}))

(defn assoc-watermark [state watermark]
  (assoc (normalize-state state) :watermark watermark))
