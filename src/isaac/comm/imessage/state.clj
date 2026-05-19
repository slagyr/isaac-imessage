(ns isaac.comm.imessage.state
  (:require
    [clojure.edn :as edn]
    [isaac.fs :as fs]))

(def default-state {:chats {} :watermark nil})

(defn- normalize-state [state]
  (merge default-state state))

(defn read-state [path]
  (if (fs/exists? path)
    (normalize-state (edn/read-string (fs/slurp path)))
    default-state))

(defn write-state! [path state]
  (fs/mkdirs (fs/parent path))
  (fs/spit path (pr-str (normalize-state state)))
  state)

(defn assoc-chat-session [state chat-guid handle session-key]
  (assoc-in (normalize-state state)
            [:chats chat-guid]
            {:handle handle :session-key session-key}))

(defn assoc-watermark [state watermark]
  (assoc (normalize-state state) :watermark watermark))
