(ns isaac.comm.imessage.chat-db
  (:require
    [clojure.java.shell :as shell]
    [clojure.string :as str]
    [isaac.comm.imessage.inbox :as inbox]))

(defprotocol RawMessageStore
  (-rows-since [store watermark]))

(def ^:private field-separator "\u001F")

(defn rows-since [store watermark]
  (-rows-since store watermark))

(defn- escape-sql-text [text]
  (-> text
      (str/replace "\\" "\\\\")
      (str/replace "'" "''")))

(defn sqlite3-command [db-path watermark]
  (let [rowid (or (:message-rowid watermark) 0)
        sql   (str "SELECT "
                   "m.ROWID || char(31) || "
                   "ifnull(c.guid, '') || char(31) || "
                   "ifnull(h.id, '') || char(31) || "
                   "ifnull(m.is_from_me, 0) || char(31) || "
                   "replace(replace(replace(replace(ifnull(m.text, ''), '\\', '\\\\'), char(10), '\\n'), char(13), '\\r'), char(9), '\\t') || char(31) || "
                   "ifnull(m.date, 0) "
                   "FROM message m "
                   "LEFT JOIN handle h ON h.ROWID = m.handle_id "
                   "LEFT JOIN chat_message_join cmj ON cmj.message_id = m.ROWID "
                   "LEFT JOIN chat c ON c.ROWID = cmj.chat_id "
                   "WHERE m.ROWID > " rowid " "
                   "ORDER BY m.ROWID ASC;")]
    ["sqlite3" db-path sql]))

(defn run-command [command]
  (apply shell/sh command))

(defn- parse-long-safe [value]
  (when-not (str/blank? value)
    (parse-long value)))

(defn- unescape-text [text]
  (-> text
      (str/replace "\\t" "\t")
      (str/replace "\\r" "\r")
      (str/replace "\\n" "\n")
      (str/replace "\\\\" "\\")))

(defn parse-row-line [line]
  (let [[rowid chat-guid handle-id from-me text date] (str/split line (re-pattern field-separator) -1)]
    {:rowid      (or (parse-long-safe rowid) 0)
     :chat_guid  chat-guid
     :handle_id  handle-id
     :is_from_me (or (parse-long-safe from-me) 0)
     :text       (unescape-text text)
     :date       (or (parse-long-safe date) 0)}))

(deftype ShellStore [db-path]
  RawMessageStore
  (-rows-since [_ watermark]
    (let [result (run-command (sqlite3-command db-path watermark))]
      (if (zero? (:exit result))
        (->> (str/split-lines (or (:out result) ""))
             (remove str/blank?)
             (mapv parse-row-line))
        (throw (ex-info "sqlite3 query failed" {:db-path db-path :error (:err result)}))))))

(defn normalize-row [row]
  {:message-rowid (:rowid row)
   :thread-id      (:chat_guid row)
   :handle         (:handle_id row)
   :from-me?       (boolean (pos? (long (or (:is_from_me row) 0))))
   :text           (:text row)
   :sent-at        (:date row)})

(defn fetch-messages [store watermark]
  (->> (rows-since store watermark)
       (map normalize-row)
       (sort-by :message-rowid)
       vec))

(deftype ChatDbSource [store]
  inbox/MessageSource
  (-messages-since [_ watermark]
    (fetch-messages store watermark)))

(defn shell-store [db-path]
  (->ShellStore db-path))

(defn message-source [store]
  (->ChatDbSource store))
