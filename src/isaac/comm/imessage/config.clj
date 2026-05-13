(ns isaac.comm.imessage.config
  (:require
    [clojure.edn :as edn]
    [clojure.java.io :as io]))

(def defaults {:service nil
               :interval-ms 1000})

(defn default-config-path [home]
  (str home "/.isaac/imessage/config.edn"))

(defn load-config [path]
  (let [file (io/file path)]
    (if (.exists file)
      (merge defaults (edn/read-string (slurp file)))
      defaults)))

(defn merge-config [file-config cli-config]
  (merge file-config (into {} (remove (comp nil? val) cli-config))))
