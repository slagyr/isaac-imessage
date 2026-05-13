(ns isaac.comm.imessage.config-spec
  (:require
    [isaac.comm.imessage.config :as sut]
    [speclj.core :refer :all]))

(defn- temp-path [name]
  (str (System/getProperty "java.io.tmpdir") "/" name "-" (random-uuid) ".edn"))

(describe "iMessage config"

  (it "defaults the config path under .isaac"
    (should= "/Users/micah/.isaac/imessage/config.edn"
             (sut/default-config-path "/Users/micah")))

  (it "returns defaults when the config file is missing"
    (should= {:service nil
              :interval-ms 1000}
             (sut/load-config "/tmp/isaac-imessage-missing-config.edn")))

  (it "loads an EDN config file and merges defaults"
    (let [path (temp-path "isaac-imessage-config")]
      (spit path (pr-str {:service "E:me" :interval-ms 2500}))
      (should= {:service "E:me"
                :interval-ms 2500}
               (sut/load-config path))))

  (it "prefers explicit CLI values over config file values"
    (should= {:service "E:cli"
              :interval-ms 5000}
             (sut/merge-config {:service "E:file" :interval-ms 2500}
                               {:service "E:cli" :interval-ms 5000})))

  (it "keeps file values when CLI values are nil"
    (should= {:service "E:file"
              :interval-ms 2500}
             (sut/merge-config {:service "E:file" :interval-ms 2500}
                               {:service nil :interval-ms nil}))))
