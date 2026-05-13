(ns isaac.comm.imessage.routing)

;; Thread-to-session routing.
;;
;; Expected responsibilities:
;; - map iMessage thread identity to Isaac session key
;; - create sessions lazily when needed
;; - support future crew/model routing rules
