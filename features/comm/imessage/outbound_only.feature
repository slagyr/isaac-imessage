Feature: a send-only iMessage comm (isaac-k00m)
  subscribe-to-inbound! is called unconditionally from on-load, so any host
  that configures an iMessage comm watches the chat.db it points at. There is
  no way to declare a comm that only sends.

  That is not hypothetical. yopp's comm reaches zanebot's imsg over SSH, so it
  watches the same chat.db zanebot already watches: on 2026-09-24 both hosts
  logged :imsg.watch/subscribed against one database. A single inbound message
  would have dispatched a turn on each and produced two independent replies.

  The stopgap was :imessage/allow-from [], which works only because allowed?
  treats an empty list as "nobody". It reads as a mistake rather than as
  intent, and it still opens a subscription the host never uses. allow-from is
  the sender whitelist; it is not an inbound switch.

  :imessage/inbound? false says so directly, and skips watch.subscribe
  entirely. Absent, the comm stays bidirectional as it is today.

  Background:
    Given default iMessage setup

  Scenario: a send-only comm never subscribes to the watch
    Given comms.imessage.inbound? is false
    When the imessage Isaac server is started
    Then the imessage watch was not subscribed
    And the log has entries matching:
      | event           | comm     |
      | :comm/activated | imessage |

  Scenario: a comm with no inbound? flag subscribes as before
    When the imessage Isaac server is started
    Then the imessage watch was subscribed

  Scenario: a send-only comm still delivers outbound
    Given comms.imessage.inbound? is false
    And the isaac EDN file comm/delivery/pending/4b1d.edn exists with:
      | path            | value             |
      | id              | 4b1d              |
      | comm            | imessage          |
      | imessage/target | friend@icloud.com |
      | content         | Still sending.    |
    When the imessage delivery worker ticks
    Then the isaac file "comm/delivery/pending/4b1d.edn" does not exist
    And the imessage runner was invoked with:
      | buddy             | body           |
      | friend@icloud.com | Still sending. |

  Scenario: a send-only comm dispatches nothing for an inbound message
    Given comms.imessage.inbound? is false
    And comms.imessage.allow-from is "cordelia@marigold.test"
    And the imessage source has rows:
      | rowid | chat-guid                    | handle                 | text | from-me | dest-caller           | chat-id |
      | 31    | any;-;cordelia@marigold.test | cordelia@marigold.test | ping | 0       | logbook@marigold.test | 3       |
    When the imessage inbox is polled
    Then the polled work items are:
      | session-key | input |
