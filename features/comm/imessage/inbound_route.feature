Feature: iMessage inbound routing — a reply lands on the chat's session
  iMessage is one point-to-point pipe: every inbound text from a chat lands
  on imessage:<chat-guid> and the default crew, even after another crew
  texted that handle from a cron session. Who sees what was sent into the
  chat is the conversations-and-channels design (isaac/doc/
  design-conversations-and-channels.md, isaac-mve9/9khs); the opt-in reply
  routing policy (isaac-ugvu) is scrapped in its favor.

  Background:
    Given default iMessage setup
    And the isaac EDN file "config/crew/red-alert.edn" exists with:
      | path  | value |
      | model | echo  |

  Scenario: a reply goes to the chat's session even after another crew texted that handle
    Given the current time is "2026-10-02T10:00:00Z"
    And the isaac EDN file comm/delivery/pending/b1rd.edn exists with:
      | path            | value                                   |
      | id              | b1rd                                    |
      | comm            | imessage                                |
      | imessage/target | +15551234567                            |
      | content         | Leo's birthday is in 3 days.            |
      | crew            | red-alert                               |
      | session         | heartbeat                               |
    When the imessage delivery worker ticks
    Given the current time is "2026-10-02T10:05:00Z"
    And the imessage source has rows:
      | rowid | chat-guid | handle       | text            | from-me |
      | 1     | T1        | +15551234567 | already got one | 0       |
    When the imessage inbox is polled
    Then the polled work items are:
      | session-key | crew | input           |
      | imessage:T1 |      | already got one |
