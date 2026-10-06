Feature: A delivery into an iMessage chat lands in that chat's session (isaac-9khs)
  Follow-up to isaac-mve9 (isaac/doc/design-conversations-and-comms.md).
  An inbound text records its chat on the session's :comms
  ("imessage:<chat-guid>"). A send addresses a handle, so send! reports
  :target as the chat that handle's messages live in (the chat id from
  the source rows). The agent
  delivery worker then appends a delivery posted by another session (a
  heartbeat ping, attention, comm__send) to the chat's session as an
  assistant message:
    [sent here by crew <crew> from session <session>] <content>
  The chat's own replies are not appended twice. A reply to the ping
  lands on the chat's session as before, which now holds the ping.

  Background:
    Given default Grover setup in "target/imessage-continuity"
    And default iMessage setup
    And the isaac EDN file "config/crew/red-alert.edn" exists with:
      | path  | value |
      | model | echo  |
    And the following model responses are queued:
      | model | type | content |
      | echo  | text | Here.   |
    And the imessage source has rows:
      | rowid | chat-guid | handle       | text | from-me |
      | 1     | T1        | +15551234567 | ping | 0       |
    And the imessage inbox is polled and dispatched
    And the imessage delivery worker ticks

  @wip
  Scenario: another crew's ping into a talked-in chat lands in that chat's session as a marked note
    Given the isaac EDN file comm/delivery/pending/b1rd.edn exists with:
      | path            | value                        |
      | id              | b1rd                         |
      | comm            | imessage                     |
      | imessage/target | +15551234567                 |
      | content         | Leo's birthday is in 3 days. |
      | crew            | red-alert                    |
      | session         | heartbeat                    |
    When the imessage delivery worker ticks
    Then the imessage runner was invoked with:
      | buddy        | body                         |
      | +15551234567 | Leo's birthday is in 3 days. |
    And session "imessage:T1" has transcript matching:
      | type    | message.role | message.content                                                                 |
      | message | user         | ping                                                                            |
      | message | assistant    | Here.                                                                           |
      | message | assistant    | #"\[sent here by crew red-alert from session heartbeat\] Leo's birthday is in 3 days\." |
    And session "imessage:T1" has 4 transcript entries
