Feature: iMessage end-to-end reply
  An inbound iMessage triggers an Isaac turn, the crew produces a
  response, the response is enqueued as a delivery record (chunked
  if it exceeds message-cap), and the delivery worker sends each
  chunk back to the originating handle through the AppleScript
  runner. This is the headline MVP acceptance goal from PLAN.md.

  Background:
    Given default Grover setup
    And default iMessage setup

  Scenario: inbound message → crew turn → outbound iMessage reply
    Given the following model responses are queued:
      | model | type | content             |
      | echo  | text | thanks for the ping |
    And the imessage source has rows:
      | rowid | chat-guid | handle       | text  | from-me |
      | 1     | T1        | +15551234567 | ping  | 0       |
    When the imessage inbox is polled and dispatched
    And the imessage delivery worker ticks
    Then the imessage runner was invoked with:
      | service  | buddy        | body                |
      | iMessage | +15551234567 | thanks for the ping |

  Scenario: a crew response longer than the message-cap is chunked into separate sends
    Given comms.imessage.message-cap is 20
    And the following model responses are queued:
      | model | type | content                                          |
      | echo  | text | one two three four five six seven eight nine ten |
    And the imessage source has rows:
      | rowid | chat-guid | handle       | text | from-me |
      | 1     | T1        | +15551234567 | go   | 0       |
    When the imessage inbox is polled and dispatched
    And the imessage delivery worker ticks
    Then the imessage runner was invoked with:
      | body                |
      | one two three four  |
      | five six seven      |
      | eight nine ten      |
