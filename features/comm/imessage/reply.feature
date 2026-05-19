@wip
Feature: iMessage end-to-end reply
  An inbound iMessage triggers an Isaac turn, the crew produces a
  response, the response is enqueued for delivery, and the
  delivery worker sends it back to the originating handle through
  the AppleScript runner. This is the headline MVP acceptance goal
  from PLAN.md.

  Background:
    Given default Grover setup
    And default iMessage setup

  Scenario: inbound message → crew turn → outbound iMessage reply
    Given the following model responses are queued:
      | response       |
      | thanks for the ping |
    And the imessage source has rows:
      | rowid | chat-guid | handle       | text  | from-me |
      | 1     | T1        | +15551234567 | ping  | 0       |
    When the imessage inbox is polled and dispatched
    And the imessage delivery worker ticks
    Then the imessage runner was invoked with:
      | service  | buddy        | body                |
      | iMessage | +15551234567 | thanks for the ping |
    And session "imessage:T1" exists

  Scenario: a crew response longer than the message cap is split
    Given comms.imessage.message-cap is 20
    And the following model responses are queued:
      | response                                              |
      | one two three four five six seven eight nine ten      |
    And the imessage source has rows:
      | rowid | chat-guid | handle       | text  | from-me |
      | 1     | T1        | +15551234567 | go    | 0       |
    When the imessage inbox is polled and dispatched
    And the imessage delivery worker ticks
    Then the imessage runner was invoked with:
      | #index | body                |
      | 0      | one two three four  |
      | 1      | five six seven      |
      | 2      | eight nine ten      |
