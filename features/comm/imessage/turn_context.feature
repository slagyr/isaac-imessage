Feature: iMessage per-turn context injection
  Every iMessage turn carries a trusted system block with schema
  "isaac.inbound_meta.v1" carrying provider, surface, chat_guid,
  handle, and was_mentioned. The block is appended to the system
  prompt with a framing note so the crew can ground responses
  without being tricked by user-controlled text.

  Background:
    Given default Grover setup in "target/imessage-grover"
    And default iMessage setup

  Scenario: trusted system block is appended for an iMessage turn
    Given the following model responses are queued:
      | model | type | content |
      | echo  | text | ok      |
    And the imessage source has rows:
      | rowid | chat-guid | handle       | text       | from-me |
      | 1     | T1        | +15551234567 | run a diag | 0       |
    When the imessage inbox is polled and dispatched
    Then the system prompt contains "isaac.inbound_meta.v1"
    And the system prompt contains "imessage"
    And the system prompt contains "T1"
    And the system prompt contains "+15551234567"
    And the system prompt contains "was_mentioned"
    And the system prompt contains "trusted metadata"
