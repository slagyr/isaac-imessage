@wip
Feature: iMessage per-turn context injection
  Every iMessage turn carries a trusted system block with schema
  "isaac.inbound_meta.v1" carrying provider, surface, chat_guid,
  handle, and was_mentioned. The block is appended to the system
  prompt with a framing note so the crew can ground responses
  without being tricked by user-controlled text.

  Background:
    Given default Grover setup
    And default iMessage setup

  Scenario: the trusted system block carries iMessage origin metadata
    Given the imessage source has rows:
      | rowid | chat-guid | handle       | text          | from-me |
      | 1     | T1        | +15551234567 | run a diag    | 0       |
    When the imessage inbox is polled and dispatched
    Then the last LLM request's system prompt contains an inbound_meta block matching:
      | path       | value                |
      | schema     | isaac.inbound_meta.v1 |
      | provider   | imessage             |
      | surface    | dm                   |
      | chat_guid  | T1                   |
      | handle     | +15551234567         |
      | was_mentioned | false             |
