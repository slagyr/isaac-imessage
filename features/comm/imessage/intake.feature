Feature: iMessage inbox poll → work item
  Polling the imessage inbox reads new rows from the message source,
  drops self-messages, routes each remaining row to an Isaac session,
  and advances the watermark so the next poll skips them. Scenarios
  install an in-memory MessageSource via the imessage source seam.

  Background:
    Given default iMessage setup

  Scenario: a new inbound message becomes a work item
    Given the imessage source has rows:
      | rowid | chat-guid | handle       | text     | from-me |
      | 1     | T1        | +15551234567 | hi there | 0       |
    When the imessage inbox is polled
    Then the polled work items are:
      | session-key | input    | origin.handle | origin.thread-id | origin.message-rowid |
      | imessage:T1 | hi there | +15551234567  | T1               | 1                    |
    And the imessage watermark is 1
