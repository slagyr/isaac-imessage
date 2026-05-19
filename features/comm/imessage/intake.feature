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

  Scenario: self-messages do not produce work items
    Given the imessage source has rows:
      | rowid | chat-guid | handle       | text          | from-me |
      | 5     | T1        | +15551234567 | i sent this   | 1       |
      | 6     | T1        | +15551234567 | they sent it  | 0       |
    When the imessage inbox is polled
    Then the polled work items are:
      | session-key | input        | origin.message-rowid |
      | imessage:T1 | they sent it | 6                    |
    And the imessage watermark is 6

  Scenario: multiple inbound rows produce work items in rowid order
    Given the imessage source has rows:
      | rowid | chat-guid | handle       | text   | from-me |
      | 11    | T1        | +15551234567 | second | 0       |
      | 10    | T1        | +15551234567 | first  | 0       |
    When the imessage inbox is polled
    Then the polled work items are:
      | #index | input  | origin.message-rowid |
      | 0      | first  | 10                   |
      | 1      | second | 11                   |
    And the imessage watermark is 11

  Scenario: previously seen rows are skipped on the next poll
    Given the EDN isaac file "imessage/state.edn" exists with:
      | path                    | value |
      | watermark.message-rowid | 10    |
    And the imessage source has rows:
      | rowid | chat-guid | handle       | text     | from-me |
      | 8     | T1        | +15551234567 | older    | 0       |
      | 10    | T1        | +15551234567 | seen     | 0       |
      | 11    | T1        | +15551234567 | brand new | 0      |
    When the imessage inbox is polled
    Then the polled work items are:
      | input     | origin.message-rowid |
      | brand new | 11                   |
    And the imessage watermark is 11
