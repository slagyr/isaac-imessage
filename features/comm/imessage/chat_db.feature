@wip
Feature: iMessage chat.db adapter
  The chat.db adapter shells out to sqlite3 to read new rows from
  ~/Library/Messages/chat.db, normalizes each row into the
  inbox/MessageSource shape, and respects the watermark. Scenarios
  stub the sqlite3 shell-out via the imessage chat-db runner seam
  so no real DB or filesystem is required.

  Background:
    Given default iMessage setup

  Scenario: chat.db rows above the watermark become inbox messages
    Given the imessage chat.db responds with rows:
      | rowid | chat_guid | handle_id    | is_from_me | text   | date |
      | 3     | T1        | +15551234567 | 0          | second | 1003 |
      | 1     | T1        | +15551234567 | 0          | first  | 1001 |
    When the imessage inbox is polled from chat.db
    Then the polled work items are:
      | #index | input  | origin.message-rowid |
      | 0      | first  | 1                    |
      | 1      | second | 3                    |
    And the imessage watermark is 3

  Scenario: an empty chat.db result produces no work items
    Given the imessage chat.db responds with rows:
      | rowid | chat_guid | handle_id | is_from_me | text | date |
    When the imessage inbox is polled from chat.db
    Then there are no polled work items
