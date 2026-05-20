Feature: iMessage inbox → work item
  imsg pushes a JSON-RPC `message` notification for each new
  inbound row in chat.db. The comm's notification handler drops
  self-messages, applies the allow-from filter, builds an Isaac
  work-item, and dispatches a turn. Scenarios install a
  FakeImsgClient so we can feed notifications directly via the
  `imessage source has rows` step.

  Background:
    Given default iMessage setup

  Scenario: a new inbound message becomes a work item
    Given the imessage source has rows:
      | rowid | chat-guid | handle       | text     | from-me |
      | 1     | T1        | +15551234567 | hi there | 0       |
    When the imessage inbox is polled
    Then the polled work items are:
      | session-key | input    | origin.handle | origin.chat-guid | origin.message-rowid |
      | imessage:T1 | hi there | +15551234567  | T1               | 1                    |

  Scenario: self-messages do not produce work items
    Given the imessage source has rows:
      | rowid | chat-guid | handle       | text          | from-me |
      | 5     | T1        | +15551234567 | i sent this   | 1       |
      | 6     | T1        | +15551234567 | they sent it  | 0       |
    When the imessage inbox is polled
    Then the polled work items are:
      | session-key | input        | origin.message-rowid |
      | imessage:T1 | they sent it | 6                    |

  Scenario: multiple inbound rows produce one work item each
    Given the imessage source has rows:
      | rowid | chat-guid | handle       | text   | from-me |
      | 10    | T1        | +15551234567 | first  | 0       |
      | 11    | T1        | +15551234567 | second | 0       |
    When the imessage inbox is polled
    Then the polled work items are:
      | #index | input  | origin.message-rowid |
      | 0      | first  | 10                   |
      | 1      | second | 11                   |
