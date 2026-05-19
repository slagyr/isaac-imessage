Feature: iMessage chat → session routing
  Each iMessage chat (chat_guid) maps to one Isaac session. New
  chats get a default session-key of "imessage:<chat-guid>" on
  first contact. Known chats reuse whatever session-key was
  recorded earlier — preserving operator-edited keys.

  Background:
    Given default iMessage setup

  Scenario: a new chat gets the default session-key on first contact
    Given the imessage source has rows:
      | rowid | chat-guid | handle       | text  | from-me |
      | 1     | T7        | +15551234567 | hello | 0       |
    When the imessage inbox is polled
    Then the polled work items are:
      | session-key | origin.chat-guid |
      | imessage:T7 | T7               |

  @wip
  Scenario: a known chat keeps its existing session-key
    Given the imessage state has chats:
      | chat-guid | handle       | session-key |
      | T7        | +15551234567 | custom-T7   |
    And the imessage source has rows:
      | rowid | chat-guid | handle       | text   | from-me |
      | 5     | T7        | +15551234567 | reuses | 0       |
    When the imessage inbox is polled
    Then the polled work items are:
      | session-key | origin.chat-guid |
      | custom-T7   | T7               |
