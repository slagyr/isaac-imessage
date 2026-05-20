Feature: iMessage chat → session routing
  Each iMessage chat (chat_guid) maps to a single Isaac session.
  The session-key is derived as "imessage:<chat-guid>" and is
  stable across restarts because imsg pushes the same chat_guid
  for every message in a thread.

  Background:
    Given default iMessage setup

  Scenario: a new chat is routed to imessage:<chat-guid>
    Given the imessage source has rows:
      | rowid | chat-guid | handle       | text  | from-me |
      | 1     | T7        | +15551234567 | hello | 0       |
    When the imessage inbox is polled
    Then the polled work items are:
      | session-key | origin.chat-guid |
      | imessage:T7 | T7               |
