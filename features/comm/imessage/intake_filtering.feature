@wip
Feature: iMessage inbound sender filtering
  Inbound polling drops messages from senders not in
  comms.imessage.allow-from. Allow-from is fail-closed: an empty
  or missing allow-from list drops everything. Drops are logged
  at debug level so misconfigurations are diagnosable. Self
  messages (is_from_me = 1) are filtered before allow-from at
  the source layer.

  Background:
    Given default iMessage setup

  Scenario: a sender in allow-from produces a work item
    Given comms.imessage.allow-from is "+15551234567"
    And the imessage source has rows:
      | rowid | chat-guid | handle       | text  | from-me |
      | 1     | T1        | +15551234567 | hello | 0       |
    When the imessage inbox is polled
    Then the polled work items are:
      | session-key | input |
      | imessage:T1 | hello |

  Scenario: a sender not in allow-from is dropped
    Given comms.imessage.allow-from is "+15551234567"
    And the imessage source has rows:
      | rowid | chat-guid | handle       | text     | from-me |
      | 1     | T9        | +15559999999 | spam msg | 0       |
    When the imessage inbox is polled
    Then there are no polled work items
    And the log has entries matching:
      | level | event                       | handle       |
      | debug | :imessage.intake/drop-sender | +15559999999 |

  Scenario: an empty allow-from drops everything
    Given comms.imessage.allow-from is ""
    And the imessage source has rows:
      | rowid | chat-guid | handle       | text  | from-me |
      | 1     | T1        | +15551234567 | hello | 0       |
    When the imessage inbox is polled
    Then there are no polled work items
