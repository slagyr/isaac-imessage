@wip
Feature: iMessage poller cadence and backoff
  The iMessage poller runs on a fixed interval, draining new
  inbound messages on each tick. On consecutive failures (DB read
  errors, etc.) it backs off exponentially; a successful tick
  resets the backoff. Shutdown is prompt — a stop signal
  interrupts the sleep.

  Background:
    Given default iMessage setup

  Scenario: the poller drains messages on each tick at the configured interval
    Given the imessage poll-interval-ms is 50
    And the imessage source has rows:
      | rowid | chat-guid | handle       | text  | from-me |
      | 1     | T1        | +15551234567 | hello | 0       |
    When the imessage poller runs for 200 ms
    Then the imessage source was polled at least 3 times
    And the imessage source was polled at most 6 times

  Scenario: consecutive poll failures back off exponentially
    Given the imessage source raises on each poll
    And the imessage poll-interval-ms is 10
    When the imessage poller runs for 500 ms
    Then the imessage poll delays were:
      | #index | delay-ms |
      | 0      | 10       |
      | 1      | 20       |
      | 2      | 40       |
      | 3      | 80       |

  Scenario: a successful poll resets the backoff
    Given the imessage source raises on the next 2 polls then succeeds
    And the imessage poll-interval-ms is 10
    When the imessage poller runs for 500 ms
    Then the imessage poll delays return to 10 ms after the first success
