Feature: iMessage poller resilience
  The iMessage poller runs on a fixed interval, draining new
  inbound messages on each tick. A transient source failure on one
  tick must not bring the loop down — subsequent ticks keep
  draining. Cadence and backoff fidelity stays in unit specs;
  the feature pins the operator contract: the poller doesn't die.

  Background:
    Given default iMessage setup

  Scenario: the poller keeps draining after a transient source failure
    Given the imessage source raises on the next 1 polls then succeeds
    And the imessage source has rows:
      | rowid | chat-guid | handle       | text  | from-me |
      | 1     | T1        | +15551234567 | hello | 0       |
    When the imessage poller is ticked 2 times
    Then the imessage source was polled 2 times
    And the polled work items are:
      | input | origin.message-rowid |
      | hello | 1                    |
