@wip
Feature: iMessage outbound send
  The iMessage Comm impl delivers queued outbound records by
  invoking osascript against Messages.app. The generic delivery
  worker (features/delivery/queue.feature in isaac) drives retries
  and dead-lettering; the impl's only job is to construct the
  AppleScript invocation and report ok/not-ok. Scenarios stub the
  AppleScript shell-out via the imessage runner seam.

  MVP: send! returns :transient? true for every failure. Permanent
  failure short-circuit waits on isaac bean isaac-pu2x.

  Background:
    Given default iMessage setup

  Scenario: a queued iMessage delivery is sent and removed
    Given the EDN isaac file "comm/delivery/pending/abc.edn" contains:
      | path    | value         |
      | id      | abc           |
      | comm    | imessage      |
      | target  | +15551234567  |
      | content | Hello, world. |
    When the delivery worker ticks
    Then the isaac file "comm/delivery/pending/abc.edn" does not exist
    And the imessage runner was invoked with:
      | service | iMessage      |
      | buddy   | +15551234567  |
      | body    | Hello, world. |

  Scenario: an iMessage delivery to an email handle is sent
    Given the EDN isaac file "comm/delivery/pending/xyz.edn" contains:
      | path    | value             |
      | id      | xyz               |
      | comm    | imessage          |
      | target  | friend@icloud.com |
      | content | Hi there.         |
    When the delivery worker ticks
    Then the isaac file "comm/delivery/pending/xyz.edn" does not exist
    And the imessage runner was invoked with:
      | service | iMessage          |
      | buddy   | friend@icloud.com |
      | body    | Hi there.         |
