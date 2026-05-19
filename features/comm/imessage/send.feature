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
    Given the EDN isaac file "comm/delivery/pending/7f3a.edn" contains:
      | path    | value         |
      | id      | 7f3a           |
      | comm    | imessage      |
      | target  | +15551234567  |
      | content | Hello, world. |
    When the imessage delivery worker ticks
    Then the isaac file "comm/delivery/pending/7f3a.edn" does not exist
    And the imessage runner was invoked with:
      | service  | buddy        | body          |
      | iMessage | +15551234567 | Hello, world. |

  Scenario: an iMessage delivery to an email handle is sent
    Given the EDN isaac file "comm/delivery/pending/9c2e.edn" contains:
      | path    | value             |
      | id      | 9c2e               |
      | comm    | imessage          |
      | target  | friend@icloud.com |
      | content | Hi there.         |
    When the imessage delivery worker ticks
    Then the isaac file "comm/delivery/pending/9c2e.edn" does not exist
    And the imessage runner was invoked with:
      | service  | buddy             | body      |
      | iMessage | friend@icloud.com | Hi there. |
