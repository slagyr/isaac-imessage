Feature: iMessage outbound send
  The iMessage Comm impl delivers queued outbound records by
  invoking osascript against Messages.app. The generic delivery
  worker (features/delivery/queue.feature in isaac) drives retries
  and dead-lettering; the impl's only job is to construct the
  AppleScript invocation and report ok/not-ok. Scenarios stub the
  AppleScript shell-out via the imessage runner seam.

  imsg answers a failed send with a structured error :data map that
  states whether re-sending is safe. Isaac honours that answer; the
  message regex is only the fallback for errors that carry no :data.

  Background:
    Given default iMessage setup

  Scenario: a queued iMessage delivery is sent and removed
    Given the isaac EDN file comm/delivery/pending/7f3a.edn exists with:
      | path    | value         |
      | id              | 7f3a           |
      | comm            | imessage      |
      | imessage/target | +15551234567  |
      | content         | Hello, world. |
    When the imessage delivery worker ticks
    Then the isaac file "comm/delivery/pending/7f3a.edn" does not exist
    And the imessage runner was invoked with:
      | buddy        | body          |
      | +15551234567 | Hello, world. |

  Scenario: an iMessage delivery to an email handle is sent
    Given the isaac EDN file comm/delivery/pending/9c2e.edn exists with:
      | path    | value             |
      | id              | 9c2e               |
      | comm            | imessage          |
      | imessage/target | friend@icloud.com |
      | content         | Hi there.         |
    When the imessage delivery worker ticks
    Then the isaac file "comm/delivery/pending/9c2e.edn" does not exist
    And the imessage runner was invoked with:
      | buddy             | body      |
      | friend@icloud.com | Hi there. |

  Scenario: no configured service leaves the transport to imsg (isaac-2zs0)
    An explicit service is the operator's decision, not Isaac's default.
    Configuring the obvious "iMessage" makes every send fail through
    imsg's AppleScript transport; with no :service imsg picks for itself.
    Given the isaac EDN file comm/delivery/pending/4d1b.edn exists with:
      | path            | value             |
      | id              | 4d1b              |
      | comm            | imessage          |
      | imessage/target | friend@icloud.com |
      | content         | No service here.  |
    When the imessage delivery worker ticks
    Then the imessage runner was invoked with:
      | service | buddy             | body             |
      |         | friend@icloud.com | No service here. |

  Scenario: an operator-configured service is passed through lowercased (isaac-2zs0)
    Given comms.imessage.service is auto
    And the isaac EDN file comm/delivery/pending/5e2c.edn exists with:
      | path            | value             |
      | id              | 5e2c              |
      | comm            | imessage          |
      | imessage/target | friend@icloud.com |
      | content         | Auto, please.     |
    When the imessage delivery worker ticks
    Then the imessage runner was invoked with:
      | service | buddy             | body          |
      | auto    | friend@icloud.com | Auto, please. |

  Scenario: a send imsg marked not retry-safe is never sent again (isaac-fkjq)
    -32001 "Delivery outcome unknown" with retry_safe false and
    disposition may_have_completed means the message MAY already have gone
    out. Retrying sends a human the same message again, so the delivery
    dead-letters on the first attempt instead.
    Given the imsg send fails with:
      | code   | message                  | disposition        | retry_safe | detail                                                                                          |
      | -32001 | Delivery outcome unknown | may_have_completed | false      | Messages automation returned success, but no matching outgoing text row was observed in 8 seconds |
    And the isaac EDN file comm/delivery/pending/c972.edn exists with:
      | path            | value             |
      | id              | c972              |
      | comm            | imessage          |
      | imessage/target | friend@icloud.com |
      | content         | Only once.        |
    When the imessage delivery worker ticks
    Then the imessage runner send count is 1
    And the isaac file "comm/delivery/pending/c972.edn" does not exist
    And the isaac file "comm/delivery/failed/c972.edn" EDN contains:
      | path     | value | #comment                              |
      | attempts | 0     | dead-lettered on the first attempt    |
      | id       | c972  |                                       |
    And the log has entries matching:
      | level | event                        | id   | reason     |
      | error | :comm.delivery/dead-lettered | c972 | :permanent |

  Scenario: a send imsg marked retry-safe is retried with backoff (isaac-fkjq)
    The structured answer cuts both ways — retry_safe true keeps the
    existing transient path, so honouring :data is not a blanket
    "never retry".
    Given the imsg send fails with:
      | code   | message                | disposition  | retry_safe |
      | -32001 | Messages is not running | did_not_send | true       |
    And the isaac EDN file comm/delivery/pending/8a3f.edn exists with:
      | path            | value             |
      | id              | 8a3f              |
      | comm            | imessage          |
      | imessage/target | friend@icloud.com |
      | content         | Try again later.  |
    When the imessage delivery worker ticks
    Then the isaac file "comm/delivery/pending/8a3f.edn" EDN contains:
      | path     | value | #comment                          |
      | attempts | 1     | transient; queued for another try |
