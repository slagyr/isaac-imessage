Feature: iMessage inbound routing — a reply finds the crew that texted (isaac-ugvu)
  iMessage is one point-to-point pipe: every inbound text from a chat lands
  on imessage:<chat-guid> and the default crew. With the opt-in
  :imessage/inbound-route policy, a reply goes to the crew that last texted
  that handle (within the TTL), or to the crew named by a leading
  @<crew>. A routed reply lands on its own per-crew session,
  imessage:<chat-guid>:<crew>, never the sender's cron session. The
  outbound crew comes from the delivery record (isaac-qn4o). With no
  policy configured, nothing changes.

  New step: comms.imessage.inbound-route is "<edn>" — sets
  :imessage/inbound-route on the registered comm's slice.

  Background:
    Given default iMessage setup
    And the isaac EDN file "config/crew/red-alert.edn" exists with:
      | path  | value |
      | model | echo  |

  Scenario: with no routing policy, a reply goes where it always has
    Given the current time is "2026-10-02T10:00:00Z"
    And the isaac EDN file comm/delivery/pending/b1rd.edn exists with:
      | path            | value                                   |
      | id              | b1rd                                    |
      | comm            | imessage                                |
      | imessage/target | +15551234567                            |
      | content         | Leo's birthday is in 3 days.            |
      | crew            | red-alert                               |
      | session         | heartbeat                               |
    When the imessage delivery worker ticks
    Given the current time is "2026-10-02T10:05:00Z"
    And the imessage source has rows:
      | rowid | chat-guid | handle       | text            | from-me |
      | 1     | T1        | +15551234567 | already got one | 0       |
    When the imessage inbox is polled
    Then the polled work items are:
      | session-key | crew | input           |
      | imessage:T1 |      | already got one |

  @wip
  Scenario: a reply within the TTL goes to the crew that last texted that handle
    Given comms.imessage.inbound-route is "{:policy :both :ttl-seconds 900}"
    And the current time is "2026-10-02T10:00:00Z"
    And the isaac EDN file comm/delivery/pending/b1rd.edn exists with:
      | path            | value                        |
      | id              | b1rd                         |
      | comm            | imessage                     |
      | imessage/target | +15551234567                 |
      | content         | Leo's birthday is in 3 days. |
      | crew            | red-alert                    |
      | session         | heartbeat                    |
    When the imessage delivery worker ticks
    Given the current time is "2026-10-02T10:14:00Z"
    And the imessage source has rows:
      | rowid | chat-guid | handle       | text            | from-me |
      | 1     | T1        | +15551234567 | already got one | 0       |
    When the imessage inbox is polled
    Then the polled work items are:
      | session-key           | crew      | input           |
      | imessage:T1:red-alert | red-alert | already got one |

  @wip
  Scenario: after the TTL, a reply goes to the default session
    Given comms.imessage.inbound-route is "{:policy :both :ttl-seconds 900}"
    And the current time is "2026-10-02T10:00:00Z"
    And the isaac EDN file comm/delivery/pending/b1rd.edn exists with:
      | path            | value                        |
      | id              | b1rd                         |
      | comm            | imessage                     |
      | imessage/target | +15551234567                 |
      | content         | Leo's birthday is in 3 days. |
      | crew            | red-alert                    |
      | session         | heartbeat                    |
    When the imessage delivery worker ticks
    Given the current time is "2026-10-02T10:16:00Z"
    And the imessage source has rows:
      | rowid | chat-guid | handle       | text            | from-me |
      | 1     | T1        | +15551234567 | already got one | 0       |
    When the imessage inbox is polled
    Then the polled work items are:
      | session-key | crew | input           |
      | imessage:T1 |      | already got one |

  @wip
  Scenario: a leading @crew routes to that crew and is stripped from the text
    Given comms.imessage.inbound-route is "{:policy :both :ttl-seconds 900}"
    And the imessage source has rows:
      | rowid | chat-guid | handle       | text                      | from-me |
      | 1     | T1        | +15551234567 | @red-alert Leo is covered | 0       |
    When the imessage inbox is polled
    Then the polled work items are:
      | session-key           | crew      | input           |
      | imessage:T1:red-alert | red-alert | Leo is covered  |

  @wip
  Scenario: an unknown @name is ordinary text for the default session
    Given comms.imessage.inbound-route is "{:policy :both :ttl-seconds 900}"
    And the imessage source has rows:
      | rowid | chat-guid | handle       | text            | from-me |
      | 1     | T1        | +15551234567 | @bumblebee ahoy | 0       |
    When the imessage inbox is polled
    Then the polled work items are:
      | session-key | crew | input           |
      | imessage:T1 |      | @bumblebee ahoy |

  @wip
  Scenario: a routed crew is told which message it is answering
    Given default Grover setup in "target/imessage-grover"
    And comms.imessage.inbound-route is "{:policy :both :ttl-seconds 900}"
    And the isaac EDN file "config/crew/red-alert.edn" exists with:
      | path  | value |
      | model | echo  |
    And the current time is "2026-10-02T10:00:00Z"
    And the isaac EDN file comm/delivery/pending/b1rd.edn exists with:
      | path            | value                        |
      | id              | b1rd                         |
      | comm            | imessage                     |
      | imessage/target | +15551234567                 |
      | content         | Leo's birthday is in 3 days. |
      | crew            | red-alert                    |
      | session         | heartbeat                    |
    When the imessage delivery worker ticks
    Given the current time is "2026-10-02T10:05:00Z"
    And the following model responses are queued:
      | model | type | content   |
      | echo  | text | Noted.    |
    And the imessage source has rows:
      | rowid | chat-guid | handle       | text            | from-me |
      | 1     | T1        | +15551234567 | already got one | 0       |
    When the imessage inbox is polled and dispatched
    Then the system prompt contains "replying to your message"
    And the system prompt contains "Leo's birthday is in 3 days."
