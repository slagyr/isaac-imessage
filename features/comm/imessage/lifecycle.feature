Feature: iMessage comm lifecycle
  Isaac activates the iMessage Comm impl when comms.imessage config
  is present at server startup, registers it in the comm-registry
  so the delivery worker can find it, and tears it down when the
  config is removed. Config changes (service, default-target,
  poll-interval) update the live instance without restart.

  Background:
    Given an in-memory Isaac state directory "target/test-state"
    And the imessage module is declared

  Scenario: comm activates when comms.imessage config is present
    Given the EDN isaac file "config/isaac.edn" exists with:
      | path                   | value    |
      | comms.imessage.service | iMessage |
    When the Isaac process is started
    Then the comm "imessage" exists with state:
      | path | value |
    And the log has entries matching:
      | event           | comm     |
      | :comm/activated | imessage |

  Scenario: comm is removed when comms.imessage config is removed
    Given the EDN isaac file "config/isaac.edn" exists with:
      | path                   | value    |
      | comms.imessage.service | iMessage |
    And the Isaac server is started
    When config is updated:
      | path           | value   |
      | comms.imessage | #delete |
    And the isaac config is reloaded
    Then the comm "imessage" does not exist

  Scenario: a config change updates the live comm without restart
    Given the EDN isaac file "config/isaac.edn" exists with:
      | path                            | value    |
      | comms.imessage.service          | iMessage |
      | comms.imessage.poll-interval-ms | 1000     |
    And the Isaac server is started
    When config is updated:
      | path                            | value |
      | comms.imessage.poll-interval-ms | 250   |
    And the isaac config is reloaded
    Then the imessage comm has state:
      | path                    | value    |
      | status                  | :changed |
      | slice.poll-interval-ms  | 250      |
