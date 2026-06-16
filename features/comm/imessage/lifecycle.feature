Feature: iMessage comm lifecycle
  Isaac activates the iMessage Comm impl when comms.imessage
  config is present at server startup, registers it in the
  comm-registry so the delivery worker can find it, and tears it
  down when the config is removed. Config changes (message-cap,
  allow-from, etc.) update the live instance in place without
  restarting the imsg subprocess.

  Background:
    Given iMessage lifecycle setup

  Scenario: comm activates when comms.imessage config is present
    Given config:
      | comms.imessage.imessage/service | iMessage |
    And the imessage Isaac server is started
    Then the comm "imessage" exists with state:
      | path | value |
    And the log has entries matching:
      | event           | comm     |
      | :comm/activated | imessage |

  Scenario: comm is removed when comms.imessage config is removed
    Given config:
      | comms.imessage.imessage/service | iMessage |
    And the imessage Isaac server is started
    When config is updated:
      | path           | value   |
      | comms.imessage | #delete |
    Then the comm "imessage" does not exist

  Scenario: a config change updates the live comm without restart
    Given config:
      | comms.imessage.imessage/service     | iMessage |
      | comms.imessage.imessage/message-cap | 2000     |
    And the imessage Isaac server is started
    When config is updated:
      | path                                | value |
      | comms.imessage.imessage/message-cap | 500   |
    Then the imessage comm has state:
      | path                       | value    |
      | status                     | :changed |
      | slice.imessage/message-cap | 500      |