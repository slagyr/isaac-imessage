@wip
Feature: iMessage watch snapshot hydration
  imsg watch.subscribe can push a message row before chat_message_join
  and handle.id are populated. That snapshot has a blank chat_guid and
  sender equal to destination_caller_id (the local Apple ID, not the
  remote handle). Isaac must treat blank strings as missing, re-fetch
  the row via imsg RPC, then allow-from against the hydrated sender.

  Background:
    Given default iMessage setup

  Scenario: a complete watch payload does not re-fetch history
    Given comms.imessage.allow-from is "cordelia@marigold.test"
    And the imessage source has rows:
      | rowid | chat-guid                        | handle                 | text | from-me | dest-caller           | chat-id |
      | 21    | any;-;cordelia@marigold.test     | cordelia@marigold.test | ping | 0       | logbook@marigold.test | 2       |
    When the imessage inbox is polled
    Then the polled work items are:
      | session-key                           | input | origin.handle          | origin.chat-guid             |
      | imessage:any;-;cordelia@marigold.test | ping  | cordelia@marigold.test | any;-;cordelia@marigold.test |
    And imsg did not receive method "messages.history"
    And imsg did not receive method "chats.list"

  Scenario: blank chat_guid hydrates from history by chat_id
    Given comms.imessage.allow-from is "cordelia@marigold.test"
    And the imsg history for chat 2 is:
      | rowid | chat-guid                    | handle                 | text | dest-caller           |
      | 21    | any;-;cordelia@marigold.test | cordelia@marigold.test | ping | logbook@marigold.test |
    And the imessage source has rows:
      | rowid | chat-guid | handle                | text | from-me | dest-caller           | chat-id |
      | 21    |           | logbook@marigold.test | ping | 0       | logbook@marigold.test | 2       |
    When the imessage inbox is polled
    Then the polled work items are:
      | session-key                           | input | origin.handle          | origin.chat-guid             |
      | imessage:any;-;cordelia@marigold.test | ping  | cordelia@marigold.test | any;-;cordelia@marigold.test |
    And the log has entries matching:
      | level | event                           | message-rowid |
      | :warn | :imessage.intake/hydrated-watch | 21            |

  Scenario: missing chat_id hydrates via chats.list then history
    Given comms.imessage.allow-from is "cordelia@marigold.test"
    And the imsg chat list is:
      | chat-id | guid                         |
      | 2       | any;-;cordelia@marigold.test |
    And the imsg history for chat 2 is:
      | rowid | chat-guid                    | handle                 | text | dest-caller           |
      | 21    | any;-;cordelia@marigold.test | cordelia@marigold.test | ping | logbook@marigold.test |
    And the imessage source has rows:
      | rowid | chat-guid | handle                | text | from-me | dest-caller           | chat-id |
      | 21    |           | logbook@marigold.test | ping | 0       | logbook@marigold.test |         |
    When the imessage inbox is polled
    Then the polled work items are:
      | session-key                           | origin.handle          |
      | imessage:any;-;cordelia@marigold.test | cordelia@marigold.test |

  Scenario: hydrated sender still not allow-listed is dropped
    Given comms.imessage.allow-from is "cordelia@marigold.test"
    And the imsg history for chat 2 is:
      | rowid | chat-guid                  | handle               | text | dest-caller           |
      | 21    | any;-;skybeam@marigold.test | skybeam@marigold.test | ping | logbook@marigold.test |
    And the imessage source has rows:
      | rowid | chat-guid | handle                | text | from-me | dest-caller           | chat-id |
      | 21    |           | logbook@marigold.test | ping | 0       | logbook@marigold.test | 2       |
    When the imessage inbox is polled
    Then there are no polled work items
    And the log has entries matching:
      | level | event                           | message-rowid |
      | :warn | :imessage.intake/hydrated-watch | 21            |

  Scenario: hydrate miss is incomplete-watch not drop-sender
    Given comms.imessage.allow-from is "cordelia@marigold.test"
    And the imessage source has rows:
      | rowid | chat-guid | handle                | text | from-me | dest-caller           | chat-id |
      | 21    |           | logbook@marigold.test | ping | 0       | logbook@marigold.test | 2       |
    When the imessage inbox is polled
    Then there are no polled work items
    And the log has entries matching:
      | level | event                              | message-rowid |
      | :warn | :imessage.intake/incomplete-watch  | 21            |
