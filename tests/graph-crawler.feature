Feature: Graph Crawler
  This feature file copies the behaviour of the unit tests.
  It instantiates the mock data bits by name.
  Although very compact way to describe the tests, it still
  does not offer a substantial advantage over plain old unit
  tests.
  It would be nice tobe able to setup ingest project features
  using the Gherkin language.

  Background:
    Given a submission

  Scenario: experiment graph scenario
    Given a assay process
    When experiment graph for process is generated
    Then graph has 12 nodes
    And graph has 3 links

  Scenario: supplementary files scenario
    Given a project
    When supplementary files graph for project is generated
    Then graph has 3 nodes
    And graph has 1 links

  Scenario: complete experiment graph scenario
    Given a assay process
    And a project
    When experiment graph is generated
    Then graph has 15 nodes
    And graph has 4 links