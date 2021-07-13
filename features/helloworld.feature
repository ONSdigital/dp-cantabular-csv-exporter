Feature: Helloworld



  Scenario: Posting and checking a response
    When these hello events are consumed:
            | InstanceID | 
            | Tim           |
    Then I should receive a hello-world response