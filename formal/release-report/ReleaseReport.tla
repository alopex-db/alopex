---------------------------- MODULE ReleaseReport ----------------------------
EXTENDS Naturals

CONSTANT
    \* @type: Int;
    RetryLimit

VerificationStates == {"pending", "success", "failure"}

VARIABLES \* @type: Str;
          verification,
          \* @type: Bool;
          resultSaved,
          \* @type: Bool;
          reportGenerated,
          \* @type: Bool;
          skipObserved,
          \* @type: Bool;
          publishRequested,
          \* @type: Bool;
          published,
          \* @type: Bool;
          failureNotified,
          \* @type: Int;
          attempts

vars == << verification,
           resultSaved,
           reportGenerated,
           skipObserved,
           publishRequested,
           published,
           failureNotified,
           attempts >>

Init ==
    /\ verification = "pending"
    /\ resultSaved = FALSE
    /\ reportGenerated = FALSE
    /\ skipObserved = FALSE
    /\ publishRequested = FALSE
    /\ published = FALSE
    /\ failureNotified = FALSE
    /\ attempts = 0

VerifySuccess ==
    /\ verification = "pending"
    /\ attempts < RetryLimit + 1
    /\ verification' = "success"
    /\ resultSaved' = TRUE
    /\ skipObserved' \in BOOLEAN
    /\ attempts' = attempts + 1
    /\ UNCHANGED << reportGenerated, publishRequested, published, failureNotified >>

VerifyFailure ==
    /\ verification = "pending"
    /\ attempts < RetryLimit + 1
    /\ verification' = "failure"
    /\ resultSaved' = TRUE
    /\ skipObserved' \in BOOLEAN
    /\ attempts' = attempts + 1
    /\ UNCHANGED << reportGenerated, publishRequested, published, failureNotified >>

GenerateReport ==
    /\ verification # "pending"
    /\ resultSaved
    /\ reportGenerated' = TRUE
    /\ UNCHANGED << verification, resultSaved, skipObserved, publishRequested,
                    published, failureNotified, attempts >>

RequestPublish ==
    /\ verification = "success"
    /\ reportGenerated
    /\ ~skipObserved
    /\ publishRequested' = TRUE
    /\ UNCHANGED << verification, resultSaved, reportGenerated, skipObserved,
                    published, failureNotified, attempts >>

Publish ==
    /\ verification = "success"
    /\ reportGenerated
    /\ ~skipObserved
    /\ publishRequested
    /\ published' = TRUE
    /\ UNCHANGED << verification, resultSaved, reportGenerated, skipObserved,
                    publishRequested, failureNotified, attempts >>

NotifyFailure ==
    /\ verification = "failure"
    /\ resultSaved
    /\ failureNotified' = TRUE
    /\ UNCHANGED << verification, resultSaved, reportGenerated, skipObserved,
                    publishRequested, published, attempts >>

Retry ==
    /\ verification = "failure"
    /\ attempts < RetryLimit + 1
    /\ verification' = "pending"
    /\ reportGenerated' = FALSE
    /\ skipObserved' = FALSE
    /\ publishRequested' = FALSE
    /\ UNCHANGED << resultSaved, published, failureNotified, attempts >>

Stutter == UNCHANGED vars

Next == VerifySuccess \/ VerifyFailure \/ GenerateReport \/ RequestPublish \/
        Publish \/ NotifyFailure \/ Retry \/ Stutter

TypeOK ==
    /\ RetryLimit \in Nat
    /\ verification \in VerificationStates
    /\ resultSaved \in BOOLEAN
    /\ reportGenerated \in BOOLEAN
    /\ skipObserved \in BOOLEAN
    /\ publishRequested \in BOOLEAN
    /\ published \in BOOLEAN
    /\ failureNotified \in BOOLEAN
    /\ attempts \in 0..(RetryLimit + 1)

ResultBeforeReport == reportGenerated => resultSaved
SuccessOnlyPublication == published => verification = "success"
NoSkippedPublication == published => ~skipObserved
ExplicitPublication == published => publishRequested
ReportBeforePublication == published => reportGenerated
FailureEvidenceRetained == verification = "failure" => resultSaved
FailureNotificationIsTruthful == failureNotified => resultSaved

=============================================================================
