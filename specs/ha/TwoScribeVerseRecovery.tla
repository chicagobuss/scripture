------------------------- MODULE TwoScribeVerseRecovery -------------------------
EXTENDS Naturals, Sequences, TLC

(*******************************************************************************
This deliberately small model describes one (Canon, Verse) pair.

It separates three concerns:

  * authorityHistory is the authoritative, fenced Canon history;
  * leaseA is a fallible control-plane observation that only lets B ATTEMPT
    recovery; and
  * each client owns one locally durable pending event and a staleable route.

The model is intentionally bounded: three clients, two Scribes, and three
send attempts per event.  The retry bound makes exhaustive exploration
tractable; it is not a proposed product retry limit.
*******************************************************************************)

CONSTANTS Clients, Scribes, RetryLimit

ASSUME /\ Clients = {"C1", "C2", "C3"}
       /\ Scribes = {"A", "B"}
       /\ RetryLimit = 3

A == "A"
B == "B"
None == "None"

Serving == "serving"
Recovering == "recovering"
Fresh == "fresh"
Expired == "expired"

Pending == "pending"
Acknowledged == "acknowledged"
Reclaimed == "reclaimed"

AppendRecord == [client : Clients, writer : Scribes, generation : Nat, term : Nat]

VARIABLES
    alive,
    phase,
    writer,
    generation,
    term,
    authorityHistory,
    sealed,
    recoveryCandidate,
    leaseA,
    route,
    outbox,
    ackObserved,
    attempts,
    appendSet

vars == << alive, phase, writer, generation, term, authorityHistory, sealed,
            recoveryCandidate, leaseA, route, outbox, ackObserved, attempts,
            appendSet >>

Init ==
    /\ alive = [s \in Scribes |-> TRUE]
    /\ phase = Serving
    /\ writer = A
    /\ generation = 0
    /\ term = 1
    /\ authorityHistory = [g \in {0} |-> A]
    /\ sealed = [s \in Scribes |-> FALSE]
    /\ recoveryCandidate = None
    /\ leaseA = Fresh
    /\ route = [c \in Clients |-> A]
    /\ outbox = [c \in Clients |-> Pending]
    /\ ackObserved = [c \in Clients |-> FALSE]
    /\ attempts = [c \in Clients |-> 0]
    /\ appendSet = {}

\* The liveness substrate may be wrong: this action is intentionally allowed
\* even while A is still alive.  It does not itself grant B write authority.
ExpireALease ==
    /\ leaseA = Fresh
    /\ leaseA' = Expired
    /\ UNCHANGED << alive, phase, writer, generation, term, authorityHistory,
                    sealed, recoveryCandidate, route, outbox, ackObserved,
                    attempts, appendSet >>

Kill(s) ==
    /\ s \in Scribes
    /\ alive[s]
    /\ alive' = [alive EXCEPT ![s] = FALSE]
    /\ UNCHANGED << phase, writer, generation, term, authorityHistory, sealed,
                    recoveryCandidate, leaseA, route, outbox, ackObserved,
                    attempts, appendSet >>

Return(s) ==
    /\ s \in Scribes
    /\ ~alive[s]
    /\ alive' = [alive EXCEPT ![s] = TRUE]
    /\ UNCHANGED << phase, writer, generation, term, authorityHistory, sealed,
                    recoveryCandidate, leaseA, route, outbox, ackObserved,
                    attempts, appendSet >>

\* A control-plane signal can only make B eligible to begin the fenced
\* transition.  Setting writer to None represents the sealed/recovering gap:
\* no Scribe may emit a committed acknowledgement in that interval.
BeginRecoveryByB ==
    /\ alive[B]
    /\ leaseA = Expired
    /\ phase = Serving
    /\ writer = A
    /\ phase' = Recovering
    /\ writer' = None
    /\ sealed' = [sealed EXCEPT ![A] = TRUE]
    /\ recoveryCandidate' = B
    /\ UNCHANGED << alive, generation, term, authorityHistory, leaseA, route,
                    outbox, ackObserved, attempts, appendSet >>

\* This is the only action that makes B authoritative.  It stands for the
\* lawful Holylog seal/replace/root-CAS transition, not for lease observation.
PublishBSuccessor ==
    /\ alive[B]
    /\ phase = Recovering
    /\ recoveryCandidate = B
    /\ phase' = Serving
    /\ writer' = B
    /\ generation' = generation + 1
    /\ term' = term + 1
    /\ authorityHistory' =
         [g \in 0..(generation + 1) |->
             IF g = generation + 1 THEN B ELSE authorityHistory[g]]
    /\ recoveryCandidate' = None
    /\ UNCHANGED << alive, sealed, leaseA, route, outbox, ackObserved,
                    attempts, appendSet >>

\* A client asks any reachable Scribe for a fresh route.  The returned route
\* can be stale before recovery completes; it never authorizes a write.
RefreshRoute(c) ==
    /\ c \in Clients
    /\ outbox[c] = Pending
    /\ (\E s \in Scribes : alive[s])
    /\ writer # None
    /\ route' = [route EXCEPT ![c] = writer]
    /\ UNCHANGED << alive, phase, writer, generation, term, authorityHistory,
                    sealed, recoveryCandidate, leaseA, outbox, ackObserved,
                    attempts, appendSet >>

\* A route is only a hint.  A send to a dead, recovering, or stale target
\* consumes one bounded model attempt but cannot append or acknowledge.
AttemptDeniedOrTimedOut(c) ==
    /\ c \in Clients
    /\ outbox[c] = Pending
    /\ attempts[c] < RetryLimit
    /\ (writer = None \/ ~alive[route[c]] \/ route[c] # writer)
    /\ attempts' = [attempts EXCEPT ![c] = @ + 1]
    /\ UNCHANGED << alive, phase, writer, generation, term, authorityHistory,
                    sealed, recoveryCandidate, leaseA, route, outbox,
                    ackObserved, appendSet >>

\* The Scribe commits but the client loses the reply.  This is deliberately
\* different from a rejected attempt: the event remains locally pending and a
\* later retry may create a duplicate physical append with the same event ID.
\* appendSet intentionally collapses repeated copies with identical
\* (client, writer, generation, term) evidence to keep the state space finite.
AttemptCommitNoReply(c) ==
    /\ c \in Clients
    /\ outbox[c] = Pending
    /\ attempts[c] < RetryLimit
    /\ writer # None
    /\ alive[route[c]]
    /\ route[c] = writer
    /\ attempts' = [attempts EXCEPT ![c] = @ + 1]
    /\ appendSet' = appendSet \cup
          {[client |-> c, writer |-> writer, generation |-> generation, term |-> term]}
    /\ UNCHANGED << alive, phase, writer, generation, term, authorityHistory,
                    sealed, recoveryCandidate, leaseA, route, outbox,
                    ackObserved >>

\* A lawful Scribe commits and the committed acknowledgement reaches the
\* client.  Only this transition permits outbox reclamation later.
AttemptCommitAndAck(c) ==
    /\ c \in Clients
    /\ outbox[c] = Pending
    /\ attempts[c] < RetryLimit
    /\ writer # None
    /\ alive[route[c]]
    /\ route[c] = writer
    /\ attempts' = [attempts EXCEPT ![c] = @ + 1]
    /\ appendSet' = appendSet \cup
          {[client |-> c, writer |-> writer, generation |-> generation, term |-> term]}
    /\ outbox' = [outbox EXCEPT ![c] = Acknowledged]
    /\ ackObserved' = [ackObserved EXCEPT ![c] = TRUE]
    /\ UNCHANGED << alive, phase, writer, generation, term, authorityHistory,
                    sealed, recoveryCandidate, leaseA, route >>

\* This is the client observing a previously lost committed reply through an
\* idempotency/readback mechanism.  It may reclaim only after that observation.
ObservePriorCommit(c) ==
    /\ c \in Clients
    /\ outbox[c] = Pending
    /\ \E record \in appendSet : record.client = c
    /\ outbox' = [outbox EXCEPT ![c] = Acknowledged]
    /\ ackObserved' = [ackObserved EXCEPT ![c] = TRUE]
    /\ UNCHANGED << alive, phase, writer, generation, term, authorityHistory,
                    sealed, recoveryCandidate, leaseA, route, attempts,
                    appendSet >>

ReclaimAcknowledged(c) ==
    /\ c \in Clients
    /\ outbox[c] = Acknowledged
    /\ ackObserved[c]
    /\ outbox' = [outbox EXCEPT ![c] = Reclaimed]
    /\ UNCHANGED << alive, phase, writer, generation, term, authorityHistory,
                    sealed, recoveryCandidate, leaseA, route, ackObserved,
                    attempts, appendSet >>

Next ==
    \/ ExpireALease
    \/ \E s \in Scribes : Kill(s)
    \/ \E s \in Scribes : Return(s)
    \/ BeginRecoveryByB
    \/ PublishBSuccessor
    \/ \E c \in Clients : RefreshRoute(c)
    \/ \E c \in Clients : AttemptDeniedOrTimedOut(c)
    \/ \E c \in Clients : AttemptCommitNoReply(c)
    \/ \E c \in Clients : AttemptCommitAndAck(c)
    \/ \E c \in Clients : ObservePriorCommit(c)
    \/ \E c \in Clients : ReclaimAcknowledged(c)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ alive \in [Scribes -> BOOLEAN]
    /\ phase \in {Serving, Recovering}
    /\ writer \in Scribes \cup {None}
    /\ generation \in Nat
    /\ term \in Nat
    /\ authorityHistory \in [0..generation -> Scribes]
    /\ sealed \in [Scribes -> BOOLEAN]
    /\ recoveryCandidate \in Scribes \cup {None}
    /\ leaseA \in {Fresh, Expired}
    /\ route \in [Clients -> Scribes]
    /\ outbox \in [Clients -> {Pending, Acknowledged, Reclaimed}]
    /\ ackObserved \in [Clients -> BOOLEAN]
    /\ attempts \in [Clients -> 0..RetryLimit]
    /\ appendSet \subseteq AppendRecord

OneAuthorityPerGeneration ==
    \A g \in DOMAIN authorityHistory : authorityHistory[g] \in Scribes

EveryPhysicalAppendWasLawful ==
    \A record \in appendSet :
        /\ record.generation \in DOMAIN authorityHistory
        /\ record.writer = authorityHistory[record.generation]

AcknowledgementImpliesCommittedAppend ==
    \A c \in Clients :
        ackObserved[c] => \E record \in appendSet : record.client = c

OnlyAcknowledgedEventsAreReclaimed ==
    \A c \in Clients : outbox[c] = Reclaimed => ackObserved[c]

RecoveryGapHasNoWriter ==
    phase = Recovering => writer = None

StaleAIsFencedAfterBPublication ==
    generation > 0 => authorityHistory[generation] = B

=============================================================================
