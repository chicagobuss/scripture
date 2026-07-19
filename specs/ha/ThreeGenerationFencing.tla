------------------------- MODULE ThreeGenerationFencing -------------------------
EXTENDS Naturals, FiniteSets, TLC

(*******************************************************************************
A deliberately small harness whose only job is to falsify the send-acceptance
rule used by TwoScribeVerseRecoveryNetwork.

That module lets authority advance at most once (A -> B).  Endpoint identity is
therefore an accidentally perfect proxy for epoch identity there: a route
naming A is stale exactly when A is not the writer, so an endpoint-only
acceptance rule looks correct and no invariant can distinguish the two rules.

Here authority may alternate, so A can lawfully regain writership in a later
generation.  A route snapshot captured in generation 0 then names an endpoint
that is once again the live writer while describing a dead epoch.  Only an
acceptance rule that compares epochs rejects it.

EnforceEpochFence selects the rule under test, so the negative case is a
configuration rather than a forked copy of this module:

  ThreeGenerationFencing.cfg        EnforceEpochFence = TRUE   expect: no error
  ThreeGenerationFencingMutant.cfg  EnforceEpochFence = FALSE  expect:
                                    CommitCarriesCurrentEpochRoute violated

The outbox/ACK lifecycle is intentionally absent.  This module is not a
product model; it exists so that the parent module's guard has a test that
can fail.  Keep LawfulSendGuard here in step with the parent.
*******************************************************************************)

CONSTANTS Clients, Scribes, RetryLimit, EnforceEpochFence

A == "A"
B == "B"
None == "None"

Serving == "serving"
Recovering == "recovering"

Generations == 0..2
Terms == 1..3
AttemptNumbers == 1..RetryLimit

RouteHint == [endpoint : Scribes, generation : Generations, term : Terms]

\* routeGeneration/routeTerm record the epoch the packet was built from,
\* independently of the epoch that accepted it.  Without this provenance the
\* fence invariant would compare the accepting state against itself.
AppendRecord == [client : Clients, writer : Scribes, generation : Generations,
                 term : Terms, routeGeneration : Generations,
                 routeTerm : Terms]

RoutePacket == [kind : {"route"}, client : Clients, endpoint : Scribes,
                generation : Generations, term : Terms]
SendPacket == [kind : {"send"}, client : Clients, endpoint : Scribes,
               generation : Generations, term : Terms, attempt : AttemptNumbers]
Packet == RoutePacket \cup SendPacket

VARIABLES
    alive,
    phase,
    writer,
    generation,
    term,
    authorityHistory,
    recoveryCandidate,
    route,
    attempts,
    appendSet,
    network

vars == << alive, phase, writer, generation, term, authorityHistory,
            recoveryCandidate, route, attempts, appendSet, network >>

Init ==
    /\ alive = [s \in Scribes |-> TRUE]
    /\ phase = Serving
    /\ writer = A
    /\ generation = 0
    /\ term = 1
    /\ authorityHistory = [g \in {0} |-> A]
    /\ recoveryCandidate = None
    /\ route = [c \in Clients |->
          [endpoint |-> A, generation |-> 0, term |-> 1]]
    /\ attempts = [c \in Clients |-> 0]
    /\ appendSet = {}
    /\ network = {}

EmitRouteSnapshot(c) ==
    /\ c \in Clients
    /\ writer # None
    /\ network' = network \cup
         {[kind |-> "route", client |-> c, endpoint |-> writer,
           generation |-> generation, term |-> term]}
    /\ UNCHANGED << alive, phase, writer, generation, term, authorityHistory,
                    recoveryCandidate, route, attempts, appendSet >>

DeliverRouteSnapshot(packet) ==
    /\ packet \in network
    /\ packet.kind = "route"
    /\ network' = network \ {packet}
    /\ route' = [route EXCEPT
          ![packet.client] = [endpoint |-> packet.endpoint,
                              generation |-> packet.generation,
                              term |-> packet.term]]
    /\ UNCHANGED << alive, phase, writer, generation, term, authorityHistory,
                    recoveryCandidate, attempts, appendSet >>

ClientSend(c) ==
    /\ c \in Clients
    /\ attempts[c] < RetryLimit
    /\ attempts' = [attempts EXCEPT ![c] = @ + 1]
    /\ network' = network \cup
         {[kind |-> "send", client |-> c,
           endpoint |-> route[c].endpoint,
           generation |-> route[c].generation,
           term |-> route[c].term,
           attempt |-> attempts[c] + 1]}
    /\ UNCHANGED << alive, phase, writer, generation, term, authorityHistory,
                    recoveryCandidate, route, appendSet >>

\* The rule under test.  With EnforceEpochFence = FALSE this degrades to the
\* endpoint-only rule, which is indistinguishable from the epoch rule in the
\* parent module and wrong here.
LawfulSendGuard(packet) ==
    /\ writer # None
    /\ alive[packet.endpoint]
    /\ packet.endpoint = writer
    /\ (EnforceEpochFence =>
          /\ packet.generation = generation
          /\ packet.term = term)

DeliverRejectedOrUnavailableSend(packet) ==
    /\ packet \in network
    /\ packet.kind = "send"
    /\ ~LawfulSendGuard(packet)
    /\ network' = network \ {packet}
    /\ UNCHANGED << alive, phase, writer, generation, term, authorityHistory,
                    recoveryCandidate, route, attempts, appendSet >>

DeliverLawfulSend(packet) ==
    /\ packet \in network
    /\ packet.kind = "send"
    /\ LawfulSendGuard(packet)
    /\ network' = network \ {packet}
    /\ appendSet' = appendSet \cup
         {[client |-> packet.client, writer |-> writer,
           generation |-> generation, term |-> term,
           routeGeneration |-> packet.generation,
           routeTerm |-> packet.term]}
    /\ UNCHANGED << alive, phase, writer, generation, term, authorityHistory,
                    recoveryCandidate, route, attempts >>

DropPacket(packet) ==
    /\ packet \in network
    /\ network' = network \ {packet}
    /\ UNCHANGED << alive, phase, writer, generation, term, authorityHistory,
                    recoveryCandidate, route, attempts, appendSet >>

\* Generic over the peer, so authority can alternate.  As in the parent, the
\* failure observation may be false: no liveness claim is made here.
BeginRecovery(s) ==
    /\ s \in Scribes
    /\ alive[s]
    /\ phase = Serving
    /\ writer # None
    /\ s # writer
    /\ generation + 1 \in Generations
    /\ term + 1 \in Terms
    /\ phase' = Recovering
    /\ writer' = None
    /\ recoveryCandidate' = s
    /\ UNCHANGED << alive, generation, term, authorityHistory, route, attempts,
                    appendSet, network >>

PublishSuccessor ==
    /\ recoveryCandidate # None
    /\ alive[recoveryCandidate]
    /\ phase = Recovering
    /\ phase' = Serving
    /\ writer' = recoveryCandidate
    /\ generation' = generation + 1
    /\ term' = term + 1
    /\ authorityHistory' =
         [g \in 0..(generation + 1) |->
             IF g = generation + 1 THEN recoveryCandidate
             ELSE authorityHistory[g]]
    /\ recoveryCandidate' = None
    /\ UNCHANGED << alive, route, attempts, appendSet, network >>

Next ==
    \/ \E c \in Clients : EmitRouteSnapshot(c)
    \/ \E packet \in network : DeliverRouteSnapshot(packet)
    \/ \E c \in Clients : ClientSend(c)
    \/ \E packet \in network : DeliverRejectedOrUnavailableSend(packet)
    \/ \E packet \in network : DeliverLawfulSend(packet)
    \/ \E packet \in network : DropPacket(packet)
    \/ \E s \in Scribes : BeginRecovery(s)
    \/ PublishSuccessor

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ alive \in [Scribes -> BOOLEAN]
    /\ phase \in {Serving, Recovering}
    /\ writer \in Scribes \cup {None}
    /\ generation \in Generations
    /\ term \in Terms
    /\ authorityHistory \in [0..generation -> Scribes]
    /\ recoveryCandidate \in Scribes \cup {None}
    /\ route \in [Clients -> RouteHint]
    /\ attempts \in [Clients -> 0..RetryLimit]
    /\ appendSet \subseteq AppendRecord
    /\ network \subseteq Packet

EveryAppendMatchesPublishedAuthority ==
    \A record \in appendSet :
        /\ record.generation \in DOMAIN authorityHistory
        /\ record.writer = authorityHistory[record.generation]

\* The property the parent module cannot falsify.  A commit must have been
\* authorised by the epoch that accepted it, not merely addressed to a Scribe
\* that happens to be the writer again.
CommitCarriesCurrentEpochRoute ==
    \A record \in appendSet :
        /\ record.routeGeneration = record.generation
        /\ record.routeTerm = record.term

RecoveryGapHasNoWriter == phase = Recovering => writer = None

\* Sanity check on the harness: if authority never actually alternates back to
\* a previous Scribe, the mutant would pass for the wrong reason.  TLC reports
\* this as a violation, which is the intended signal that the state space does
\* reach the interesting shape.
AuthorityNeverRepeats ==
    ~(\E g1, g2 \in DOMAIN authorityHistory :
        /\ g1 < g2
        /\ authorityHistory[g1] = authorityHistory[g2])

\* Same bounding rationale as the parent module: a state-space bound, not a
\* network capacity claim.  Two in-flight packets suffice for the stale-route
\* race (one surviving snapshot plus one send).
NetworkBound == Cardinality(network) <= 2

ClientSymmetry == Permutations(Clients)

=============================================================================
