--------------------- MODULE TwoScribeVerseRecoveryNetwork ---------------------
EXTENDS Naturals, FiniteSets, TLC

(*******************************************************************************
The networked refinement of TwoScribeVerseRecovery.

Packets are durable model objects only long enough to be delivered or dropped.
The network is explicitly asynchronous: it may delay packets indefinitely,
drop them, and deliver any queued packet before an older one.  A client may
retry before an earlier send packet is delivered.

The model intentionally constrains only the number of client send attempts.
That keeps exploration finite without treating retry exhaustion as a product
policy.  A production durable outbox retries until it receives an explicit
operator outcome or observes Canon commit.

The TLC configuration also limits the explicit network to three concurrent
packets.  That keeps this first network model exhaustive while retaining the
useful old-route + retry + late-ACK races.  It is not a network capacity claim.
*******************************************************************************)

CONSTANTS Clients, Scribes, RetryLimit

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

Generations == 0..1
Terms == 1..2
AttemptNumbers == 1..RetryLimit

RouteHint == [endpoint : Scribes, generation : Generations, term : Terms]
\* Physical duplicate deliveries in the same generation deliberately collapse
\* into one event-to-authority fact. A retry spanning A→B remains distinct.
AppendRecord == [client : Clients, writer : Scribes, generation : Generations,
                 term : Terms]

LeasePacket == [kind : {"lease-expired"}, recipient : {B}]
RoutePacket == [kind : {"route"}, client : Clients, endpoint : Scribes,
                generation : Generations, term : Terms]
SendPacket == [kind : {"send"}, client : Clients, endpoint : Scribes,
               generation : Generations, term : Terms, attempt : AttemptNumbers]
AckPacket == [kind : {"ack"}, client : Clients, writer : Scribes,
              generation : Generations, term : Terms]
Packet == LeasePacket \cup RoutePacket \cup SendPacket \cup AckPacket

VARIABLES
    alive,
    phase,
    writer,
    generation,
    term,
    authorityHistory,
    sealed,
    recoveryCandidate,
    bLeaseView,
    leaseExpirySent,
    route,
    outbox,
    ackObserved,
    attempts,
    appendSet,
    network

vars == << alive, phase, writer, generation, term, authorityHistory, sealed,
            recoveryCandidate, bLeaseView, leaseExpirySent, route, outbox,
            ackObserved, attempts, appendSet, network >>

Init ==
    /\ alive = [s \in Scribes |-> TRUE]
    /\ phase = Serving
    /\ writer = A
    /\ generation = 0
    /\ term = 1
    /\ authorityHistory = [g \in {0} |-> A]
    /\ sealed = [s \in Scribes |-> FALSE]
    /\ recoveryCandidate = None
    /\ bLeaseView = Fresh
    /\ leaseExpirySent = FALSE
    /\ route = [c \in Clients |->
          [endpoint |-> A, generation |-> 0, term |-> 1]]
    /\ outbox = [c \in Clients |-> Pending]
    /\ ackObserved = [c \in Clients |-> FALSE]
    /\ attempts = [c \in Clients |-> 0]
    /\ appendSet = {}
    /\ network = {}

Kill(s) ==
    /\ s \in Scribes
    /\ alive[s]
    /\ alive' = [alive EXCEPT ![s] = FALSE]
    /\ UNCHANGED << phase, writer, generation, term, authorityHistory, sealed,
                    recoveryCandidate, bLeaseView, leaseExpirySent, route,
                    outbox, ackObserved, attempts, appendSet, network >>

Return(s) ==
    /\ s \in Scribes
    /\ ~alive[s]
    /\ alive' = [alive EXCEPT ![s] = TRUE]
    /\ UNCHANGED << phase, writer, generation, term, authorityHistory, sealed,
                    recoveryCandidate, bLeaseView, leaseExpirySent, route,
                    outbox, ackObserved, attempts, appendSet, network >>

\* This represents an arbitrarily early, late, or false failure observation.
\* It is deliberately allowed while A remains alive.
EmitLeaseExpiry ==
    /\ ~leaseExpirySent
    /\ leaseExpirySent' = TRUE
    /\ network' = network \cup {[kind |-> "lease-expired", recipient |-> B]}
    /\ UNCHANGED << alive, phase, writer, generation, term, authorityHistory,
                    sealed, recoveryCandidate, bLeaseView, route, outbox,
                    ackObserved, attempts, appendSet >>

DeliverLeaseExpiry ==
    /\ [kind |-> "lease-expired", recipient |-> B] \in network
    /\ network' = network \ { [kind |-> "lease-expired", recipient |-> B] }
    /\ bLeaseView' = Expired
    /\ UNCHANGED << alive, phase, writer, generation, term, authorityHistory,
                    sealed, recoveryCandidate, leaseExpirySent, route, outbox,
                    ackObserved, attempts, appendSet >>

\* A route snapshot captures the current authoritative route at send time, but
\* may be delivered after that route is obsolete.  Repeated route publication
\* models duplicate snapshots; the packet set collapses identical copies.
EmitRouteSnapshot(c) ==
    /\ c \in Clients
    /\ outbox[c] = Pending
    /\ writer # None
    /\ (\E s \in Scribes : alive[s])
    /\ network' = network \cup
         {[kind |-> "route", client |-> c, endpoint |-> writer,
           generation |-> generation, term |-> term]}
    /\ UNCHANGED << alive, phase, writer, generation, term, authorityHistory,
                    sealed, recoveryCandidate, bLeaseView, leaseExpirySent,
                    route, outbox, ackObserved, attempts, appendSet >>

DeliverRouteSnapshot(packet) ==
    /\ packet \in network
    /\ packet.kind = "route"
    /\ network' = network \ {packet}
    /\ route' = [route EXCEPT
          ![packet.client] = [endpoint |-> packet.endpoint,
                              generation |-> packet.generation,
                              term |-> packet.term]]
    /\ UNCHANGED << alive, phase, writer, generation, term, authorityHistory,
                    sealed, recoveryCandidate, bLeaseView, leaseExpirySent,
                    outbox, ackObserved, attempts, appendSet >>

\* Retrying before an earlier request is delivered is intentional.  Each
\* packet carries the route snapshot and attempt number it had at send time.
ClientSend(c) ==
    /\ c \in Clients
    /\ outbox[c] = Pending
    /\ attempts[c] < RetryLimit
    /\ attempts' = [attempts EXCEPT ![c] = @ + 1]
    /\ network' = network \cup
         {[kind |-> "send", client |-> c,
           endpoint |-> route[c].endpoint,
           generation |-> route[c].generation,
           term |-> route[c].term,
           attempt |-> attempts[c] + 1]}
    /\ UNCHANGED << alive, phase, writer, generation, term, authorityHistory,
                    sealed, recoveryCandidate, bLeaseView, leaseExpirySent,
                    route, outbox, ackObserved, appendSet >>

\* A send delivered to a stale, dead, or recovering Scribe is removed without
\* an acknowledgement.  The client keeps its durable pending event and may
\* retry through another route.
DeliverRejectedOrUnavailableSend(packet) ==
    /\ packet \in network
    /\ packet.kind = "send"
    /\ (writer = None \/ ~alive[packet.endpoint] \/ packet.endpoint # writer)
    /\ network' = network \ {packet}
    /\ UNCHANGED << alive, phase, writer, generation, term, authorityHistory,
                    sealed, recoveryCandidate, bLeaseView, leaseExpirySent,
                    route, outbox, ackObserved, attempts, appendSet >>

\* A delivery to the current writer makes durable Canon evidence and emits an
\* independently delayable ACK packet.  The route generation/term are not a
\* grant: only packet.endpoint = current writer permits this transition.
DeliverLawfulSend(packet) ==
    /\ packet \in network
    /\ packet.kind = "send"
    /\ writer # None
    /\ alive[packet.endpoint]
    /\ packet.endpoint = writer
    /\ network' = (network \ {packet}) \cup
         {[kind |-> "ack", client |-> packet.client, writer |-> writer,
           generation |-> generation, term |-> term]}
    /\ appendSet' = appendSet \cup
         {[client |-> packet.client, writer |-> writer,
           generation |-> generation, term |-> term]}
    /\ UNCHANGED << alive, phase, writer, generation, term, authorityHistory,
                    sealed, recoveryCandidate, bLeaseView, leaseExpirySent,
                    route, outbox, ackObserved, attempts >>

\* ACKs are matched by the durable client event represented by client identity
\* in this first model.  A late ACK remains safe even after an earlier retry.
DeliverAck(packet) ==
    /\ packet \in network
    /\ packet.kind = "ack"
    /\ network' = network \ {packet}
    /\ ackObserved' = [ackObserved EXCEPT ![packet.client] = TRUE]
    /\ outbox' = [outbox EXCEPT
          ![packet.client] = IF @ = Pending THEN Acknowledged ELSE @]
    /\ UNCHANGED << alive, phase, writer, generation, term, authorityHistory,
                    sealed, recoveryCandidate, bLeaseView, leaseExpirySent,
                    route, attempts, appendSet >>

\* Any packet may be lost.  A dropped ACK is the useful case: Canon evidence
\* remains, while the producer retains the event and may retry or read it back.
DropPacket(packet) ==
    /\ packet \in network
    /\ network' = network \ {packet}
    /\ UNCHANGED << alive, phase, writer, generation, term, authorityHistory,
                    sealed, recoveryCandidate, bLeaseView, leaseExpirySent,
                    route, outbox, ackObserved, attempts, appendSet >>

ObservePriorCommit(c) ==
    /\ c \in Clients
    /\ outbox[c] = Pending
    /\ \E record \in appendSet : record.client = c
    /\ ackObserved' = [ackObserved EXCEPT ![c] = TRUE]
    /\ outbox' = [outbox EXCEPT ![c] = Acknowledged]
    /\ UNCHANGED << alive, phase, writer, generation, term, authorityHistory,
                    sealed, recoveryCandidate, bLeaseView, leaseExpirySent,
                    route, attempts, appendSet, network >>

ReclaimAcknowledged(c) ==
    /\ c \in Clients
    /\ outbox[c] = Acknowledged
    /\ ackObserved[c]
    /\ outbox' = [outbox EXCEPT ![c] = Reclaimed]
    /\ UNCHANGED << alive, phase, writer, generation, term, authorityHistory,
                    sealed, recoveryCandidate, bLeaseView, leaseExpirySent,
                    route, ackObserved, attempts, appendSet, network >>

\* Lease delivery lets B attempt recovery.  It does not itself make B writer.
BeginRecoveryByB ==
    /\ alive[B]
    /\ bLeaseView = Expired
    /\ phase = Serving
    /\ writer = A
    /\ phase' = Recovering
    /\ writer' = None
    /\ sealed' = [sealed EXCEPT ![A] = TRUE]
    /\ recoveryCandidate' = B
    /\ UNCHANGED << alive, generation, term, authorityHistory, bLeaseView,
                    leaseExpirySent, route, outbox, ackObserved, attempts,
                    appendSet, network >>

\* Represents lawful seal/replace/root-CAS completion.  This is the sole
\* transition that publishes B as writer; every queued old-A packet remains
\* harmless because delivery rechecks current writer authority.
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
    /\ UNCHANGED << alive, sealed, bLeaseView, leaseExpirySent, route, outbox,
                    ackObserved, attempts, appendSet, network >>

Next ==
    \/ \E s \in Scribes : Kill(s)
    \/ \E s \in Scribes : Return(s)
    \/ EmitLeaseExpiry
    \/ DeliverLeaseExpiry
    \/ \E c \in Clients : EmitRouteSnapshot(c)
    \/ \E packet \in network : DeliverRouteSnapshot(packet)
    \/ \E c \in Clients : ClientSend(c)
    \/ \E packet \in network : DeliverRejectedOrUnavailableSend(packet)
    \/ \E packet \in network : DeliverLawfulSend(packet)
    \/ \E packet \in network : DeliverAck(packet)
    \/ \E packet \in network : DropPacket(packet)
    \/ \E c \in Clients : ObservePriorCommit(c)
    \/ \E c \in Clients : ReclaimAcknowledged(c)
    \/ BeginRecoveryByB
    \/ PublishBSuccessor

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ alive \in [Scribes -> BOOLEAN]
    /\ phase \in {Serving, Recovering}
    /\ writer \in Scribes \cup {None}
    /\ generation \in Generations
    /\ term \in Terms
    /\ authorityHistory \in [0..generation -> Scribes]
    /\ sealed \in [Scribes -> BOOLEAN]
    /\ recoveryCandidate \in Scribes \cup {None}
    /\ bLeaseView \in {Fresh, Expired}
    /\ leaseExpirySent \in BOOLEAN
    /\ route \in [Clients -> RouteHint]
    /\ outbox \in [Clients -> {Pending, Acknowledged, Reclaimed}]
    /\ ackObserved \in [Clients -> BOOLEAN]
    /\ attempts \in [Clients -> 0..RetryLimit]
    /\ appendSet \subseteq AppendRecord
    /\ network \subseteq Packet

OneAuthorityPerGeneration ==
    \A g \in DOMAIN authorityHistory : authorityHistory[g] \in Scribes

EveryAppendMatchesPublishedAuthority ==
    \A record \in appendSet :
        /\ record.generation \in DOMAIN authorityHistory
        /\ record.writer = authorityHistory[record.generation]

AcknowledgementImpliesCommittedAppend ==
    \A c \in Clients :
        ackObserved[c] => \E record \in appendSet : record.client = c

OnlyAcknowledgedEventsAreReclaimed ==
    \A c \in Clients : outbox[c] = Reclaimed => ackObserved[c]

RecoveryGapHasNoWriter == phase = Recovering => writer = None

StaleAIsFencedAfterBPublication ==
    generation = 1 => authorityHistory[1] = B

\* A bounded model-checking aid, activated by the .cfg file.
NetworkBound == Cardinality(network) <= 2

\* Clients are intentionally symmetric in this scenario. TLC canonicalizes
\* states under their permutations; this preserves the three-client protocol
\* while avoiding three copies of every equivalent packet schedule.
ClientSymmetry == Permutations(Clients)

=============================================================================
