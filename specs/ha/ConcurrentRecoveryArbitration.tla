--------------------- MODULE ConcurrentRecoveryArbitration ---------------------
EXTENDS Naturals, FiniteSets, TLC

(*******************************************************************************
Which substrate is load-bearing for multi-Scribe recovery safety?

TwoScribeVerseRecoveryNetwork serialises recovery by construction: a single
`recoveryCandidate` scalar plus a `phase = Serving` guard means two Scribes
never race to publish a successor.  That mutual exclusion *is* an external
consistency engine.  The model therefore establishes the authority rule only
for deployments that already have one, which is the case that was never in
doubt.

This module removes the assumption and makes both substrates explicit
parameters, so the question can be answered by TLC rather than by architecture
prose:

  ExclusiveCandidacy   TRUE  = an external engine (Consul, etcd/Kubernetes,
                               Postgres) serialises recovery-candidate
                               selection.
                       FALSE = no external engine.  Any Scribe may suspect the
                               writer at any time, including falsely, and any
                               number may attempt recovery concurrently.

  RegisterSemantics    "cas" = the durable root is a conditional register:
                               a write lands only if the version the candidate
                               observed is still current.  This is what
                               ObjectStoreConditionalRegister provides.
                       "lww" = the root is a plain last-writer-wins object.

The four combinations answer the product question directly:

  cfg                     Exclusive  Register  Expected
  Arbitration.cfg         FALSE      cas       safe   <- the load-bearing claim
  ArbitrationLww.cfg      TRUE       lww       UNSAFE <- engine alone is not
  ArbitrationNoEngine.cfg FALSE      lww       UNSAFE <- neither
  ArbitrationBoth.cfg     TRUE       cas       safe   <- belt and braces

If the first is safe and the second is not, then the conditional register is
what carries the *modelled arbitration safety* and the external engine is an
availability convenience.  This result intentionally does not cover
seal-and-tail conservation, object-store durability, or the runtime admission
path; those have separate models and tests.

Authority is not modelled as a separate seal step.  A deposed writer is fenced
because the register version it observed is no longer current, which is the
same rule `ServingAuthorityRecord::is_effective_writer` enforces against the
witnessed root: exact record and generation binding, not endpoint identity.

Scribe crash/return is deliberately absent.  A falsely-suspected *live* writer
is the harder case and is already reachable here, since suspicion is
unconditional.
*******************************************************************************)

CONSTANTS Scribes, ExclusiveCandidacy, RegisterSemantics

Generations == 0..2
Terms == 1..3
Versions == 1..6

AuthorityValue == [writer : Scribes, generation : Generations, term : Terms]
\* "No current observation" is carried by hasObserved rather than a sentinel
\* value, so every variable stays type-uniform. A heterogeneous
\* `Observation \cup {None}` makes TLC compare a record against a string.
Publication == [writer : Scribes, generation : Generations, term : Terms]
AppendRecord == [writer : Scribes, generation : Generations]

VARIABLES
    registerValue,
    registerVersion,
    observedValue,
    observedVersion,
    hasObserved,
    candidate,
    publications,
    appendSet

vars == << registerValue, registerVersion, observedValue, observedVersion,
           hasObserved, candidate, publications, appendSet >>

InitialWriter == CHOOSE s \in Scribes : TRUE

InitialValue == [writer |-> InitialWriter, generation |-> 0, term |-> 1]

Init ==
    /\ registerValue = InitialValue
    /\ registerVersion = 1
    /\ observedValue = [s \in Scribes |-> InitialValue]
    /\ observedVersion = [s \in Scribes |-> 1]
    \* The initial writer has witnessed the root it serves under; peers have not.
    /\ hasObserved = [s \in Scribes |-> s = InitialWriter]
    /\ candidate = [s \in Scribes |-> FALSE]
    /\ publications = {InitialValue}
    /\ appendSet = {}

\* Reading the durable root.  Always current: object stores in scope provide
\* read-after-write consistency, so stale reads are not modelled here.
Read(s) ==
    /\ s \in Scribes
    /\ observedValue' = [observedValue EXCEPT ![s] = registerValue]
    /\ observedVersion' = [observedVersion EXCEPT ![s] = registerVersion]
    /\ hasObserved' = [hasObserved EXCEPT ![s] = TRUE]
    /\ UNCHANGED << registerValue, registerVersion, candidate, publications,
                    appendSet >>

\* An arbitrarily early, late, or simply false failure observation.  With no
\* external engine this is unconstrained, which is the entire point: nothing
\* prevents every peer from suspecting a healthy writer at once.
Suspect(s) ==
    /\ s \in Scribes
    /\ ~candidate[s]
    /\ hasObserved[s]
    /\ observedValue[s].writer # s
    /\ (ExclusiveCandidacy => \A p \in Scribes : ~candidate[p])
    /\ candidate' = [candidate EXCEPT ![s] = TRUE]
    /\ UNCHANGED << registerValue, registerVersion, observedValue,
                    observedVersion, hasObserved, publications, appendSet >>

\* The candidate proposes the successor derived from what it observed.  Two
\* candidates that observed the same root therefore propose the *same*
\* generation, which is what the register must arbitrate.
ProposedFrom(s) ==
    [writer |-> s,
     generation |-> observedValue[s].generation + 1,
     term |-> observedValue[s].term + 1]

CanPublish(s) ==
    /\ candidate[s]
    /\ hasObserved[s]
    /\ observedValue[s].generation + 1 \in Generations
    /\ observedValue[s].term + 1 \in Terms
    /\ registerVersion + 1 \in Versions

\* The conditional root write. Under "cas" it lands only if no one else has
\* written since this candidate read; under "lww" it always lands.
PublishSucceeds(s) ==
    /\ CanPublish(s)
    /\ (RegisterSemantics = "cas" => observedVersion[s] = registerVersion)
    /\ registerValue' = ProposedFrom(s)
    /\ registerVersion' = registerVersion + 1
    /\ publications' = publications \cup {ProposedFrom(s)}
    /\ candidate' = [candidate EXCEPT ![s] = FALSE]
    \* The reply may be lost: the candidate must reread rather than assume.
    /\ hasObserved' = [hasObserved EXCEPT ![s] = FALSE]
    /\ UNCHANGED << observedValue, observedVersion, appendSet >>

\* A rejected conditional write. The candidate learns nothing about who won and
\* must reread before acting -- it never blindly retries.
PublishRejected(s) ==
    /\ CanPublish(s)
    /\ RegisterSemantics = "cas"
    /\ observedVersion[s] # registerVersion
    /\ candidate' = [candidate EXCEPT ![s] = FALSE]
    /\ hasObserved' = [hasObserved EXCEPT ![s] = FALSE]
    /\ UNCHANGED << registerValue, registerVersion, observedValue,
                    observedVersion, publications, appendSet >>

\* The analogue of is_effective_writer: a Scribe may commit only while the
\* record it witnessed is still exactly the current root. A deposed writer
\* fails this because the version moved, not because anyone told it to stop.
Append(s) ==
    /\ s \in Scribes
    /\ hasObserved[s]
    /\ observedValue[s].writer = s
    /\ observedValue[s] = registerValue
    /\ observedVersion[s] = registerVersion
    /\ appendSet' = appendSet \cup
         {[writer |-> s, generation |-> registerValue.generation]}
    /\ UNCHANGED << registerValue, registerVersion, observedValue,
                    observedVersion, hasObserved, candidate, publications >>

Next ==
    \/ \E s \in Scribes : Read(s)
    \/ \E s \in Scribes : Suspect(s)
    \/ \E s \in Scribes : PublishSucceeds(s)
    \/ \E s \in Scribes : PublishRejected(s)
    \/ \E s \in Scribes : Append(s)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ registerValue \in AuthorityValue
    /\ registerVersion \in Versions
    /\ observedValue \in [Scribes -> AuthorityValue]
    /\ observedVersion \in [Scribes -> Versions]
    /\ hasObserved \in [Scribes -> BOOLEAN]
    /\ candidate \in [Scribes -> BOOLEAN]
    /\ publications \subseteq Publication
    /\ appendSet \subseteq AppendRecord

\* The safety property the optional-substrates foundation asserts: a false
\* liveness suspicion may cause an unnecessary recovery attempt, but cannot
\* produce two lawful writers.
OneAuthorityPerGeneration ==
    \A p1, p2 \in publications :
        p1.generation = p2.generation => p1.writer = p2.writer

\* The same hazard observed where it actually hurts: two Scribes committing
\* into one generation is the split-brain that loses messages.
NoTwoWritersInAGeneration ==
    \A r1, r2 \in appendSet :
        r1.generation = r2.generation => r1.writer = r2.writer

\* Every commit happened under a published authority for that generation.
EveryAppendMatchesPublishedAuthority ==
    \A r \in appendSet :
        \E p \in publications :
            /\ p.generation = r.generation
            /\ p.writer = r.writer

\* Deliberately no SYMMETRY declaration. `InitialWriter` uses CHOOSE, which
\* singles out one Scribe, so the Scribes are not interchangeable and a
\* symmetry set would be unsound here. The state space is small enough that it
\* is not needed.

=============================================================================
