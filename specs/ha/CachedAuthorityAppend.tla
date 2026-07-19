----------------------- MODULE CachedAuthorityAppend -----------------------
EXTENDS Naturals, FiniteSets, TLC

(*******************************************************************************
Is it safe for a writer to cache its authority observation?

Measured on a live fleet, the Serving-Authority path costs about four register
GETs per record and is flat across concurrency, roughly 86% of all
object-store requests.  Those reads do not amortise with batching, so batching
-- the stated economic defence against API cost -- cannot reach the dominant
term.  The only lever that moves it is re-observing the root less often.

That is a safety trade, and this module exists to price it before anyone
implements it.  Today every admission and every acknowledgement re-observes the
root, which is exactly what makes a deposed writer fail closed.  Caching
introduces a window in which a writer acts on a belief that may already be
false.

Two constants:

  AuthorityCacheBound  0 = re-observe before every append (today's behaviour).
                       N = a writer may make up to N appends on one cached
                           observation before it must re-read.

  SealFencesAppends    TRUE  = an append into a sealed generation fails at the
                               storage layer, independent of what the writer
                               believes about authority.
                       FALSE = the seal is a marker that a well-behaved writer
                               respects, but does not itself stop a write.

The hypothesis under test is that caching is safe *iff* the seal independently
fences appends -- that what actually stops a deposed writer is the seal, not
its belief about authority, and therefore that the authority read is buying
liveness of refusal rather than safety.  If that holds, the cost lever is
available at a stated price.  If it does not, the four reads per record are
load-bearing and the cost is irreducible without a different mechanism.

The hazard modelled is not "two processes think they are writer" -- that is
harmless on its own.  It is a record committed into a generation that has
already been sealed, which a reader honouring the sealed tail will never
return.  That is silent loss, and it is the same boundary the no-loss cutover
question turns on.
*******************************************************************************)

CONSTANTS Writers, AuthorityCacheBound, SealFencesAppends

Generations == 0..2
Versions == 1..4

\* An observation a writer holds: which generation it believes it may write,
\* and the register version that belief came from.
Observation == [generation : Generations, version : Versions]

VARIABLES
    registerGeneration,  \* generation named by the durable root
    registerWriter,      \* writer named by the durable root
    registerVersion,     \* CAS version of the durable root
    sealed,              \* generations that have been sealed
    cached,              \* per writer: the observation it is acting on
    cacheUses,           \* per writer: appends made on the current observation
    committed,           \* [writer, generation] records that landed
    lateCommits          \* records that landed *after* their generation sealed

vars == << registerGeneration, registerWriter, registerVersion, sealed,
           cached, cacheUses, committed, lateCommits >>

InitialWriter == CHOOSE w \in Writers : TRUE

Init ==
    /\ registerGeneration = 0
    /\ registerWriter = InitialWriter
    /\ registerVersion = 1
    /\ sealed = {}
    /\ cached = [w \in Writers |-> [generation |-> 0, version |-> 1]]
    /\ cacheUses = [w \in Writers |-> 0]
    /\ committed = {}
    /\ lateCommits = {}

\* Re-reading the root refreshes the belief and resets the budget.
Observe(w) ==
    /\ w \in Writers
    /\ cached' = [cached EXCEPT
          ![w] = [generation |-> registerGeneration, version |-> registerVersion]]
    /\ cacheUses' = [cacheUses EXCEPT ![w] = 0]
    /\ UNCHANGED << registerGeneration, registerWriter, registerVersion, sealed,
                    committed, lateCommits >>

\* A writer may append while it still believes it holds authority.
\*
\* With AuthorityCacheBound = 0 it must have re-observed since its last append,
\* which is today's behaviour: the belief is always current.  With a positive
\* bound it may spend up to that many appends on one observation, and the root
\* may move underneath it in the meantime.
MayUseCache(w) ==
    IF AuthorityCacheBound = 0
    THEN cacheUses[w] = 0 /\ cached[w].version = registerVersion
    ELSE cacheUses[w] < AuthorityCacheBound

\* The storage-layer fence.  When the seal genuinely fences appends, a write
\* into a sealed generation cannot land whatever the writer believes.
SealPermits(g) == (SealFencesAppends => g \notin sealed)

Append(w) ==
    /\ w \in Writers
    /\ MayUseCache(w)
    /\ cached[w].generation = registerGeneration \/ AuthorityCacheBound > 0
    /\ SealPermits(cached[w].generation)
    \* The writer believes it is the authority for the generation it cached.
    /\ \/ registerWriter = w /\ cached[w].version = registerVersion
       \/ AuthorityCacheBound > 0 /\ cached[w].version # registerVersion
    /\ committed' = committed \cup
         {[writer |-> w, generation |-> cached[w].generation]}
    \* A record that lands in an already-sealed generation is the hazard: a
    \* reader honouring the sealed tail will never return it.
    /\ lateCommits' =
         IF cached[w].generation \in sealed
         THEN lateCommits \cup {[writer |-> w, generation |-> cached[w].generation]}
         ELSE lateCommits
    /\ cacheUses' = [cacheUses EXCEPT ![w] = @ + 1]
    /\ UNCHANGED << registerGeneration, registerWriter, registerVersion, sealed,
                    cached >>

\* Lawful recovery: seal the predecessor, then CAS the root to the successor.
\* Sealing first is what the ordering fix established; a peer that could
\* publish before sealing would make this question moot.
Recover(w) ==
    /\ w \in Writers
    /\ w # registerWriter
    /\ registerGeneration + 1 \in Generations
    /\ registerVersion + 1 \in Versions
    /\ sealed' = sealed \cup {registerGeneration}
    /\ registerGeneration' = registerGeneration + 1
    /\ registerWriter' = w
    /\ registerVersion' = registerVersion + 1
    /\ cached' = [cached EXCEPT
          ![w] = [generation |-> registerGeneration + 1,
                  version |-> registerVersion + 1]]
    /\ cacheUses' = [cacheUses EXCEPT ![w] = 0]
    /\ UNCHANGED << committed, lateCommits >>

Next ==
    \/ \E w \in Writers : Observe(w)
    \/ \E w \in Writers : Append(w)
    \/ \E w \in Writers : Recover(w)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ registerGeneration \in Generations
    /\ registerWriter \in Writers
    /\ registerVersion \in Versions
    /\ sealed \subseteq Generations
    /\ cached \in [Writers -> Observation]
    /\ cacheUses \in [Writers -> 0..(AuthorityCacheBound + 1)]
    /\ committed \subseteq [writer : Writers, generation : Generations]
    /\ lateCommits \subseteq [writer : Writers, generation : Generations]

\* The hazard that matters. A record committed into a sealed generation is
\* invisible to any reader honouring the sealed tail: silent loss, not a
\* visible conflict.
\* Records committed *before* a seal legitimately survive it; sealing stops new
\* writes, it does not retract old ones. Only a commit that lands after the
\* seal is loss.
NoCommitIntoSealedGeneration == lateCommits = {}

\* Vacuity probe: TLC must REPORT THIS VIOLATED. A "safe" run in which nothing
\* was ever committed, or the root never moved, would prove nothing at all.
ReachesInterestingStates ==
    ~(Cardinality(committed) >= 2 /\ sealed # {})

\* The classic split-brain statement, kept for comparison. Note it is the
\* weaker property here: loss happens before two writers ever share one
\* generation.
OneWriterPerGeneration ==
    \A r1, r2 \in committed :
        r1.generation = r2.generation => r1.writer = r2.writer

=============================================================================
