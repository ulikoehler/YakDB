# An O(1)-space O(n)-time algorithm to equally subdivide large databases.

## Purpose of this algorithm

This algorithm was designed to allow fast, <2-iteration
division of a large n-size key-value database into *k* different parts
of approximately equal size. The parts shall be written to *k* other databases.

The uncertainty of the size (--> *approximately* equal)
shall be deterministic and not grow beyond +-k.

The space consumed by the algorithm may be proportional to *k*,
but not proportial to any n-dependent function(n, log n, ...)

Two versions of the algorithm are provided:
    - Non-sorted: Divides the source DB into k-tuples and assigns them to the dst DBs by index
    - Sorted: Provides an iterator-based algorithm to guarantee each dst DB receives a consecutive range

## Algorithm description (not sorted)

- Create the iterator *SrcIt* in the source DB
- While True:
    - Read k values from database
        - If no values have been read: break
        - If l < k values have been read: Write values 1..l to dstDB 1..l and break
        - If k values have been read, write value 1..k to dstDB 1..k and continue

Correctness: Trivial
Dst DB size: Trivial, last DstDBs may receive one value less if n !== 0 (mod k)
Termination: Trivial, if n < $\inf$

Time complexity: Trivial, O(n), 1 iteration
Space-complexity: Trivial, O(k)

## Algorithm description (sorted)

- Create k+1 read pointers. We call them $RP_{0-(k-1)}$ and $RP_{main}$
- Initialize $mainPosCtr$ = 0, $stepCtr = 0$
- While True:
    - Try to advance $RP_{main}$ by k
        - If it could be advanced by 0 steps, break
        - Else, set $a$ = the number of steps it has been advanced successfully
    - Increment $stepCtr$ by 1
    - Increment $mainPosCtr$ by $a$
    - If $(k-1) \cdot stepCtr < mainPosCtr$
        - //Equivalent to a < k
        - break
    - $\forall i \in [0..(k-1)]: Advance $RP_{i}$ by $i$ steps
- $\forall i \in [0..(k-1)]:$
    - Write value of $RP_{i}$ to DstDB_{i+1}
    - Advanced value