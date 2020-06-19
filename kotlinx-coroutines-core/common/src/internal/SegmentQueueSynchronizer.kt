/*
 * Copyright 2016-2020 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.internal

import kotlinx.atomicfu.*
import kotlinx.coroutines.*
import kotlin.coroutines.*
import kotlin.native.concurrent.*

@InternalCoroutinesApi
internal open class SegmentQueueSynchronizer<T>(val mode: Mode) {
    /*
   The queue of waiting acquirers is essentially an infinite array based on the list of segments
   (see `SemaphoreSegment`); each segment contains a fixed number of slots. To determine a slot for each enqueue
   and dequeue operation, we increment the corresponding counter at the beginning of the operation
   and use the value before the increment as a slot number. This way, each enqueue-dequeue pair
   works with an individual cell. We use the corresponding segment pointers to find the required ones.

   Here is a state machine for cells. Note that only one `acquire` and at most one `release` operation
   can deal with each cell, and that `release` uses `getAndSet(PERMIT)` to perform transitions for performance reasons
   so that the state `PERMIT` represents different logical states.

     +------+ `acquire` suspends   +------+   `release` tries    +--------+                    // if `cont.tryResume(..)` succeeds, then
     | NULL | -------------------> | cont | -------------------> | PERMIT | (cont RETRIEVED)   // the corresponding `acquire` operation gets
     +------+                      +------+   to resume `cont`   +--------+                    // a permit and the `release` one completes.
        |                             |
        |                             | `acquire` request is cancelled and the continuation is
        | `release` comes             | replaced with a special `CANCEL` token to avoid memory leaks
        | to the slot before          V
        | `acquire` and puts    +-----------+   `release` has    +--------+
        | a permit into the     | CANCELLED | -----------------> | PERMIT | (RElEASE FAILED)
        | slot, waiting for     +-----------+        failed      +--------+
        | `acquire` after
        | that.
        |
        |           `suspend` gets   +-------+
        |        +-----------------> | TAKEN | (ELIMINATION HAPPENED)
        V        |    the permit     +-------+
    +-------+    |
    | VALUE | --<
    +-------+   |
                |  `release` has waited a bounded time,   +--------+
                +---------------------------------------> | BROKEN | (BOTH RELEASE AND ACQUIRE FAILED)
                       but `acquire` has not come         +--------+
    */

    private val head: AtomicRef<SQSSegment>
    private val deqIdx = atomic(0L)
    private val tail: AtomicRef<SQSSegment>
    private val enqIdx = atomic(0L)

    init {
        val s = SQSSegment(0, null, 2)
        head = atomic(s)
        tail = atomic(s)
    }

    /**
     * Returns `false` if the received permit cannot be used and the calling operation should restart.
     */
    @Suppress("UNCHECKED_CAST")
    fun suspend(cont: CancellableContinuation<T>): Boolean {
        val curTail = this.tail.value
        val enqIdx = enqIdx.getAndIncrement()
        val segment = this.tail.findSegmentAndMoveForward(id = enqIdx / SEGMENT_SIZE, startFrom = curTail,
            createNewSegment = ::createSegment).segment // cannot be closed
        val i = (enqIdx % SEGMENT_SIZE).toInt()
        // the regular (fast) path -- if the cell is empty, try to install continuation
        if (segment.cas(i, null, cont)) { // installed continuation successfully
            cont.invokeOnCancellation(CancelSemaphoreAcquisitionHandler(segment, i).asHandler)
            return true
        }
        // On CAS failure -- the cell must be either PERMIT or BROKEN
        // If the cell already has PERMIT from tryResumeNextFromQueue, try to grab it
        val value = segment.get(i)
        if (value !== BROKEN && segment.cas(i, value, TAKEN)) { // took permit thus eliminating acquire/release pair
            cont.resume(value as T)
            return true
        }
        assert { segment.get(i) === BROKEN } // it must be broken in this case, no other way around it
        return false // broken cell, need to retry on a different cell
    }

    @Suppress("UNCHECKED_CAST")
    fun tryResume(value: T): Boolean {
        val curHead = this.head.value
        val deqIdx = deqIdx.getAndIncrement()
        val id = deqIdx / SEGMENT_SIZE
        val segment = this.head.findSegmentAndMoveForward(id, startFrom = curHead,
            createNewSegment = ::createSegment).segment // cannot be closed
        segment.cleanPrev()
        if (segment.id > id) return false
        val i = (deqIdx % SEGMENT_SIZE).toInt()
        val cellState = segment.getAndSet(i, value) // set PERMIT and retrieve the prev cell state
        when {
            cellState === null -> {
                // Return immediately in the async mode
                if (mode == Mode.ASYNC) return true
                // Acquire has not touched this cell yet, wait until it comes for a bounded time
                // The cell state can only transition from PERMIT to TAKEN by addAcquireToQueue
                repeat(MAX_SPIN_CYCLES) {
                    if (segment.get(i) === TAKEN) return true
                }
                // Try to break the slot in order not to wait
                return !segment.cas(i, value, BROKEN)
            }
            cellState === CANCELLED -> return false // the acquire was already cancelled
            else -> return (cellState as CancellableContinuation<T>).tryResume0(value)
        }
    }

    fun resume(value: T) {
        while(!tryResume(value)) {}
    }

    internal enum class Mode { SYNC, ASYNC }
}

private fun <T> CancellableContinuation<T>.tryResume0(value: T): Boolean {
    val token = tryResume(value) ?: return false
    completeResume(token)
    return true
}

private class CancelSemaphoreAcquisitionHandler(
    private val segment: SQSSegment,
    private val index: Int
) : CancelHandler() {
    override fun invoke(cause: Throwable?) {
        segment.cancel(index)
    }

    override fun toString() = "CancelSemaphoreAcquisitionHandler[$segment, $index]"
}

private fun createSegment(id: Long, prev: SQSSegment?) = SQSSegment(id, prev, 0)

private class SQSSegment(id: Long, prev: SQSSegment?, pointers: Int) : Segment<SQSSegment>(id, prev, pointers) {
    val acquirers = atomicArrayOfNulls<Any?>(SEGMENT_SIZE)
    override val maxSlots: Int get() = SEGMENT_SIZE

    @Suppress("NOTHING_TO_INLINE")
    inline fun get(index: Int): Any? = acquirers[index].value

    @Suppress("NOTHING_TO_INLINE")
    inline fun set(index: Int, value: Any?) {
        acquirers[index].value = value
    }

    @Suppress("NOTHING_TO_INLINE")
    inline fun cas(index: Int, expected: Any?, value: Any?): Boolean = acquirers[index].compareAndSet(expected, value)

    @Suppress("NOTHING_TO_INLINE")
    inline fun getAndSet(index: Int, value: Any?) = acquirers[index].getAndSet(value)

    // Cleans the acquirer slot located by the specified index
    // and removes this segment physically if all slots are cleaned.
    fun cancel(index: Int) {
        // Clean the slot
        set(index, CANCELLED)
        // Remove this segment if needed
        onSlotCleaned()
    }

    override fun toString() = "SQSSegment[id=$id, hashCode=${hashCode()}]"
}

@SharedImmutable
private val SEGMENT_SIZE = systemProp("kotlinx.coroutines.sqs.segmentSize", 16)
@SharedImmutable
private val MAX_SPIN_CYCLES = systemProp("kotlinx.coroutines.semaphore.maxSpinCycles", 100)
@SharedImmutable
private val TAKEN = Symbol("TAKEN")
@SharedImmutable
private val BROKEN = Symbol("BROKEN")
@SharedImmutable
private val CANCELLED = Symbol("CANCELLED")