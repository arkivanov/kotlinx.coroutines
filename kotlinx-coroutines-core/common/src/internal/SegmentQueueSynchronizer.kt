/*
 * Copyright 2016-2020 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.internal

import kotlinx.atomicfu.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.internal.DONE
import kotlinx.coroutines.internal.SegmentQueueSynchronizer.Mode.*
import kotlinx.coroutines.sync.*
import kotlin.coroutines.*
import kotlin.native.concurrent.*

/**
 * [SegmentQueueSynchronizer] is an abstraction for implementing _fair_ synchronization
 * and communication primitives that maintains a FIFO queue of waiting requests.
 * The two main functions it provides:
 * + [suspend] that stores the specified continuation into the queue, and
 * + [tryResume] function that tries to retrieve and resume the first continuation with the specified value.
 *
 * One may consider the structure as an infinite array with two counters that reference the next cells
 * for enqueueing a continuation in [suspend] and for retrieving one in [tryResume]. To be short, when
 * [suspend] is invoked, it increments the corresponding counter via fast `Fetch-And-Add` and stores the
 * continuation into the cell. At the same time, [tryResume] increments its own counter and comes to the
 * corresponding cell.
 *
 * Since [suspend] can store [CancellableContinuation]-s, it is possible for [tryResume] to fail if the
 * continuation is already cancelled. In this case, most of the algorithms retry the whole operation.
 * However, some solutions may invoke [tryResume] until it succeeds, so that [SegmentQueueSynchronizer]
 * is provided with a nice short-cut [resume], which also efficiently skips consecutive cancelled continuations.
 *
 * The typical implementations via [SegmentQueueSynchronizer] perform some synchronization at first,
 * (e.g., [Semaphore] modifies the number of available permits), and invoke [suspend] or [tryResume]
 * after that. Following this pattern, it is possible in a concurrent environment that [tryResume]
 * is executed before [suspend] (similarly to the race between `park` and `unpark` for threads),
 * so that [tryResume] comes to an empty cell. This race can be solved with two [strategies][Mode]:
 * [asynchronous][Mode.ASYNC] and [synchronous][Mode.SYNC].
 * In the [synchronous][Mode.ASYNC] mode, [tryResume] puts the element if the cell is empty
 * and finishes, the next [suspend] comes to this cell and simply grabs the element without suspension.
 * At the same time, in the [synchronous][Mode.SYNC] mode, [tryResume] waits in a bounded spin-loop
 * until the put element is taken, marking the cell as broken if it is not after all. In this case both
 * the current [tryResume] and the [suspend] that comes to this broken cell fail.
 *
 * Here is a state machine for cells. Note that only one [suspend] and at most one [tryResume] (or [resume]) operation
 * can deal with each cell.
 *
 *  +------+ `suspend` succeeds   +------+  `tryResume` tries   +------+                        // if `cont.tryResume(..)` succeeds, then
 *  | NULL | -------------------> | cont | -------------------> | DONE | (`cont` IS RETRIEVED)  // the corresponding `tryResume` succeeds gets
 *  +------+                      +------+   to resume `cont`   +------+                        // as well, fails otherwise.
 *     |                             |
 *     |                             | The suspended request is cancelled and the continuation is
 *     | `tryResume` comes           | replaced with a special `CANCELLED` token to avoid memory leaks.
 *     | to the cell before          V
 *     | `suspend` and puts    +-----------+
 *     | the element into      | CANCELLED |  (`cont` IS CANCELLED, `tryResume` FAILS)
 *     | the cell, waiting     +-----------+
 *     | for `suspend` in
 *     | ASYNC mode.
 *     |
 *     |              `suspend` gets   +-------+  ( ELIMINATION HAPPENED, )
 *     |           +-----------------> | TAKEN |  ( BOTH `tryResume` and  )
 *     V           |    the element    +-------+  ( `suspend` SUCCEED     )
 *  +---------+    |
 *  | element | --<
 *  +---------+   |
 *                |
 *                | `tryResume` has waited a bounded time   +--------+
 *                +---------------------------------------> | BROKEN | (BOTH `suspend` AND `tryResume` FAIL)
 *                       but `suspend` has not come         +--------+
 *
 *  As for the infinite array implementation, it is organized as a linked list of [segments][SQSSegment];
 *  each segment contains a fixed number of cells. To determine the cell for each [suspend] and [tryResume]
 *  operation, the algorithm reads the current [tail] or [head], increments [enqIdx] or [deqIdx], and
 *  finds the required segment starting from the initially read one.
 */
@InternalCoroutinesApi
internal open class SegmentQueueSynchronizer<T>(val mode: Mode) {
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
     * TODO
     * Returns `false` if the received permit cannot be used and the calling operation should restart.
     */
    @Suppress("UNCHECKED_CAST")
    fun suspend(cont: Continuation<T>): Boolean {
        val curTail = this.tail.value
        val enqIdx = enqIdx.getAndIncrement()
        val segment = this.tail.findSegmentAndMoveForward(id = enqIdx / SEGMENT_SIZE, startFrom = curTail,
            createNewSegment = ::createSegment).segment // cannot be closed
        val i = (enqIdx % SEGMENT_SIZE).toInt()
        // the regular (fast) path -- if the cell is empty, try to install continuation
        if (segment.cas(i, null, cont)) { // try to install the continuation
            if (cont is CancellableContinuation<*>) {
                cont.invokeOnCancellation(SQSCancellationHandler(this, segment, i).asHandler)
            }
            return true
        }
        // On CAS failure -- the cell must either contain a value or be broken.
        // Try to grab the value.
        val value = segment.get(i)
        if (value !== BROKEN && segment.cas(i, value, TAKEN)) { // took the value eliminating suspend/tryResume pair
            cont.resume(value as T)
            return true
        }
        assert { segment.get(i) === BROKEN } // it must be broken in this case, no other way around it
        return false // broken cell, need to retry on a different cell
    }

    /**
     * TODO
     */
    fun tryResume(value: T): Boolean = tryResumeImpl(value, adjustDeqIdx = false)

    /**
     * Essentially, this is a short-cut for `while (!tryResume(..)) {}`, but
     * works in O(1) without contention independently on how many
     * [suspended][suspend] continuations has been cancelled.
     */
    fun resume(value: T) {
        while (!tryResumeImpl(value, adjustDeqIdx = true)) { /* try again */ }
    }

    @Suppress("UNCHECKED_CAST")
    private fun tryResumeImpl(value: T, adjustDeqIdx: Boolean): Boolean {
        val curHead = this.head.value
        val deqIdx = deqIdx.getAndIncrement()
        val id = deqIdx / SEGMENT_SIZE
        val segment = this.head.findSegmentAndMoveForward(id, startFrom = curHead,
            createNewSegment = ::createSegment).segment // cannot be closed
        segment.cleanPrev()
        if (segment.id > id) {
            if (adjustDeqIdx) adjustDeqIdx(segment.id * SEGMENT_SIZE)
            return false
        }
        val i = (deqIdx % SEGMENT_SIZE).toInt()
        modify_cell@while (true) { // modify the cell state
            val cellState = segment.get(i)
            when {
                cellState === null -> {
                    if (!segment.cas(i, null, value)) continue@modify_cell
                    // Return immediately in the async mode
                    if (mode.async) return true
                    // Acquire has not touched this cell yet, wait until it comes for a bounded time
                    // The cell state can only transition from PERMIT to TAKEN by addAcquireToQueue
                    repeat(MAX_SPIN_CYCLES) {
                        if (segment.get(i) === TAKEN) return true
                    }
                    // Try to break the slot in order not to wait
                    return !segment.cas(i, value, BROKEN)
                }
                cellState === CANCELLING -> {
                    if (segment.cas(i, CANCELLING, value)) return true
                }
                cellState === CANCELLED -> {
                    return false
                } // the acquire was already cancelled
                cellState === IGNORE -> return true
                cellState is CancellableContinuation<*> -> {
                    val success = (cellState as CancellableContinuation<T>).tryResume0(value)
                    if (success) {
                        segment.set(i, DONE)
                        return true
                    }
                    if (mode != ASYNC_SMART) return false
                    if (!segment.cas(i, cellState, CANCELLING)) continue@modify_cell
                    val ignore = onCancellation()
                    if (ignore) {
                        segment.set(i, IGNORE)
                        return true
                    } else {
                        segment.set(i, CANCELLED)
                        return false
                    }
                }
                else -> {
                    (cellState as Continuation<T>).resume(value)
                    segment.set(i, DONE)
                    return true
                }
            }
        }
    }

    private fun adjustDeqIdx(newValue: Long): Unit = deqIdx.loop { cur ->
        if (cur >= newValue) return
        if (deqIdx.compareAndSet(cur, newValue)) return
    }

    /**
     * TODO
     */
    open fun onCancellation() : Boolean = false

    /**
     * These modes define the strategy that [tryResume] and [resume] should
     * use when they come to the cell before [suspend] and find it empty.
     * In the [asynchronous][ASYNC] mode, they put the value into the slot,
     * so that [suspend] grabs it and immediately resumes. However,
     * this strategy produces an incorrect behavior when used for some
     * data structures (e.g., for [Semaphore]), and the [synchronous][SYNC]
     * mode is used in this case. Similarly to the asynchronous mode,
     * [tryResume] and [resume] put the value into the cell, but do not finish
     * after that. In opposite, they wait in a bounded spin-loop
     * (see [MAX_SPIN_CYCLES]) until the value is taken, marking the cell
     * as [broken][BROKEN] and failing if it is not, so that the corresponding
     * [suspend] invocation finds the cell [broken][BROKEN] and fails as well.
     */
    internal enum class Mode(val async: Boolean) {
        SYNC(false),
        ASYNC(true),
        ASYNC_SMART(true)
    }
}

private fun <T> CancellableContinuation<T>.tryResume0(value: T): Boolean {
    val token = tryResume(value) ?: return false
    completeResume(token)
    return true
}

private class SQSCancellationHandler<T>(
    private val sqs: SegmentQueueSynchronizer<T>,
    private val segment: SQSSegment,
    private val index: Int
) : CancelHandler() {
    override fun invoke(cause: Throwable?) {
        if (sqs.mode != ASYNC_SMART) {
            segment.cancel(index)
            return
        }
        val cellState = segment.get(index)
        if (cellState === CANCELLING || cellState === CANCELLED || cellState === IGNORE) return
        if (!segment.cas(index, cellState, CANCELLING)) {
            return
        }
        val ignore = sqs.onCancellation()
        if (ignore) {
            segment.set(index, IGNORE)
        } else {
            if (segment.cas(index, CANCELLING, CANCELLED)) return
            val value = segment.get(index) as T
            segment.set(index, CANCELLED)
            segment.onSlotCleaned()
            sqs.resume(value)
        }
    }

    override fun toString() = "SQSCancellationHandler[$segment, $index]"
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

    // Cleans the acquirer slot located by the specified index
    // and removes this segment physically if all slots are cleaned.
    fun cancel(index: Int): Boolean {
        // Clean the slot
        val cur = get(index)
        if (cur === CANCELLED) return false
        if (!cas(index, cur, CANCELLED)) return false
        // Remove this segment if needed
        onSlotCleaned()
        return true
    }

    override fun toString() = "SQSSegment[id=$id, hashCode=${hashCode()}]"
}

@SharedImmutable
private val SEGMENT_SIZE = systemProp("kotlinx.coroutines.sqs.segmentSize", 16)
@SharedImmutable
private val MAX_SPIN_CYCLES = systemProp("kotlinx.coroutines.sqs.maxSpinCycles", 100)
@SharedImmutable
private val TAKEN = Symbol("TAKEN")
@SharedImmutable
private val BROKEN = Symbol("BROKEN")
@SharedImmutable
private val CANCELLING = Symbol("CANCELLING")
@SharedImmutable
private val CANCELLED = Symbol("CANCELLED")
@SharedImmutable
private val IGNORE = Symbol("IGNORE")