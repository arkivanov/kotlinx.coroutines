/*
 * Copyright 2016-2020 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

@file:Suppress("unused")
package kotlinx.coroutines.linearizability

import kotlinx.atomicfu.*
import kotlinx.coroutines.*
import kotlinx.coroutines.internal.*
import kotlinx.coroutines.internal.SegmentQueueSynchronizer.ResumeMode.*
import kotlinx.coroutines.internal.SegmentQueueSynchronizer.CancellationMode.*
import kotlinx.coroutines.sync.*
import org.jetbrains.kotlinx.lincheck.annotations.Operation
import org.jetbrains.kotlinx.lincheck.verifier.*
import org.junit.*
import kotlin.coroutines.*
import kotlin.reflect.*

// This test suit serves two purposes. First of all, it tests the `SegmentQueueSynchronizer`
// implementation under different use-cases and workloads. At the same time, this test suite
// provides different well-known synchronization and communication primitive implementations
// via `SegmentQueueSynchronizer`, which can be considered as an API richness check as well as
// a collection of examples on how to use `SegmentQueueSynchronizer` to build new primitives.

// ##################################
// # SEMAPHORES WITH ASYNC SQS MODE #
// ##################################
//
// These semaphore implementations are correct when it is guaranteed
// that `release` is invoked only after a successful `acquire` invocation.
// The difference in `AsyncSemaphore` and `AsyncSemaphoreSmart` is the
// cancellation mechanism -- the smart version always works in a constant time
// without contention (independently on the number of cancelled requests)
// but requires some non-trivial tricks and much more complicated analysis.

internal abstract class AsyncSemaphoreBase(permits: Int) : SegmentQueueSynchronizer<Unit>(), Semaphore {
    override val resumeMode get() = ASYNC

    private val _availablePermits = atomic(permits)
    override val availablePermits get() = error("Not implemented")

    protected fun incPermits() = _availablePermits.getAndIncrement()
    protected fun decPermits() = _availablePermits.getAndDecrement()

    override suspend fun acquire() {
        val p = decPermits()
        // Is the permit acquired?
        if (p > 0) return
        // Suspend otherwise
        suspendAtomicCancellableCoroutine<Unit> { cont ->
            check(suspend(cont)) { "Should not fail in ASYNC mode" }
        }
    }

    override fun tryAcquire() =  error("Not supported in the ASYNC version")
}

internal class AsyncSemaphore(permits: Int) : AsyncSemaphoreBase(permits) {
    override fun release() {
        while (true) {
            val p = incPermits()
            if (p >= 0) return // no waiters
            if (resume(Unit)) return // can fail due to cancellation
        }
    }
}

internal class AsyncSemaphoreSmart(permits: Int) : AsyncSemaphoreBase(permits) {
    override val cancellationMode get() = SMART_SYNC

    override fun release() {
        val p = incPermits()
        if (p >= 0) return // no waiters
        resume(Unit)
    }

    override fun onCancellation(): Boolean {
        val p = incPermits()
        return p >= 0
    }
}

internal class SyncSemaphoreSmart(permits: Int) : SegmentQueueSynchronizer<Boolean>(), Semaphore {
    override val resumeMode get() = SYNC
    override val cancellationMode get() = SMART_SYNC

    private val _availablePermits = atomic(permits)
    override val availablePermits get() = error("Not implemented")

    protected fun incPermits() = _availablePermits.getAndIncrement()
    protected fun decPermits() = _availablePermits.getAndDecrement()

    override suspend fun acquire() {
        while (true) {
            val p = decPermits()
            // Is the permit acquired?
            if (p > 0) return
            // Suspend otherwise
            val acquired = suspendAtomicCancellableCoroutine<Boolean> { cont ->
                if (!suspend(cont)) cont.resume(false)
            }
            if (acquired) return
        }
    }

    override fun tryAcquire(): Boolean = _availablePermits.loop { cur ->
        if (cur <= 0) return false
        if (_availablePermits.compareAndSet(cur, cur -1)) return true
    }

    override fun release() {
        while (true) {
            val p = incPermits()
            if (p >= 0) return // no waiters
            if (resume(true)) return // can fail due to cancellation
        }
    }

    override fun onCancellation(): Boolean {
        val p = incPermits()
        return p >= 0
    }

    override fun onResumeFailure(value: Boolean) {
        release()
    }
}

abstract class AsyncSemaphoreLCStressTestBase(semaphore: Semaphore, val seqSpec: KClass<*>) {
    private val s = semaphore

    @Operation
    suspend fun acquire() = s.acquire()

    @Operation
    fun release() = s.release()

    @Test
    fun test() = LCStressOptionsDefault()
        .actorsBefore(0)
        .sequentialSpecification(seqSpec.java)
        .check(this::class)
}

class SemaphoreUnboundedSequential1 : SemaphoreSequential(1, false)
class SemaphoreUnboundedSequential2 : SemaphoreSequential(2, false)

class AsyncSemaphore1LCStressTest : AsyncSemaphoreLCStressTestBase(AsyncSemaphore(1), SemaphoreUnboundedSequential1::class)
class AsyncSemaphore2LCStressTest : AsyncSemaphoreLCStressTestBase(AsyncSemaphore(2), SemaphoreUnboundedSequential2::class)

class AsyncSemaphoreSmart1LCStressTest : AsyncSemaphoreLCStressTestBase(AsyncSemaphoreSmart(1), SemaphoreUnboundedSequential1::class)
class AsyncSemaphoreSmart2LCStressTest : AsyncSemaphoreLCStressTestBase(AsyncSemaphoreSmart(2), SemaphoreUnboundedSequential2::class)

class SyncSemaphoreSmart1LCStressTest : SemaphoreLCStressTestBase(SyncSemaphoreSmart(1), SemaphoreUnboundedSequential1::class)
class SyncSemaphoreSmart2LCStressTest : SemaphoreLCStressTestBase(SyncSemaphoreSmart(2), SemaphoreUnboundedSequential2::class)


// ####################################
// # COUNT-DOWN-LATCH SYNCHRONIZATION #
// ####################################

internal open class CountDownLatch(count: Int) : SegmentQueueSynchronizer<Unit>() {
    override val resumeMode get() = ASYNC

    private val count = atomic(count)
    private val waiters = atomic(0)

    protected fun decWaiters() = waiters.decrementAndGet()

    fun countDown() {
        val r = count.decrementAndGet()
        if (r <= 0) releaseWaiters()
    }

    private fun releaseWaiters() {
        val w = waiters.getAndUpdate { cur ->
            // is the mark set?
            if (cur and DONE_MARK != 0) return
            cur or DONE_MARK
        }
        repeat(w) { resume(Unit) }
    }

    suspend fun await() {
        // check whether the count has been reached zero
        if (remaining() == 0) return
        // add a new waiter (checking the counter again)
        val w = waiters.incrementAndGet()
        if (w and DONE_MARK != 0) return
        suspendAtomicCancellableCoroutine<Unit> { suspend(it) }
    }

    fun remaining(): Int = count.value.coerceAtLeast(0)

    protected companion object {
        const val DONE_MARK = 1 shl 31
    }
}

internal class CountDownLatchSmart(count: Int) : CountDownLatch(count) {
    override val cancellationMode get() = SMART_ASYNC

    override fun onCancellation(): Boolean {
        val w = decWaiters()
        return (w and DONE_MARK) != 0
    }
}

internal abstract class CountDownLatchLCStressTestBase(val cdl: CountDownLatch, val seqSpec: KClass<*>) {
    @Operation
    fun countDown() = cdl.countDown()

    @Operation
    fun remaining() = cdl.remaining()

    @Operation
    suspend fun await() = cdl.await()

    @Test
    fun test() = LCStressOptionsDefault()
        .actorsBefore(0)
        .actorsAfter(0)
        .sequentialSpecification(seqSpec.java)
        .check(this::class)
}

class CountDownLatchSequential1 : CountDownLatchSequential(1)
class CountDownLatchSequential2 : CountDownLatchSequential(2)

internal class CountDownLatch1LCStressTest : CountDownLatchLCStressTestBase(CountDownLatch(1), CountDownLatchSequential1::class)
internal class CountDownLatch2LCStressTest : CountDownLatchLCStressTestBase(CountDownLatch(2), CountDownLatchSequential2::class)

internal class CountDownLatchSmart1LCStressTest : CountDownLatchLCStressTestBase(CountDownLatchSmart(1), CountDownLatchSequential1::class)
internal class CountDownLatchSmart2LCStressTest : CountDownLatchLCStressTestBase(CountDownLatchSmart(2), CountDownLatchSequential2::class)

open class CountDownLatchSequential(initialCount: Int) : VerifierState() {
    private var count = initialCount
    private val waiters = ArrayList<CancellableContinuation<Unit>>()

    fun countDown() {
        if (--count == 0) {
            waiters.forEach { it.tryResume0(Unit) }
            waiters.clear()
        }
    }

    suspend fun await() {
        if (count <= 0) return
        suspendAtomicCancellableCoroutine<Unit> { cont ->
            waiters.add(cont)
        }
    }

    fun remaining(): Int = count.coerceAtLeast(0)

    override fun extractState() = remaining()
}


// ###########################
// # BARRIER SYNCHRONIZATION #
// ###########################

internal class Barrier(private val parties: Int) : SegmentQueueSynchronizer<Unit>() {
    override val resumeMode get() = ASYNC
    override val cancellationMode get() = SMART_ASYNC

    private val arrived = atomic(0L)

    suspend fun arrive(): Boolean {
        val a = arrived.incrementAndGet()
        return when {
            a < parties -> {
                suspendAtomicCancellableCoroutine<Unit> { cont -> suspend(cont) }
                true
            }
            a == parties.toLong() -> {
                repeat(parties - 1) {
                    resume(Unit)
                }
                true
            }
            else -> false
        }
    }

    override fun onCancellation(): Boolean {
        arrived.loop { cur ->
            if (cur == parties.toLong()) return true // just ignore the result
            if (arrived.compareAndSet(cur, cur - 1)) return false
        }
    }
}
// TODO: non-atomic cancellation test?

abstract class BarrierLCStressTestBase(parties: Int, val seqSpec: KClass<*>) {
    private val b = Barrier(parties)

    @Operation(cancellableOnSuspension = false)
    suspend fun arrive() = b.arrive()

    @Test
    fun test() = LCStressOptionsDefault()
        .actorsBefore(0)
        .actorsAfter(0)
        .threads(3)
        .sequentialSpecification(seqSpec.java)
        .check(this::class)
}

class BarrierSequential1 : BarrierSequential(1)
class Barrier1LCStressTest : BarrierLCStressTestBase(1, BarrierSequential1::class)
class BarrierSequential2 : BarrierSequential(2)
class Barrier2LCStressTest : BarrierLCStressTestBase(2, BarrierSequential2::class)
class BarrierSequential3 : BarrierSequential(3)
class Barrier3LCStressTest : BarrierLCStressTestBase(3, BarrierSequential3::class)

open class BarrierSequential(parties: Int) : VerifierState() {
    private var remainig = parties
    private val waiters = ArrayList<Continuation<Unit>>()

    suspend fun arrive(): Boolean {
        val r = --remainig
        return when {
            r > 0 -> {
                suspendAtomicCancellableCoroutine<Unit> { cont ->
                    waiters.add(cont)
                    cont.invokeOnCancellation {
                        remainig++
                        waiters.remove(cont)
                    }
                }
                true
            }
            r == 0 -> {
                waiters.forEach { it.resume(Unit) }
                true
            }
            else -> false
        }
    }

    override fun extractState() = remainig > 0
}


// ##################
// # BLOCKING POOLS #
// ##################

/**
 * While using resources such as database connections, sockets, etc.,
 * it is typical to reuse them; that requires a fast and handy mechanism.
 * This [BlockingPool] abstraction maintains a set of elements that can be put
 * into the pool for further reuse or be retrieved to process the current operation.
 * When [retrieve] comes to an empty pool, it blocks, and the following [put] operation
 * resumes it; all the waiting requests are processed in the first-in-first-out (FIFO) order.
 *
 * In our tests we consider two pool implementations: the [queue-based][BlockingQueuePool]
 * and the [stack-based][BlockingStackPool]. Intuitively, the queue-based implementation is
 * faster since it is built on arrays and uses `Fetch-And-Add`-s on the contended path,
 * while the stack-based pool retrieves the last inserted, thus the "hottest", elements.
 *
 * Please note that both these implementations are not atomic and can retrieve elements
 * out-of-order under some races. However, since pools by themselves do not guarantee
 * that the stored elements are ordered (the one may consider them as bags),
 * these queue- and stack-based versions should be considered as ones with the specific heuristics.
 */
interface BlockingPool<T: Any> {
    /**
     * Either resumes the first waiting [retrieve] operation
     * and passes the [element] to it, or simply put the
     * [element] into the pool.
     */
    fun put(element: T)

    /**
     * Retrieves one of the elements from the pool
     * (the order is not specified), or suspends if it is
     * empty -- the following [put] operations resume
     * waiting [retrieve]-s in the first-in-first-out order.
     */
    suspend fun retrieve(): T
} // TODO: smart cancellation is possible!

internal class BlockingQueuePool<T: Any> : SegmentQueueSynchronizer<T>(), BlockingPool<T> {
    override val resumeMode get() = ASYNC

    private val balance = atomic(0L) // #put  - #retrieve

    private val elements = atomicArrayOfNulls<Any?>(100) // This is an infinite array by design :)
    private val insertIdx = atomic(0) // the next slot for insertion
    private val retrieveIdx = atomic(0) // the next slot for retrieval

    override fun put(element: T) {
        while (true) {
            // Increment the number of `put`
            // operations in the balance.
            val b = balance.getAndIncrement()
            // Is there a waiting `retrieve`?
            if (b < 0) {
                // Try to resume the first waiter,
                // can fail if it is already cancelled.
                if (resume(element)) return
            } else {
                // Try to insert the element into the
                // array, can fail if the slot is broken.
                if (tryInsert(element)) return
            }
        }
    }

    /**
     * Tries to insert the [element] into the next
     * [elements] array slot. Returns `true` if
     * succeeds, or `fail` if the slot is [broken][BROKEN].
     */
    private fun tryInsert(element: T): Boolean {
        val i = insertIdx.getAndIncrement()
        return elements[i].compareAndSet(null, element)
    }

    override suspend fun retrieve(): T {
        while (true) {
            // Increment the number of `retrieve`
            // operations in the balance.
            val b = balance.getAndDecrement()
            // Is there an element in the pool?
            if (b > 0) {
                // Try to retrieve the first element,
                // can fail if the first [elements] slot
                // is empty due to a race.
                val x = tryRetrieve()
                if (x != null) return x
            } else {
                // The pool is empty, suspend
                return suspendAtomicCancellableCoroutine { cont ->
                    suspend(cont)
                }
            }
        }
    }

    /**
     * Tries to retrieve the first element from
     * the [elements] array. Return the element if
     * succeeds, or `null` if the first slot is empty
     * due to a race -- it marks the slot as [broken][BROKEN]
     * in this case, so that the corresponding [tryInsert]
     * invocation fails.
     */
    private fun tryRetrieve(): T? {
        val i = retrieveIdx.getAndIncrement()
        return elements[i].getAndSet(BROKEN) as T?
    }

    companion object {
        @JvmStatic
        val BROKEN = Symbol("BROKEN")
    }
}

internal class BlockingStackPool<T: Any> : SegmentQueueSynchronizer<T>(), BlockingPool<T> {
    override val resumeMode get() = ASYNC
    override val cancellationMode get() = SMART_SYNC

    private val head = atomic<StackNode<T>?>(null)
    private val availableElements = atomic(0) // #put - #retrieve

    override fun put(element: T) { // == release
        while (true) {
            val b = availableElements.getAndIncrement()
            if (b >= 0) {
                if (tryInsert(element)) return
            } else {
                resume(element)
                return
            }
        }
    }

    private fun tryInsert(element: T): Boolean {
        head.loop { h ->
            if (h != null && h.element == null) {
                if (head.compareAndSet(h, h.next)) return false
            } else {
                val newHead = StackNode(element, h)
                if (head.compareAndSet(h, newHead)) return true
            }
        }
    }

    override suspend fun retrieve(): T { // == acquire
        while (true) {
            val b = availableElements.getAndDecrement()
            if (b > 0) {
                val x = tryRetrieve()
                if (x != null) return x
            } else {
                return suspendAtomicCancellableCoroutine { cont ->
                    suspend(cont)
                }
            }
        }
    }

    private fun tryRetrieve(): T? {
        head.loop { h ->
            if (h == null || h.element == null) {
                val failNode = StackNode(null, h)
                if (head.compareAndSet(h, failNode)) return null
            } else {
                if (head.compareAndSet(h, h.next)) return h.element
            }
        }
    }

    override fun onCancellation(): Boolean {
        val b = availableElements.getAndIncrement()
        if (b >= 0) return true // return the element to the stack
        return false // resume the next waiter
    }

    override fun onIgnoredValue(value: T) {
        if (tryInsert(value)) return
        put(value)
    }

    class StackNode<T>(val element: T?, val next: StackNode<T>?)
}

abstract class BlockingPoolLCStressTestBase(val p: BlockingPool<Unit>) {
    @Operation
    fun put() = p.put(Unit)

    @Operation
    suspend fun retrieve() = p.retrieve()

    @Test
    fun test() = LCStressOptionsDefault()
        .sequentialSpecification(BlockingPoolUnitSequential::class.java)
        .check(this::class)
}
class BlockingQueuePoolLCStressTest : BlockingPoolLCStressTestBase(BlockingQueuePool())
class BlockingStackPoolLCStressTest : BlockingPoolLCStressTestBase(BlockingStackPool())

class BlockingPoolUnitSequential : VerifierState() {
    private var elements = 0
    private val waiters = ArrayList<CancellableContinuation<Unit>>()

    fun put() {
        while (true) {
            if (waiters.isNotEmpty()) {
                val w = waiters.removeAt(0)
                if (w.tryResume0(Unit)) return
            } else {
                elements ++
                return
            }
        }
    }

    suspend fun retrieve() {
        if (elements > 0) {
            elements--
        } else {
            suspendAtomicCancellableCoroutine<Unit> { cont ->
                waiters.add(cont)
            }
        }
    }

    override fun extractState() = elements
}


// #############
// # UTILITIES #
// #############

/**
 * Tries to resume this continuation atomically,
 * returns `true` if succeeds and `false` otherwise.
 */
private fun <T> CancellableContinuation<T>.tryResume0(value: T): Boolean {
    val token = tryResume(value) ?: return false
    completeResume(token)
    return true
}