/*
 * Copyright 2016-2020 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

@file:Suppress("unused")
package kotlinx.coroutines.linearizability

import kotlinx.atomicfu.*
import kotlinx.coroutines.*
import kotlinx.coroutines.internal.*
import kotlinx.coroutines.sync.*
import org.jetbrains.kotlinx.lincheck.*
import org.jetbrains.kotlinx.lincheck.annotations.Operation
import org.jetbrains.kotlinx.lincheck.execution.*
import org.jetbrains.kotlinx.lincheck.verifier.*
import org.junit.*
import kotlin.coroutines.*
import kotlin.random.*
import kotlin.reflect.*
import kotlin.reflect.jvm.*

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
    override val resumeMode get() = ResumeMode.ASYNC

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
            if (tryResume(Unit)) return // can fail due to cancellation
        }
    }
}

internal class AsyncSemaphoreSmart(permits: Int) : AsyncSemaphoreBase(permits) {
    override val cancellationMode: CancellationMode get() = CancellationMode.SMART

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

abstract class AsyncSemaphoreLCStressTestBase(semaphore: Semaphore, seqSpec: KClass<*>)
    : SemaphoreLCStressTestBase(semaphore, seqSpec)
{
    override fun Options<*, *>.customize() = this.executionGenerator(CustomSemaphoreScenarioGenerator::class.java)

    class CustomSemaphoreScenarioGenerator(testConfiguration: CTestConfiguration, testStructure: CTestStructure)
        : ExecutionGenerator(testConfiguration, testStructure)
    {
        override fun nextExecution() = ExecutionScenario(
            emptyList(),
            listOf(
                listOf(
                    Actor(AsyncSemaphoreLCStressTestBase::acquire.javaMethod!!, emptyList(), emptyList())
                ),
                listOf(
                    Actor(AsyncSemaphoreLCStressTestBase::acquire.javaMethod!!, emptyList(), emptyList()),
                    Actor(AsyncSemaphoreLCStressTestBase::acquire.javaMethod!!, emptyList(), emptyList(), cancelOnSuspension = true),
                    Actor(AsyncSemaphoreLCStressTestBase::release.javaMethod!!, emptyList(), emptyList())
                )
            ),
//            generateParallelPart(testConfiguration.threads, testConfiguration.actorsPerThread),
            emptyList()
        )

        private fun generateParallelPart(threads: Int, actorsPerThread: Int) = (1..threads).map {
            val actors = ArrayList<Actor>()
            var acquiredPermits = 0
            repeat(actorsPerThread) {
                actors += if (acquiredPermits == 0 || Random.nextBoolean()) {
                    // acquire
                    acquiredPermits++
                    Actor(AsyncSemaphoreLCStressTestBase::acquire.javaMethod!!, emptyList(), emptyList(), Random.nextBoolean())
                } else {
                    // release
                    acquiredPermits--
                    Actor(AsyncSemaphoreLCStressTestBase::release.javaMethod!!, emptyList(), emptyList())
                }
            }
            actors
        }
    }
}

class AsyncSemaphore1LCStressTest : AsyncSemaphoreLCStressTestBase(AsyncSemaphore(1), SemaphoreSequential1::class)
class AsyncSemaphore2LCStressTest : AsyncSemaphoreLCStressTestBase(AsyncSemaphore(2), SemaphoreSequential2::class)

class AsyncSemaphoreSmart1LCStressTest : AsyncSemaphoreLCStressTestBase(AsyncSemaphoreSmart(1), SemaphoreSequential1::class)
class AsyncSemaphoreSmart2LCStressTest : AsyncSemaphoreLCStressTestBase(AsyncSemaphoreSmart(2), SemaphoreSequential2::class)


// ####################################
// # COUNT-DOWN-LATCH SYNCHRONIZATION #
// ####################################

internal class CountDownLatch(count: Int) : SegmentQueueSynchronizer<Unit>() {
    override val resumeMode: ResumeMode get() = ResumeMode.ASYNC
    override val cancellationMode: CancellationMode get() = CancellationMode.SMART

    private val count = atomic(count)
    private val waiters = atomic(0)

    fun countDown() {
        val r = count.decrementAndGet()
        if (r <= 0) releaseWaiters()
    }

    override fun onCancellation(): Boolean {
        val w = waiters.decrementAndGet()
        return (w and DONE_MARK) != 0
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
}
private const val DONE_MARK = 1 shl 31

abstract class CountDownLatchLCStressTestBase(count: Int, val seqSpec: KClass<*>) {
    private val cdl = CountDownLatch(count)

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
class CountDownLatch1LCStressTest : CountDownLatchLCStressTestBase(1, CountDownLatchSequential1::class)
class CountDownLatchSequential2 : CountDownLatchSequential(2)
class CountDownLatch2LCStressTest : CountDownLatchLCStressTestBase(2, CountDownLatchSequential2::class)

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
    override val resumeMode: ResumeMode get() = ResumeMode.ASYNC
    override val cancellationMode: CancellationMode get() = CancellationMode.SMART

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
                    tryResume(Unit)
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

interface BlockingPool<T: Any> {
    fun put(element: T)
    suspend fun retrieve(): T
}

internal class BlockingQueuePool<T: Any> : SegmentQueueSynchronizer<T>(), BlockingPool<T> {
    private val balance = atomic(0L) // #put  - #retrieve

    private val elements = atomicArrayOfNulls<Any?>(100) // This is an infinite array by design :)
    private val adds = atomic(0)
    private val polls = atomic(0)

    override fun put(element: T) {
        while (true) {
            val b = balance.getAndIncrement()
            if (b < 0) {
                if (tryResume(element)) return
            } else {
                if (tryInsert(element)) return
            }
        }
    }

    private fun tryInsert(element: T): Boolean {
        val i = adds.getAndIncrement()
        return elements[i].compareAndSet(null, element)
    }

    override suspend fun retrieve(): T {
        while (true) {
            val b = balance.getAndDecrement()
            if (b > 0) {
                val x = tryRetrieve()
                if (x != null) return x
            } else {
                val x = suspendAtomicCancellableCoroutine<T?> { cont ->
                    if (!suspend(cont)) cont.resume(null)
                }
                if (x != null) return x
            }

        }
    }

    private fun tryRetrieve(): T? {
        val i = polls.getAndIncrement()
        return elements[i].getAndSet(BROKEN) as T?
    }

    companion object {
        @JvmStatic
        val BROKEN = Symbol("BROKEN")
    }
}

internal class BlockingStackPool<T: Any> : SegmentQueueSynchronizer<T>(), BlockingPool<T> {
    private val head = atomic<StackNode<T>?>(null)
    private val balance = atomic(0) // #put - #retrieve

    override fun put(element: T) {
        while (true) {
            val b = balance.getAndIncrement()
            if (b < 0) {
                if (tryResume(element)) return
            } else {
                if (tryInsert(element)) return
            }
        }
    }

    private fun tryInsert(element: T): Boolean {
        while (true) {
            val h = head.value
            if (h != null && h.element == null) {
                if (head.compareAndSet(h, h.next)) return false
            } else {
                val newHead = StackNode(element, h)
                if (head.compareAndSet(h, newHead)) return true
            }
        }
    }

    override suspend fun retrieve(): T {
        while (true) {
            val b = balance.getAndDecrement()
            if (b > 0) {
                val x = tryRetrieve()
                if (x != null) return x
            } else {
                val x = suspendAtomicCancellableCoroutine<T?> { cont ->
                    if (!suspend(cont)) cont.resume(null)
                }
                if (x != null) return x
            }
        }
    }

    private fun tryRetrieve(): T? {
        while (true) {
            val h = head.value
            if (h == null || h.element == null) {
                val suspendedNode = StackNode(null, h)
                if (head.compareAndSet(h, suspendedNode)) return null
            } else {
                if (head.compareAndSet(h, h.next)) return h.element
            }
        }
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

private fun <T> CancellableContinuation<T>.tryResume0(value: T): Boolean {
    val token = tryResume(value) ?: return false
    completeResume(token)
    return true
}