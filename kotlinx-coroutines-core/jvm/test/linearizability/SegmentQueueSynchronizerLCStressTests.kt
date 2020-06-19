/*
 * Copyright 2016-2020 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

@file:Suppress("unused")
package kotlinx.coroutines.linearizability

import kotlinx.atomicfu.*
import kotlinx.coroutines.*
import kotlinx.coroutines.internal.*
import kotlinx.coroutines.internal.SegmentQueueSynchronizer.Mode.*
import kotlinx.coroutines.sync.*
import org.jetbrains.kotlinx.lincheck.*
import org.jetbrains.kotlinx.lincheck.annotations.Operation
import org.jetbrains.kotlinx.lincheck.execution.*
import org.jetbrains.kotlinx.lincheck.verifier.*
import org.junit.*
import kotlin.coroutines.*
import kotlin.coroutines.intrinsics.*
import kotlin.random.*
import kotlin.reflect.*
import kotlin.reflect.jvm.*

/*
  This test suite is not only but tests, but also provides a set of example
  on how the `SegmentQueueSynchronizer` abstraction can be used for different
  synchronization primitives.
 */

internal class SimpleSemaphoreAsync(permits: Int) : SegmentQueueSynchronizer<Unit>(ASYNC), Semaphore {
    private val _availablePermits = atomic(permits)
    override val availablePermits get() = error("Not implemented")

    override suspend fun acquire() {
        val p = _availablePermits.getAndDecrement()
        // Is the lock acquired?
        if (p > 0) return
        // Suspend otherwise
        suspendAtomicCancellableCoroutine<Unit> { cont ->
            check(suspend(cont)) { "Should not fail in ASYNC mode" }
        }
    }

    override fun tryAcquire() =  error("Not supported in the ASYNC version")

    override fun release() {
        while (true) {
            val p = _availablePermits.getAndIncrement()
            if (p >= 0) return // no waiters
            if (tryResume(Unit)) return // can fail due to cancellation
        }
    }
}

internal class SimpleSemaphoreAsyncSmart(permits: Int) : SegmentQueueSynchronizer<Unit>(ASYNC), Semaphore {
    private val _availablePermits = atomic(permits)
    override val availablePermits get() = error("Not implemented")

    override suspend fun acquire() {
        val p = _availablePermits.getAndDecrement()
        // Is the lock acquired?
        if (p > 0) return
        // Suspend otherwise
        suspendMyAtomicCancellableCoroutine(this::onCancellation) { cont ->
            check(suspend(cont)) { "Should not fail in ASYNC mode" }
        }
    }

    private fun onCancellation(cont: MyCancellableContinuationImpl<Unit>): Boolean {
        // TODO note that this code works only because
        // TODO cancellation can be invoked at most once.
        val p = _availablePermits.getAndIncrement()
        if (p >= 0) return false
        if (!cont.cancelImpl()) {
            resume(Unit)
            return true
        } else {
            return true
        }
    }

    override fun tryAcquire() =  error("Not supported in the ASYNC version")

    override fun release() {
        val p = _availablePermits.getAndIncrement()
        if (p >= 0) return // no waiters
        resume(Unit)
    }
}

abstract class SemaphoreAsyncLCStressTestBase(semaphore: Semaphore, seqSpec: KClass<*>)
    : SemaphoreLCStressTestBase(semaphore, seqSpec)
{
    override fun Options<*, *>.customize() = this.executionGenerator(CustomSemaphoreScenarioGenerator::class.java)

    class CustomSemaphoreScenarioGenerator(testConfiguration: CTestConfiguration, testStructure: CTestStructure)
        : ExecutionGenerator(testConfiguration, testStructure)
    {
        override fun nextExecution() = ExecutionScenario(
            emptyList(),
            generateParallelPart(testConfiguration.threads, testConfiguration.actorsPerThread),
            emptyList()
        )

        private fun generateParallelPart(threads: Int, actorsPerThread: Int) = (1..threads).map {
            val actors = ArrayList<Actor>()
            var acquiredPermits = 0
            repeat(actorsPerThread) {
                actors += if (acquiredPermits == 0 || Random.nextBoolean()) {
                    // acquire
                    acquiredPermits++
                    Actor(SemaphoreAsyncLCStressTestBase::acquire.javaMethod!!, emptyList(), emptyList(), Random.nextBoolean())
                } else {
                    // release
                    acquiredPermits--
                    Actor(SemaphoreAsyncLCStressTestBase::release.javaMethod!!, emptyList(), emptyList())
                }
            }
            actors
        }
    }
}

class SemaphoreAsync1LCStressTest : SemaphoreAsyncLCStressTestBase(SimpleSemaphoreAsync(1), SemaphoreSequential1::class)
class SemaphoreAsync2LCStressTest : SemaphoreAsyncLCStressTestBase(SimpleSemaphoreAsync(2), SemaphoreSequential2::class)

class SemaphoreAsyncSmart1LCStressTest : SemaphoreAsyncLCStressTestBase(SimpleSemaphoreAsyncSmart(1), SemaphoreSequential1::class)
class SemaphoreAsyncSmart2LCStressTest : SemaphoreAsyncLCStressTestBase(SimpleSemaphoreAsyncSmart(2), SemaphoreSequential2::class)







internal class SimpleCountDownLatch(count: Int) : SegmentQueueSynchronizer<Unit>(ASYNC) {
    private val count = atomic(count)
    private val waiters = atomic(0)

    fun countDown() {
        val r = count.decrementAndGet()
        if (r <= 0) releaseWaiters()
    }

    private fun releaseWaiters() {
        val w = waiters.getAndUpdate {
            // is the mark set?
            if (it and DONE_MARK != 0) return
            it or DONE_MARK
        }
        repeat(w) { tryResume(Unit) }
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

open class SimpleCountDownLatchSequential(count: Int) : VerifierState() {
    private var count = count
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

abstract class SimpleCountDownLatchLCStressTest(count: Int, val seqSpec: KClass<*>) {
    private val cdl = SimpleCountDownLatch(count)

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

class SimpleCountDownLatchSequential1 : SimpleCountDownLatchSequential(1)
class SimpleCountDownLatch1LCStressTest : SimpleCountDownLatchLCStressTest(1, SimpleCountDownLatchSequential1::class)

class SimpleCountDownLatchSequential2 : SimpleCountDownLatchSequential(2)
class SimpleCountDownLatch2LCStressTest : SimpleCountDownLatchLCStressTest(2, SimpleCountDownLatchSequential2::class)








internal class SimpleBarrier(private val parties: Int) : SegmentQueueSynchronizer<Boolean>(ASYNC) {
    private val arrived = atomic(0L)

    suspend fun arrive(): Boolean {
        val a = arrived.incrementAndGet()
        return when {
            a < parties -> {
                suspendMyAtomicCancellableCoroutine(this::onCancellation) { cont -> suspend(cont) }
            }
            a == parties.toLong() -> {
                repeat(parties - 1) {
                    tryResume(true)
                }
                true
            }
            else -> false
        }
    }

    private fun onCancellation(cont: MyCancellableContinuationImpl<Boolean>): Boolean {
        arrived.loop { cur ->
            if (cur >= parties) {
                cont.tryResume0(true)
                return false
            } else {
                // TODO this code works only because we guarantee
                // TODO that cancellation can be invoked at most once.
                if (arrived.compareAndSet(cur, cur - 1)) return false
            }
        }
    }
}

open class SimpleBarrierSequential(parties: Int) : VerifierState() {
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

abstract class SimpleBarrierLCStressTest(parties: Int, val seqSpec: KClass<*>) {
    private val b = SimpleBarrier(parties)

    @Operation
    suspend fun arrive() = b.arrive()

    @Test
    fun test() = LCStressOptionsDefault()
        .actorsBefore(0)
        .actorsAfter(0)
        .threads(3)
        .sequentialSpecification(seqSpec.java)
        .check(this::class)
}

class SimpleBarrierSequential1 : SimpleBarrierSequential(1)
class SimpleBarrier1LCStressTest : SimpleBarrierLCStressTest(1, SimpleBarrierSequential1::class)
class SimpleBarrierSequential2 : SimpleBarrierSequential(2)
class SimpleBarrier2LCStressTest : SimpleBarrierLCStressTest(2, SimpleBarrierSequential2::class)
class SimpleBarrierSequential3 : SimpleBarrierSequential(3)
class SimpleBarrier3LCStressTest : SimpleBarrierLCStressTest(3, SimpleBarrierSequential3::class)








internal class SimpleBlockingQueue<T: Any> : SegmentQueueSynchronizer<T>(SYNC) {
    private val balance = atomic(0L) // #add  - #poll

    private val elements = atomicArrayOfNulls<Any?>(100) // This is an infinite array by design :)
    private val adds = atomic(0)
    private val polls = atomic(0)

    fun add(x: T) {
        while (true) {
            val b = balance.getAndIncrement()
            if (b < 0) {
                if (tryResume(x)) return
            } else {
                if (tryInsert(x)) return
            }
        }
    }

    private fun tryInsert(x: T): Boolean {
        val i = adds.getAndIncrement()
        return elements[i].compareAndSet(null, x)
    }

    suspend fun poll(): T {
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

class SimpleBlockingQueueIntSequential : VerifierState() {
    private val elements = ArrayList<Int>()
    private val waiters = ArrayList<CancellableContinuation<Int>>()

    fun add(x: Int) {
        while (true) {
            if (waiters.isNotEmpty()) {
                val w = waiters.removeAt(0)
                if (w.tryResume0(x)) return
            } else {
                elements.add(x)
                return
            }
        }
    }

    suspend fun poll(): Int =
        if (elements.isNotEmpty()) {
            elements.removeAt(0)
        } else {
            suspendAtomicCancellableCoroutine { cont ->
                waiters.add(cont)
            }
        }

    override fun extractState() = elements
}

class SimpleBlockingQueueLCStressTest {
    private val q = SimpleBlockingQueue<Int>()

    @Operation
    fun add(x: Int) = q.add(x)

    @Operation
    suspend fun poll(): Int = q.poll()

    @Test
    fun test() = LCStressOptionsDefault()
        .sequentialSpecification(SimpleBlockingQueueIntSequential::class.java)
        .check(this::class)
}









internal class SimpleBlockingStack<T: Any> : SegmentQueueSynchronizer<T>(SYNC) {
    private val head = atomic<StackNode<T>?>(null)
    private val balance = atomic(0) // #put - #pop

    fun put(x: T) {
        while (true) {
            val b = balance.getAndIncrement()
            if (b < 0) {
                if (tryResume(x)) return
            } else {
                if (tryInsert(x)) return
            }
        }
    }

    private fun tryInsert(x: T): Boolean {
        while (true) {
            val h = head.value
            if (h != null && h.element == null) {
                if (head.compareAndSet(h, h.next)) return false
            } else {
                val newHead = StackNode(x, h)
                if (head.compareAndSet(h, newHead)) return true
            }
        }
    }

    suspend fun pop(): T {
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

class SimpleBlockingStackIntSequential : VerifierState() {
    private val elements = ArrayList<Int>()
    private val waiters = ArrayList<CancellableContinuation<Int>>()

    fun put(x: Int) {
        while (true) {
            if (waiters.isNotEmpty()) {
                val w = waiters.removeAt(0)
                if (w.tryResume0(x)) return
            } else {
                elements.add(x)
                return
            }
        }
    }

    suspend fun pop(): Int =
        if (elements.isNotEmpty()) {
            elements.removeAt(elements.size - 1)
        } else {
            suspendAtomicCancellableCoroutine { cont ->
                waiters.add(cont)
            }
        }

    override fun extractState() = elements
}

class SimpleBlockingStackLCStressTest {
    private val s = SimpleBlockingStack<Int>()

    @Operation
    fun put(x: Int) = s.put(x)

    @Operation()
    suspend fun pop(): Int = s.pop()

    @Test
    fun test() = LCStressOptionsDefault()
        .sequentialSpecification(SimpleBlockingStackIntSequential::class.java)
        .check(this::class)
}








private fun <T> CancellableContinuation<T>.tryResume0(value: T): Boolean {
    val token = tryResume(value) ?: return false
    completeResume(token)
    return true
}


internal class MyCancellableContinuationImpl<T>(
    delegate: Continuation<T>,
    resumeMode: Int,
    val beforeCancelAction: (MyCancellableContinuationImpl<T>) -> Boolean
) : CancellableContinuationImpl<T>(delegate, resumeMode) {

    private val firstCancel = atomic(false)

    override fun cancel(cause: Throwable?): Boolean {
        // Is this the first `cancel` invocation?
        if (!firstCancel.compareAndSet(false, true)) return false
        // Perform the cancellation
        if (beforeCancelAction(this)) return true
        return cancelImpl(cause)
    }

    fun cancelImpl(cause: Throwable? = null): Boolean  = super.cancel(cause)
}

internal suspend inline fun <T> suspendMyAtomicCancellableCoroutine(
    noinline beforeCancelAction: (MyCancellableContinuationImpl<T>) -> Boolean,
    crossinline block: (CancellableContinuationImpl<T>) -> Unit
): T =
    suspendCoroutineUninterceptedOrReturn { uCont ->
        val cancellable = MyCancellableContinuationImpl(uCont.intercepted(), MODE_ATOMIC_DEFAULT, beforeCancelAction)
        block(cancellable)
        cancellable.getResult()
    }