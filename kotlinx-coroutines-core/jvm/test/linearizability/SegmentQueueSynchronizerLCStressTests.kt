/*
 * Copyright 2016-2020 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

@file:Suppress("unused")
package kotlinx.coroutines.linearizability

import kotlinx.atomicfu.*
import kotlinx.coroutines.*
import kotlinx.coroutines.internal.*
import kotlinx.coroutines.internal.SegmentQueueSynchronizer.Mode.*
import org.jetbrains.kotlinx.lincheck.annotations.Operation
import org.jetbrains.kotlinx.lincheck.verifier.*
import org.junit.*
import kotlin.coroutines.*

/*
  This test suite is not only but tests, but also provides a set of example
  on how the `SegmentQueueSynchronizer` abstraction can be used for different
  synchronization primitives.
 */

internal class SimpleMutex : SegmentQueueSynchronizer<Unit>(ASYNC) {
    private val state = atomic(-1) // -1 -- locked, x>=0 -- number of waiters

    fun isLocked() = state.value != -1

    suspend fun lock() {
        val s = state.getAndIncrement()
        // Is the lock acquired?
        if (s == -1) return
        // Suspend otherwise
        suspendAtomicCancellableCoroutineReusable<Unit> { cont ->
            check(suspend(cont)) { "Should not fail in ASYNC mode" }
        }
    }

    fun release() {
        while (true) {
            val s = state.getAndUpdate { cur ->
                if (cur == -1) throw IllegalStateException("This mutex is unlocked")
                cur - 1
            }
            if (s == 0) return // no waiters
            if (tryResume(Unit)) return
        }
    }
}

class SimpleMutexLCSressTest : VerifierState() {
    private val m = SimpleMutex()

    @Operation
    suspend fun lock() = m.lock()

    @Operation(handleExceptionsAsResult = [IllegalStateException::class])
    fun release() = m.release()

    override fun extractState() = m.isLocked()

    @Test
    fun test() = LCStressOptionsDefault()
        .actorsBefore(0)
        .actorsAfter(0)
        .check(this::class)

}

class SimpleMutexStressTest {
    @Test
    fun testSimple() = runBlocking {
        val m = SimpleMutex()
        check(!m.isLocked())
        m.lock()
        check(m.isLocked())
        m.release()
        check(!m.isLocked())
    }

    @Test
    fun test() = runBlocking {
        val t = 10
        val n = 100_000
        val m = SimpleMutex()
        var c = 0
        val jobs = (1..t).map { GlobalScope.launch {
            repeat(n) {
                m.lock()
                c++
                m.release()
            }
        } }
        jobs.forEach { it.join() }
        assert(c == n * t)
    }
}


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
        suspendAtomicCancellableCoroutineReusable<Unit> { suspend(it) }
    }

    fun remaining(): Int = count.value.coerceAtLeast(0)
}
private const val DONE_MARK = 1 shl 31

abstract class SimpleCountDownLatchLCStressTest(count: Int) : VerifierState() {
    private val cdl = SimpleCountDownLatch(count)

    @Operation
    fun countDown() = cdl.countDown()

    @Operation
    fun remaining() = cdl.remaining()

    @Operation
    suspend fun await() = cdl.await()

    override fun extractState() = remaining()

    @Test
    fun test() = LCStressOptionsDefault()
        .actorsBefore(0)
        .actorsAfter(0)
        .check(this::class)
}
class SimpleCountDownLatch1LCStressTest : SimpleCountDownLatchLCStressTest(1)
class SimpleCountDownLatch2LCStressTest : SimpleCountDownLatchLCStressTest(2)


internal class SimpleBarrier(private val parties: Int) : SegmentQueueSynchronizer<Boolean>(ASYNC) {
    private val arrived = atomic(0L)

    val done get() = arrived.value >= parties

    suspend fun arrive(): Boolean {
        val a = arrived.incrementAndGet()
        return when {
            a < parties -> {
                suspendAtomicCancellableCoroutineReusable { cont -> suspend(cont) }
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
}

class SimpleBarrierLCStressTest : VerifierState() {
    private val b = SimpleBarrier(3)

    @Operation(cancellableOnSuspension = false)
    suspend fun arrive() = b.arrive()

    override fun extractState() = b.done

    @Test
    fun test() = LCStressOptionsDefault()
        .actorsBefore(0)
        .actorsAfter(0)
        .threads(3)
        .check(this::class)
}

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
                val x = suspendAtomicCancellableCoroutineReusable<T?> { cont ->
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
                val x = suspendAtomicCancellableCoroutineReusable<T?> { cont ->
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

internal class SimpleBlockingStackIntSequential : VerifierState() {
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

    @Operation(cancellableOnSuspension = false)
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