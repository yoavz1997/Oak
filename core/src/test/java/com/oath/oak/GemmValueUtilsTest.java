package com.oath.oak;

import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.oath.oak.GemmValueUtils.Result.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GemmValueUtilsTest {
    private GemmAllocator gemmAllocator;
    private Slice s;
    private final GemmValueUtils operator = new GemmValueOperationsImpl();

    @Before
    public void init() {
        gemmAllocator = new GemmAllocator(new OakNativeMemoryAllocator(128));
        s = gemmAllocator.allocateSlice(16);
        s.getByteBuffer().putInt(s.getByteBuffer().position(), 0);
    }

    @Test
    public void testCannotReadLockDeleted() {
        assertEquals(TRUE, operator.deleteValue(s, 0));
        assertEquals(FALSE, operator.lockRead(s, 0));
    }

    @Test
    public void testCannotWriteLockDeleted() {
        assertEquals(TRUE, operator.deleteValue(s, 0));
        assertEquals(FALSE, operator.lockWrite(s, 0));
    }

    @Test
    public void testCannotDeletedMultipleTimes() {
        assertEquals(TRUE, operator.deleteValue(s, 0));
        assertEquals(FALSE, operator.deleteValue(s, 0));
    }

    @Test
    public void testCanReadLockMultipleTimes() {
        for (int i = 0; i < 10000; i++) {
            assertEquals(TRUE, operator.lockRead(s, 0));
        }
    }

    @Test
    public void testCannotWriteLockReadLocked() throws InterruptedException {
        AtomicInteger flag = new AtomicInteger(0);
        CyclicBarrier barrier = new CyclicBarrier(3);
        Thread writer = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            assertEquals(TRUE, operator.lockWrite(s, 0));
            assertEquals(2, flag.get());
        });
        Thread reader = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            assertEquals(TRUE, operator.lockRead(s, 0));
            flag.incrementAndGet();
            operator.unlockRead(s, 0);
        });
        assertEquals(TRUE, operator.lockRead(s, 0));
        writer.start();
        reader.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        flag.incrementAndGet();
        operator.unlockRead(s, 0);
        reader.join();
        writer.join();
    }

    @Test
    public void testCannotDeletedReadLocked() throws InterruptedException {
        AtomicInteger flag = new AtomicInteger(0);
        CyclicBarrier barrier = new CyclicBarrier(3);
        Thread deleter = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            assertEquals(TRUE, operator.deleteValue(s, 0));
            assertEquals(2, flag.get());
        });
        Thread reader = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            assertEquals(TRUE, operator.lockRead(s, 0));
            flag.incrementAndGet();
            operator.unlockRead(s, 0);
        });
        assertEquals(TRUE, operator.lockRead(s, 0));
        deleter.start();
        reader.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        flag.incrementAndGet();
        operator.unlockRead(s, 0);
        reader.join();
        deleter.join();
    }

    @Test
    public void testCannotReadLockWriteLocked() throws InterruptedException {
        AtomicBoolean flag = new AtomicBoolean(false);
        CyclicBarrier barrier = new CyclicBarrier(2);
        Thread reader = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            assertEquals(TRUE, operator.lockRead(s, 0));
            assertTrue(flag.get());
        });
        assertEquals(TRUE, operator.lockWrite(s, 0));
        reader.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        flag.set(true);
        operator.unlockWrite(s);
        reader.join();
    }

    @Test
    public void testCannotWriteLockMultipleTimes() throws InterruptedException {
        AtomicBoolean flag = new AtomicBoolean(false);
        CyclicBarrier barrier = new CyclicBarrier(2);
        Thread writer = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            assertEquals(TRUE, operator.lockWrite(s, 0));
            assertTrue(flag.get());
        });
        assertEquals(TRUE, operator.lockWrite(s, 0));
        writer.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        flag.set(true);
        operator.unlockWrite(s);
        writer.join();
    }

    @Test
    public void testCannotDeletedWriteLocked() throws InterruptedException {
        AtomicBoolean flag = new AtomicBoolean(false);
        CyclicBarrier barrier = new CyclicBarrier(2);
        Thread deleter = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            assertEquals(TRUE, operator.deleteValue(s, 0));
            assertTrue(flag.get());
        });
        assertEquals(TRUE, operator.lockWrite(s, 0));
        deleter.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        flag.set(true);
        operator.unlockWrite(s);
        deleter.join();
    }

    private void changeGeneration() {
        for (int i = 0; i < GemmAllocator.RELEASE_LIST_LIMIT; i++) {
            gemmAllocator.releaseSlice(gemmAllocator.allocateSlice(1));
        }
    }

    @Test
    public void testCannotReadLockDifferentGeneration() {
        assertEquals(RETRY, operator.lockRead(s, 1));
    }

    @Test
    public void testCannotWriteLockDifferentGeneration() {
        assertEquals(RETRY, operator.lockWrite(s, 1));
    }

    @Test
    public void testCannotDeletedDifferentGeneration() {
        assertEquals(RETRY, operator.deleteValue(s, 1));
    }
}