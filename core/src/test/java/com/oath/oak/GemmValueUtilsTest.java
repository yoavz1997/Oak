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
        System.out.println("STARTED testCannotReadLockDeleted");
        assertEquals(TRUE, operator.deleteValue(s, 0));
        assertEquals(FALSE, operator.lockRead(s, 0));
        System.out.println("ENDED testCannotReadLockDeleted");
    }

    @Test
    public void testCannotWriteLockDeleted() {
        System.out.println("STARTED testCannotWriteLockDeleted");
        assertEquals(TRUE, operator.deleteValue(s, 0));
        assertEquals(FALSE, operator.lockWrite(s, 0));
        System.out.println("ENDED testCannotWriteLockDeleted");
    }

    @Test
    public void testCannotDeletedMultipleTimes() {
        System.out.println("STARTED testCannotDeletedMultipleTimes");
        assertEquals(TRUE, operator.deleteValue(s, 0));
        assertEquals(FALSE, operator.deleteValue(s, 0));
        System.out.println("ENDED testCannotDeletedMultipleTimes");
    }

    @Test
    public void testCanReadLockMultipleTimes() {
        System.out.println("STARTED testCanReadLockMultipleTimes");
        for (int i = 0; i < 10000; i++) {
            assertEquals(TRUE, operator.lockRead(s, 0));
        }
        System.out.println("ENDED testCanReadLockMultipleTimes");
    }

    @Test
    public void testCannotWriteLockReadLocked() throws InterruptedException {
        System.out.println("STARTED testCannotWriteLockReadLocked");
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
        System.out.println("ENDED testCannotWriteLockReadLocked");
    }

    @Test
    public void testCannotDeletedReadLocked() throws InterruptedException {
        System.out.println("STARTED testCannotDeletedReadLocked");
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
        System.out.println("ENDED testCannotDeletedReadLocked");
    }

    @Test
    public void testCannotReadLockWriteLocked() throws InterruptedException {
        System.out.println("STARTED testCannotReadLockWriteLocked");
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
        System.out.println("ENDED testCannotReadLockWriteLocked");
    }

    @Test
    public void testCannotWriteLockMultipleTimes() throws InterruptedException {
        System.out.println("STARTED testCannotWriteLockMultipleTimes");
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
        System.out.println("ENDED testCannotWriteLockMultipleTimes");
    }

    @Test
    public void testCannotDeletedWriteLocked() throws InterruptedException {
        System.out.println("STARTED testCannotDeletedWriteLocked");
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
        System.out.println("ENDED testCannotDeletedWriteLocked");
    }

    private void changeGeneration() {
        for (int i = 0; i < GemmAllocator.RELEASE_LIST_LIMIT; i++) {
            gemmAllocator.releaseSlice(gemmAllocator.allocateSlice(1));
        }
    }

    @Test
    public void testCannotReadLockDifferentGeneration() {
        System.out.println("STARTED testCannotReadLockDifferentGeneration");
        assertEquals(RETRY, operator.lockRead(s, 1));
        System.out.println("ENDED testCannotReadLockDifferentGeneration");
    }

    @Test
    public void testCannotWriteLockDifferentGeneration() {
        System.out.println("STARTED testCannotWriteLockDifferentGeneration");
        assertEquals(RETRY, operator.lockWrite(s, 1));
        System.out.println("ENDED testCannotWriteLockDifferentGeneration");
    }

    @Test
    public void testCannotDeletedDifferentGeneration() {
        System.out.println("STARTED testCannotDeletedDifferentGeneration");
        assertEquals(RETRY, operator.deleteValue(s, 1));
        System.out.println("ENDED testCannotDeletedDifferentGeneration");
    }
}
