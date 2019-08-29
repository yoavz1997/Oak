package com.oath.oak;

import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.oath.oak.ValueUtils.ValueResult.FAILURE;
import static com.oath.oak.ValueUtils.ValueResult.SUCCESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ValueUtilsTest {
    private GemmAllocator gemmAllocator;
    private Slice s;

    @Before
    public void init() {
        gemmAllocator = new GemmAllocator(new OakNativeMemoryAllocator(128));
        s = gemmAllocator.allocateSlice(16);
        s.getByteBuffer().putInt(s.getByteBuffer().position(), 0);
    }

    @Test
    public void testCannotReadLockDeleted() {
        assertEquals(SUCCESS, ValueUtils.remove(s, gemmAllocator));
        assertEquals(FAILURE, ValueUtils.lockRead(s));
    }

    @Test
    public void testCannotWriteLockDeleted() {
        assertEquals(SUCCESS, ValueUtils.remove(s, gemmAllocator));
        assertEquals(FAILURE, ValueUtils.compute(s, buffer -> {
        }));
    }

    @Test
    public void testCannotDeletedMultipleTimes() {
        assertEquals(SUCCESS, ValueUtils.remove(s, gemmAllocator));
        assertEquals(FAILURE, ValueUtils.remove(s, gemmAllocator));
    }

    @Test
    public void testCanReadLockMultipleTimes() {
        for (int i = 0; i < 10000; i++) {
            assertEquals(SUCCESS, ValueUtils.lockRead(s));
        }
    }

    @Test
    public void testCannotWriteLockReadLocked() throws InterruptedException {
        AtomicInteger flag = new AtomicInteger(0);
        Thread writer = new Thread(() -> {
            assertEquals(SUCCESS, ValueUtils.compute(s, oakWBuffer -> {
            }));
            assertEquals(2, flag.get());
        });
        Thread reader = new Thread(() -> {
            assertEquals(SUCCESS, ValueUtils.lockRead(s));
            flag.incrementAndGet();
            ValueUtils.unlockRead(s);
        });
        assertEquals(SUCCESS, ValueUtils.lockRead(s));
        writer.start();
        Thread.sleep(2000);
        reader.start();
        Thread.sleep(2000);
        flag.incrementAndGet();
        ValueUtils.unlockRead(s);
        reader.join();
        writer.join();
    }

    @Test
    public void testCannotDeletedReadLocked() throws InterruptedException {
        AtomicInteger flag = new AtomicInteger(0);
        Thread deleter = new Thread(() -> {
            assertEquals(SUCCESS, ValueUtils.remove(s, gemmAllocator));
            assertEquals(2, flag.get());
        });
        Thread reader = new Thread(() -> {
            assertEquals(SUCCESS, ValueUtils.lockRead(s));
            flag.incrementAndGet();
            ValueUtils.unlockRead(s);
        });
        assertEquals(SUCCESS, ValueUtils.lockRead(s));
        deleter.start();
        Thread.sleep(2000);
        reader.start();
        Thread.sleep(2000);
        flag.incrementAndGet();
        ValueUtils.unlockRead(s);
        reader.join();
        deleter.join();
    }

    @Test
    public void testCannotReadLockWriteLocked() throws InterruptedException {
        AtomicBoolean flag = new AtomicBoolean(false);
        Thread reader = new Thread(() -> {
            assertEquals(SUCCESS, ValueUtils.lockRead(s));
            assertTrue(flag.get());
        });
        assertEquals(SUCCESS, ValueUtils.lockWrite(s.getByteBuffer()));
        reader.start();
        Thread.sleep(2000);
        flag.set(true);
        ValueUtils.unlockWrite(s.getByteBuffer());
        reader.join();
    }

    @Test
    public void testCannotWriteLockMultipleTimes() throws InterruptedException {
        AtomicBoolean flag = new AtomicBoolean(false);
        Thread writer = new Thread(() -> {
            assertEquals(SUCCESS, ValueUtils.lockWrite(s.getByteBuffer()));
            assertTrue(flag.get());
        });
        assertEquals(SUCCESS, ValueUtils.lockWrite(s.getByteBuffer()));
        writer.start();
        Thread.sleep(2000);
        flag.set(true);
        ValueUtils.unlockWrite(s.getByteBuffer());
        writer.join();
    }

    @Test
    public void testCannotDeletedWriteLocked() throws InterruptedException {
        AtomicBoolean flag = new AtomicBoolean(false);
        Thread deleter = new Thread(() -> {
            assertEquals(SUCCESS, ValueUtils.remove(s, gemmAllocator));
            assertTrue(flag.get());
        });
        assertEquals(SUCCESS, ValueUtils.lockWrite(s.getByteBuffer()));
        deleter.start();
        Thread.sleep(2000);
        flag.set(true);
        ValueUtils.unlockWrite(s.getByteBuffer());
        deleter.join();
    }

    @Test
    public void testCannotReadLockDifferentGeneration() {

    }

    @Test
    public void testCannotWriteLockDifferentGeneration() {

    }

    @Test
    public void testCannotDeletedDifferentGeneration() {

    }
}
