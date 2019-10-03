package com.oath.oak.MemoryManagment;

import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;
import com.oath.oak.Slice;
import com.oath.oak.StringSerializer;
import com.oath.oak.ThreadIndexCalculator;
import org.junit.Test;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.oath.oak.MemoryManagment.Result.FALSE;
import static com.oath.oak.MemoryManagment.Result.RETRY;
import static com.oath.oak.MemoryManagment.Result.TRUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OffHeapSliceTest {
    private final NovaValueUtilitiesImpl utilities = new NovaValueUtilitiesImpl();
    private NovaManager manager = new NovaManagerImpl(new OakNativeMemoryAllocator(128), utilities);

    @Test
    public void writeToOffHeapSliceReadFromByteBufferTest() {
        int size = 32;
        OffHeapSlice offHeapSlice = manager.allocateSlice(size);
        Slice slice = offHeapSlice.intoSlice();
        assertEquals(size + utilities.getHeaderSize(), slice.getByteBuffer().remaining());
        int numOfChars = (size - Integer.BYTES) / Character.BYTES;
        Random random = new Random();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < numOfChars; i++) {
            builder.append((char) random.nextInt(Character.MAX_VALUE));
        }
        StringSerializer serializer = new StringSerializer();
        String string = builder.toString();
        assert serializer.calculateSize(string) == size;
        assertEquals(TRUE, offHeapSlice.put(string, serializer));
        String readResult = serializer.deserialize(utilities.getActualReadOnlyBuffer(slice.getByteBuffer()));
        assertEquals(string, readResult);
    }

    @Test
    public void writeToByteBufferReadFromOffHeapSliceTest() {
        int size = 32;
        OffHeapSlice offHeapSlice = manager.allocateSlice(size);
        Slice slice = offHeapSlice.intoSlice();
        assertEquals(size + utilities.getHeaderSize(), slice.getByteBuffer().remaining());
        int numOfChars = (size - Integer.BYTES) / Character.BYTES;
        Random random = new Random();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < numOfChars; i++) {
            builder.append((char) random.nextInt(Character.MAX_VALUE));
        }
        StringSerializer serializer = new StringSerializer();
        String string = builder.toString();
        assert serializer.calculateSize(string) == size;
        serializer.serialize(string, utilities.getActualBuffer(slice.getByteBuffer()));
        Map.Entry<Result, String> resultStringEntry = offHeapSlice.get(serializer);
        assertEquals(TRUE, resultStringEntry.getKey());
        assertEquals(string, resultStringEntry.getValue());
    }

    @Test
    public void cannotPutToDeletedSliceTest() {
        int size = 32;
        OffHeapSlice offHeapSlice = manager.allocateSlice(size);
        assertEquals(TRUE, offHeapSlice.delete(null).getKey());
        assertEquals(FALSE, offHeapSlice.put("hello", new StringSerializer()));
    }

    @Test
    public void concurrentPutTest() throws InterruptedException {
        int size = 32;
        OffHeapSlice offHeapSlice = manager.allocateSlice(size);
        AtomicBoolean successFlag = new AtomicBoolean(true);
        final int numOfThreads = ThreadIndexCalculator.MAX_THREADS - 1;
        Thread[] threads = new Thread[numOfThreads];
        for (int i = 0; i < numOfThreads; i++) {
            threads[i] = new Thread(() -> {
                StringSerializer serializer = new StringSerializer();
                String string;
                for (int k = 0; k < 100000; k++) {
                    int numOfChars = (size - Integer.BYTES) / Character.BYTES;
                    Random random = new Random();
                    StringBuilder builder = new StringBuilder();
                    for (int j = 0; j < numOfChars; j++) {
                        builder.append((char) random.nextInt(Character.MAX_VALUE));
                    }
                    string = builder.toString();
                    if (offHeapSlice.put(string, serializer) != TRUE) {
                        successFlag.set(false);
                    }
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }
        assertTrue(successFlag.get());
    }

    @Test
    public void concurrentExchangeTest() throws InterruptedException {
        int size = 32;
        OffHeapSlice offHeapSlice = manager.allocateSlice(size);
        offHeapSlice.put("", new StringSerializer());
        AtomicBoolean successFlag = new AtomicBoolean(true);
        final int numOfThreads = ThreadIndexCalculator.MAX_THREADS - 1;
        Thread[] threads = new Thread[numOfThreads];
        for (int i = 0; i < numOfThreads; i++) {
            threads[i] = new Thread(() -> {
                StringSerializer serializer = new StringSerializer();
                String string;
                for (int k = 0; k < 10000; k++) {
                    int numOfChars = (size - Integer.BYTES) / Character.BYTES;
                    Random random = new Random();
                    StringBuilder builder = new StringBuilder();
                    for (int j = 0; j < numOfChars; j++) {
                        builder.append((char) random.nextInt(Character.MAX_VALUE));
                    }
                    string = builder.toString();
                    Map.Entry<Result, String> entry = offHeapSlice.exchange(string, serializer);
                    if (entry.getKey() != TRUE || entry.getValue() == null) {
                        successFlag.set(false);
                    }
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }
        assertTrue(successFlag.get());
    }

    @Test
    public void exchangeTest() {
        int size = 32;
        OffHeapSlice offHeapSlice = manager.allocateSlice(size);
        int numOfChars = (size - Integer.BYTES) / Character.BYTES;
        Random random = new Random();
        StringBuilder builder1 = new StringBuilder();
        StringBuilder builder2 = new StringBuilder();
        for (int i = 0; i < numOfChars; i++) {
            builder1.append((char) random.nextInt(Character.MAX_VALUE));
            builder2.append((char) random.nextInt(Character.MAX_VALUE));
        }
        StringSerializer serializer = new StringSerializer();
        String string1 = builder1.toString();
        String string2 = builder2.toString();
        assertEquals(TRUE, offHeapSlice.put(string1, serializer));
        Map.Entry<Result, String> resultStringEntry = offHeapSlice.exchange(string2, serializer);
        assertEquals(TRUE, resultStringEntry.getKey());
        assertEquals(string1, resultStringEntry.getValue());
        resultStringEntry = offHeapSlice.get(serializer);
        assertEquals(TRUE, resultStringEntry.getKey());
        assertEquals(string2, resultStringEntry.getValue());
    }

    @Test
    public void cannotExchangeDeletedSliceTest() {
        int size = 32;
        OffHeapSlice offHeapSlice = manager.allocateSlice(size);
        assertEquals(TRUE, offHeapSlice.delete(null).getKey());
        assertEquals(FALSE, offHeapSlice.exchange("hello", new StringSerializer()).getKey());
    }

    @Test
    public void putThenGetTest() {
        int size = 32;
        OffHeapSlice offHeapSlice = manager.allocateSlice(size);
        Slice slice = offHeapSlice.intoSlice();
        assertEquals(size + utilities.getHeaderSize(), slice.getByteBuffer().remaining());
        int numOfChars = (size - Integer.BYTES) / Character.BYTES;
        Random random = new Random();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < numOfChars; i++) {
            builder.append((char) random.nextInt(Character.MAX_VALUE));
        }
        StringSerializer serializer = new StringSerializer();
        String string = builder.toString();
        assert serializer.calculateSize(string) == size;
        assertEquals(TRUE, offHeapSlice.put(string, serializer));
        Map.Entry<Result, String> resultStringEntry = offHeapSlice.get(serializer);
        assertEquals(TRUE, resultStringEntry.getKey());
        assertEquals(string, resultStringEntry.getValue());
    }

    @Test
    public void cannotReadDeletedSliceTest() {
        int size = 32;
        OffHeapSlice offHeapSlice = manager.allocateSlice(size);
        assertEquals(TRUE, offHeapSlice.delete(null).getKey());
        assertEquals(FALSE, offHeapSlice.get(new StringSerializer()).getKey());
    }

    @Test
    public void successfulCasTest() {
        int size = 32;
        OffHeapSlice offHeapSlice = manager.allocateSlice(size);
        int numOfChars = (size - Integer.BYTES) / Character.BYTES;
        Random random = new Random();
        StringBuilder builder1 = new StringBuilder();
        StringBuilder builder2 = new StringBuilder();
        for (int i = 0; i < numOfChars; i++) {
            builder1.append((char) random.nextInt(Character.MAX_VALUE));
            builder2.append((char) random.nextInt(Character.MAX_VALUE));
        }
        StringSerializer serializer = new StringSerializer();
        String string1 = builder1.toString();
        String string2 = builder2.toString();
        assertEquals(TRUE, offHeapSlice.put(string1, serializer));
        assertEquals(TRUE, offHeapSlice.compareAndExchange(string1, string2, new StringSerializer(),
                String::compareTo));
        Map.Entry<Result, String> resultStringEntry = offHeapSlice.get(serializer);
        assertEquals(TRUE, resultStringEntry.getKey());
        assertEquals(string2, resultStringEntry.getValue());
    }

    @Test
    public void failedCasTest() {
        int size = 32;
        OffHeapSlice offHeapSlice = manager.allocateSlice(size);
        int numOfChars = (size - Integer.BYTES) / Character.BYTES;
        Random random = new Random();
        StringBuilder builder1 = new StringBuilder();
        StringBuilder builder2 = new StringBuilder();
        for (int i = 0; i < numOfChars; i++) {
            builder1.append((char) random.nextInt(Character.MAX_VALUE));
            builder2.append((char) random.nextInt(Character.MAX_VALUE));
        }
        StringSerializer serializer = new StringSerializer();
        String string1 = builder1.toString();
        String string2 = builder2.toString();
        assertEquals(TRUE, offHeapSlice.put(string1, serializer));
        assertEquals(FALSE, offHeapSlice.compareAndExchange(string2, string1, new StringSerializer(),
                String::compareTo));
        Map.Entry<Result, String> resultStringEntry = offHeapSlice.get(serializer);
        assertEquals(TRUE, resultStringEntry.getKey());
        assertEquals(string1, resultStringEntry.getValue());
    }

    @Test
    public void cannotDeleteSliceTwiceTest() {
        int size = 32;
        OffHeapSlice offHeapSlice = manager.allocateSlice(size);
        assertEquals(TRUE, offHeapSlice.delete(null).getKey());
        assertEquals(FALSE, offHeapSlice.delete(null).getKey());
    }

    @Test
    public void cannotComputeOnADeletedSliceTest() {
        int size = 32;
        OffHeapSlice offHeapSlice = manager.allocateSlice(size);
        assertEquals(TRUE, offHeapSlice.delete(null).getKey());
        assertEquals(FALSE, offHeapSlice.compute(v -> v, new StringSerializer()));
    }

    // This test has knowledge of OffHeapSliceImpl and NovaManagerImpl.
    // It tests that put after the slice is reused fails.
    @Test
    public void cannotPutToSliceWithDifferentVersion() {
        int size = 32;
        OffHeapSlice offHeapSlice = manager.allocateSlice(size);
        Slice slice = offHeapSlice.intoSlice();
        for (int i = 0; i < NovaManagerImpl.RELEASE_LIST_LIMIT - 1; i++) {
            OffHeapSlice s = manager.allocateSlice(1);
            s.delete(null);
        }
        offHeapSlice.delete(null);

        OffHeapSlice other = manager.allocateSlice(size);
        Slice otherSlice = other.intoSlice();
        assertEquals(slice.getBlockID(), otherSlice.getBlockID());
        assertEquals(slice.getByteBuffer().position(), otherSlice.getByteBuffer().position());
        assertEquals(slice.getByteBuffer().remaining(), otherSlice.getByteBuffer().remaining());

        assertEquals(RETRY, offHeapSlice.put("Hello", new StringSerializer()));
        assertEquals(RETRY, offHeapSlice.exchange("Hello", new StringSerializer()).getKey());
        assertEquals(RETRY, offHeapSlice.compute(v -> v, new StringSerializer()));
        assertEquals(RETRY, offHeapSlice.get(new StringSerializer()).getKey());
        assertEquals(RETRY, offHeapSlice.compareAndExchange("Hello", "Bye", new StringSerializer(), String::compareTo));
        assertEquals(RETRY, offHeapSlice.delete(new StringSerializer()).getKey());
    }
}
