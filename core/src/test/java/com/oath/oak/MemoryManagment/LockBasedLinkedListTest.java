package com.oath.oak.MemoryManagment;

import com.oath.oak.MemoryManagment.ExampleList.LockBasedLinkedList;
import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;
import com.oath.oak.OakComparator;
import com.oath.oak.OakSerializer;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LockBasedLinkedListTest {

    private final OakSerializer<Integer> integerSerializer = new OakSerializer<Integer>() {

        @Override
        public void serialize(Integer obj, ByteBuffer targetBuffer) {
            targetBuffer.putInt(targetBuffer.position(), obj);
        }

        @Override
        public Integer deserialize(ByteBuffer serializedObj) {
            return serializedObj.getInt(serializedObj.position());
        }

        @Override
        public int calculateSize(Integer key) {
            return Integer.BYTES;
        }

    };
    private final OakComparator<Integer> integerComparator = new OakComparator<Integer>() {
        @Override
        public int compareKeys(Integer key1, Integer key2) {
            return key1.compareTo(key2);
        }

        @Override
        public int compareSerializedKeys(ByteBuffer serializedKey1, ByteBuffer serializedKey2) {
            return Integer.compare(serializedKey1.getInt(0), serializedKey2.getInt(0));
        }

        @Override
        public int compareSerializedKeyAndKey(ByteBuffer serializedKey, Integer key) {
            return Integer.compare(serializedKey.getInt(0), key);
        }
    };

    private final LockBasedLinkedList<Integer, Integer> myList =
            new LockBasedLinkedList<>(new NovaManagerImpl(new OakNativeMemoryAllocator(128),
                    new NovaValueUtilitiesImpl()), integerSerializer, Integer.MIN_VALUE, Integer.MAX_VALUE,
                    integerSerializer, integerComparator);


    @Test
    public void sanityCheck() throws InterruptedException {
        int numOfThreads = 1; //ThreadIndexCalculator.MAX_THREADS - 1;
        int keyRange = 32;
        AtomicIntegerArray successfulPuts = new AtomicIntegerArray(keyRange);
        AtomicIntegerArray successfulRemoves = new AtomicIntegerArray(keyRange);
        Thread[] threads = new Thread[numOfThreads];
        CyclicBarrier barrier = new CyclicBarrier(numOfThreads + 1);
        AtomicBoolean stop = new AtomicBoolean(false);
        for (int i = 0; i < numOfThreads; i++) {
            final int tid = i;
            threads[i] = new Thread(() -> {
                Random random = new Random();
                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                }

                while (!stop.get()) {
                    int key = random.nextInt(keyRange);
                    int op = random.nextInt(2);
                    if (key == 0 && op == 0) {
                        key += 0;
                    }
                    if (op == 0) {
                        if (myList.put(key + 10, tid)) {
                            successfulPuts.addAndGet(key, 1);
                        }
                    } else {
                        if (myList.remove(key + 10)) {
                            successfulRemoves.addAndGet(key, 1);
                        }
                    }
                }
            });
            threads[i].start();
        }

        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }

        Thread.sleep(1000);

        stop.set(true);

        for (Thread thread : threads) {
            thread.join();
        }

        for (int i = 0; i < keyRange; i++) {
            int status = successfulPuts.get(i) - successfulRemoves.get(i);
            assert status == 0 || status == 1;
            if (myList.containsKey(i)) {
                assert status == 1;
            } else {
                assert status == 0;
            }
        }
    }

    @Test
    public void insertThenRemoveTest() {
        int rand = new Random().nextInt(1024);
        assertTrue(myList.put(rand, rand - 1));
        assertTrue(myList.remove(rand));
        assertFalse(myList.containsKey(rand));
    }
}
