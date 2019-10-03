package com.oath.oak.MemoryManagment;

import com.oath.oak.MemoryManagment.ExampleList.LinkedList;
import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;
import com.oath.oak.OakComparator;
import com.oath.oak.OakSerializer;
import com.oath.oak.ThreadIndexCalculator;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class LinkedListTest {

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


    @Test
    public void sanityCheck() throws InterruptedException {
        int numOfThreads = 1; //ThreadIndexCalculator.MAX_THREADS - 1;
        int keyRange = 128;
        AtomicIntegerArray successfulPuts = new AtomicIntegerArray(numOfThreads);
        AtomicIntegerArray successfulRemoves = new AtomicIntegerArray(numOfThreads);
        LinkedList<Integer, Integer> myList = new LinkedList<>(new NovaManagerImpl(new OakNativeMemoryAllocator(128),
                new NovaValueUtilitiesImpl()), integerSerializer, Integer.MIN_VALUE, Integer.MAX_VALUE,
                integerSerializer, integerComparator);
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
                    if (op == 0) {
                        if (myList.putIfAbsent(key, tid) == null) {
                            successfulPuts.addAndGet(tid, 1);
                        }
                    } else {
                        if (myList.remove(key) != null) {
                            successfulRemoves.addAndGet(tid, 1);
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
            if (myList.get(i) != null) {
                assert status == 1;
            } else {
                assert status == 0;
            }
        }
    }
}
