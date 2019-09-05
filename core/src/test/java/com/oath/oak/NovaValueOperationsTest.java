package com.oath.oak;

import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static com.oath.oak.NovaValueUtils.Result.*;
import static org.junit.Assert.*;

public class NovaValueOperationsTest {

    private MemoryManager memoryManager;
    private Slice s;
    private final NovaValueOperations operator = new NovaValueOperationsImpl();
    private final OakSerializer<Integer> oakSerializer = new OakSerializer<Integer>() {
        @Override
        public void serialize(Integer object, ByteBuffer targetBuffer) {
            targetBuffer.putInt(0, object);
        }

        @Override
        public Integer deserialize(ByteBuffer byteBuffer) {
            return null;
        }

        @Override
        public int calculateSize(Integer object) {
            return 0;
        }
    };

    @Before
    public void init() {
        memoryManager = new MemoryManager(new OakNativeMemoryAllocator(128));
        s = memoryManager.allocateSlice(operator.getHeaderSize() + 12);
        putInt(0, 0);
        putInt(operator.getLockLocation(), 0);
    }

    private void putInt(int index, int value) {
        s.getByteBuffer().putInt(s.getByteBuffer().position() + index, value);
    }

    private int getInt(int index) {
        return s.getByteBuffer().getInt(s.getByteBuffer().position() + index);
    }

    @Test
    public void transformTest() {
        putInt(operator.getHeaderSize(), 10);
        putInt(operator.getHeaderSize() + 4, 20);
        putInt(operator.getHeaderSize() + 8, 30);

        Map.Entry<NovaValueUtils.Result, Integer> result = operator.transform(s, byteBuffer -> byteBuffer.getInt(0) + byteBuffer.getInt(4) + byteBuffer.getInt(8), NovaValueUtils.NO_VERSION);
        assertEquals(TRUE, result.getKey());
        assertEquals(60, result.getValue().intValue());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void transformUpperBoundTest() {
        operator.transform(s, byteBuffer -> byteBuffer.getInt(12), NovaValueUtils.NO_VERSION);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void transformLowerBoundTest() {
        operator.transform(s, byteBuffer -> byteBuffer.getInt(-4), NovaValueUtils.NO_VERSION);
    }

    @Test(timeout = 5000)
    public void cannotTransformWriteLockedTest() throws InterruptedException {
        Random random = new Random();
        final int randomValue = random.nextInt();
        CyclicBarrier barrier = new CyclicBarrier(2);
        Thread transformer = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            Map.Entry<NovaValueOperations.Result, Integer> result = operator.transform(s, byteBuffer -> byteBuffer.getInt(4), NovaValueUtils.NO_VERSION);
            assertEquals(TRUE, result.getKey());
            assertEquals(randomValue, result.getValue().intValue());
        });
        assertEquals(TRUE, operator.lockWrite(s, 0));
        transformer.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        putInt(12, randomValue);
        operator.unlockWrite(s);
        transformer.join();
    }

    @Test
    public void multipleConcurrentTransformsTest() {
        putInt(operator.getHeaderSize(), 10);
        putInt(operator.getHeaderSize() + 4, 14);
        putInt(operator.getHeaderSize() + 8, 18);
        final int parties = 4;
        CyclicBarrier barrier = new CyclicBarrier(parties);
        Thread[] threads = new Thread[parties];
        for (int i = 0; i < parties; i++) {
            threads[i] = new Thread(() -> {
                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                }
                int index = new Random().nextInt(3) * 4;
                Map.Entry<NovaValueOperations.Result, Integer> result = operator.transform(s, byteBuffer -> byteBuffer.getInt(index), NovaValueUtils.NO_VERSION);
                assertEquals(TRUE, result.getKey());
                assertEquals(10 + index, result.getValue().intValue());
            });
            threads[i].start();
        }
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void cannotTransformDeletedTest() {
        operator.deleteValue(s, NovaValueUtils.NO_VERSION);
        Map.Entry<NovaValueOperations.Result, Integer> result = operator.transform(s, byteBuffer -> byteBuffer.getInt(0), NovaValueUtils.NO_VERSION);
        assertEquals(FALSE, result.getKey());
    }

    @Test
    public void putWithNoResizeTest() {
        Chunk.LookUp lookUp = new Chunk.LookUp(s, 0, -1);
        Random random = new Random();
        int[] randomValues = new int[3];
        for (int i = 0; i < randomValues.length; i++) {
            randomValues[i] = random.nextInt();
        }
        assertEquals(TRUE, operator.put(null, lookUp, 10, new OakSerializer<Integer>() {
            @Override
            public void serialize(Integer object, ByteBuffer targetBuffer) {
                for (int randomValue : randomValues) {
                    targetBuffer.putInt(randomValue);
                }
            }

            @Override
            public Integer deserialize(ByteBuffer byteBuffer) {
                return null;
            }

            @Override
            public int calculateSize(Integer object) {
                return 0;
            }
        }, memoryManager));
        assertEquals(randomValues[0], getInt(operator.getHeaderSize()));
        assertEquals(randomValues[1], getInt(operator.getHeaderSize() + 4));
        assertEquals(randomValues[2], getInt(operator.getHeaderSize() + 8));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void putUpperBoundTest() {
        Chunk.LookUp lookUp = new Chunk.LookUp(s, 0, 0);
        operator.put(null, lookUp, 5, new OakSerializer<Integer>() {
            @Override
            public void serialize(Integer object, ByteBuffer targetBuffer) {
                targetBuffer.putInt(12, 30);
            }

            @Override
            public Integer deserialize(ByteBuffer byteBuffer) {
                return null;
            }

            @Override
            public int calculateSize(Integer object) {
                return 0;
            }
        }, memoryManager);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void putLowerBoundTest() {
        Chunk.LookUp lookUp = new Chunk.LookUp(s, 0, 0);
        operator.put(null, lookUp, 5, new OakSerializer<Integer>() {
            @Override
            public void serialize(Integer object, ByteBuffer targetBuffer) {
                targetBuffer.putInt(-4, 30);
            }

            @Override
            public Integer deserialize(ByteBuffer byteBuffer) {
                return null;
            }

            @Override
            public int calculateSize(Integer object) {
                return 0;
            }
        }, memoryManager);
    }

    @Test
    public void cannotPutReadLockedTest() throws InterruptedException {
        Chunk.LookUp lookUp = new Chunk.LookUp(s, 0, 0);
        CyclicBarrier barrier = new CyclicBarrier(2);
        Random random = new Random();
        int[] randomValues = new int[3];
        for (int i = 0; i < randomValues.length; i++) {
            randomValues[i] = random.nextInt();
        }
        Thread putter = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            operator.put(null, lookUp, 10, new OakSerializer<Integer>() {
                @Override
                public void serialize(Integer object, ByteBuffer targetBuffer) {
                    for (int randomValue : randomValues) {
                        targetBuffer.putInt(randomValue);
                    }
                }

                @Override
                public Integer deserialize(ByteBuffer byteBuffer) {
                    return null;
                }

                @Override
                public int calculateSize(Integer object) {
                    return 0;
                }
            }, memoryManager);
        });
        operator.lockRead(s, NovaValueUtils.NO_VERSION);
        putter.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        int a = getInt(operator.getHeaderSize()), b = getInt(operator.getHeaderSize() + 4), c = getInt(operator.getHeaderSize() + 8);
        operator.unlockRead(s, NovaValueUtils.NO_VERSION);
        putter.join();
        assertNotEquals(randomValues[0], a);
        assertNotEquals(randomValues[1], b);
        assertNotEquals(randomValues[2], c);
    }

    @Test
    public void cannotPutWriteLockedTest() throws InterruptedException {
        Chunk.LookUp lookUp = new Chunk.LookUp(s, 0, 0);
        CyclicBarrier barrier = new CyclicBarrier(2);
        Random random = new Random();
        int[] randomValues = new int[3];
        for (int i = 0; i < randomValues.length; i++) {
            randomValues[i] = random.nextInt();
        }
        putInt(operator.getHeaderSize(), randomValues[0] - 1);
        putInt(operator.getHeaderSize() + 4, randomValues[1] - 1);
        putInt(operator.getHeaderSize() + 8, randomValues[2] - 1);
        Thread putter = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            operator.put(null, lookUp, 10, new OakSerializer<Integer>() {
                @Override
                public void serialize(Integer object, ByteBuffer targetBuffer) {
                    for (int i = 0; i < targetBuffer.remaining(); i += 4) {
                        assertEquals(randomValues[i / 4], targetBuffer.getInt(i));
                    }
                }

                @Override
                public Integer deserialize(ByteBuffer byteBuffer) {
                    return null;
                }

                @Override
                public int calculateSize(Integer object) {
                    return 0;
                }
            }, memoryManager);
        });
        operator.lockWrite(s, NovaValueUtils.NO_VERSION);
        putter.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        putInt(operator.getHeaderSize(), randomValues[0]);
        putInt(operator.getHeaderSize() + 4, randomValues[1]);
        putInt(operator.getHeaderSize() + 8, randomValues[2]);
        operator.unlockWrite(s);
        putter.join();
    }

    @Test
    public void cannotPutInDeletedValueTest() {
        operator.deleteValue(s, NovaValueUtils.NO_VERSION);
        Chunk.LookUp lookUp = new Chunk.LookUp(s, 0, -1);
        assertEquals(FALSE, operator.put(null, lookUp, null, null, memoryManager));
    }

    @Test
    public void computeTest() {
        Random random = new Random();
        int sum = 0;
        for (int i = 4; i < 12; i += 4) {
            int tmp = random.nextInt(128);
            putInt(operator.getHeaderSize() + i, tmp);
            sum += tmp;
        }
        assertEquals(TRUE, operator.compute(s, oakWBuffer -> {
            oakWBuffer.putInt(0, oakWBuffer.getInt(4) + oakWBuffer.getInt(8));
        }, NovaValueUtils.NO_VERSION));
        assertEquals(sum, getInt(operator.getHeaderSize()));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void computeUpperBoundTest() {
        operator.compute(s, oakWBuffer -> {
            oakWBuffer.getInt(12);
        }, NovaValueUtils.NO_VERSION);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void computeLowerBoundTest() {
        operator.compute(s, oakWBuffer -> {
            oakWBuffer.getInt(-4);
        }, NovaValueUtils.NO_VERSION);
    }

    @Test
    public void cannotComputeReadLockedTest() throws InterruptedException {
        CyclicBarrier barrier = new CyclicBarrier(2);
        int randomValue = new Random().nextInt(128);
        Thread computer = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            operator.compute(s, oakWBuffer -> {
                oakWBuffer.putInt(0, oakWBuffer.getInt(0) * 2);
            }, NovaValueUtils.NO_VERSION);
        });
        operator.lockRead(s, NovaValueUtils.NO_VERSION);
        computer.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        int a = getInt(operator.getHeaderSize());
        operator.unlockRead(s, NovaValueUtils.NO_VERSION);
        computer.join();
        assertNotEquals(randomValue, a);
    }

    @Test
    public void cannotComputeWriteLockedTest() throws InterruptedException {
        CyclicBarrier barrier = new CyclicBarrier(2);
        int randomValue = new Random().nextInt(128);
        putInt(operator.getHeaderSize(), 200);
        Thread computer = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            operator.compute(s, oakWBuffer -> {
                oakWBuffer.putInt(0, oakWBuffer.getInt(0) * 2);
            }, NovaValueUtils.NO_VERSION);
        });
        operator.lockWrite(s, NovaValueUtils.NO_VERSION);
        computer.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        putInt(operator.getHeaderSize(), randomValue);
        operator.unlockWrite(s);
        computer.join();
        assertEquals(randomValue * 2, getInt(operator.getHeaderSize()));
    }

    @Test
    public void cannotComputeDeletedValueTest() {
        operator.deleteValue(s, NovaValueUtils.NO_VERSION);
        assertEquals(FALSE, operator.compute(s, oakWBuffer -> {
        }, 0));
    }

    @Test
    public void removeTest() {
        assertEquals(TRUE, operator.remove(s, memoryManager, NovaValueUtils.NO_VERSION));
        assertEquals(TRUE, operator.isValueDeleted(s, NovaValueUtils.NO_VERSION));
    }

    @Test
    public void cannotRemoveReadLockedTest() throws InterruptedException {
        CyclicBarrier barrier = new CyclicBarrier(2);
        Thread remover = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            assertEquals(TRUE, operator.remove(s, memoryManager, NovaValueUtils.NO_VERSION));
        });
        operator.lockRead(s, NovaValueUtils.NO_VERSION);
        remover.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        NovaValueUtils.Result result = operator.isValueDeleted(s, NovaValueUtils.NO_VERSION);
        operator.unlockRead(s, NovaValueUtils.NO_VERSION);
        remover.join();
        assertEquals(FALSE, result);
    }

    @Test
    public void cannotRemoveWriteLockedTest() throws InterruptedException {
        CyclicBarrier barrier = new CyclicBarrier(2);
        Thread remover = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            assertEquals(TRUE, operator.remove(s, memoryManager, NovaValueUtils.NO_VERSION));
        });
        operator.lockWrite(s, NovaValueUtils.NO_VERSION);
        remover.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        NovaValueUtils.Result result = operator.isValueDeleted(s, NovaValueUtils.NO_VERSION);
        operator.unlockWrite(s);
        remover.join();
        assertEquals(FALSE, result);
    }

    @Test
    public void cannotRemoveDeletedValueTest() {
        operator.deleteValue(s, NovaValueUtils.NO_VERSION);
        assertEquals(FALSE, operator.remove(s, memoryManager, NovaValueUtils.NO_VERSION));
    }

    @Test
    public void reuseSliceWithNoHeaderTest() {
        int randomValue = new Random().nextInt();
        putInt(operator.getHeaderSize() + 4, randomValue);
        operator.remove(s, memoryManager, NovaValueUtils.NO_VERSION);
        Slice newSlice = memoryManager.allocateSlice(operator.getHeaderSize() + 8);
        assertEquals(randomValue, newSlice.getByteBuffer().getInt(operator.getHeaderSize() + newSlice.getByteBuffer().position()));
    }

    @Test
    public void cannotReuseSliceTest() {
        int randomValue = new Random().nextInt();
        putInt(operator.getHeaderSize() + 4, randomValue);
        operator.remove(s, memoryManager, NovaValueUtils.NO_VERSION);
        Slice newSlice = memoryManager.allocateSlice(operator.getHeaderSize() + 12);
        assertNotEquals(randomValue, newSlice.getByteBuffer().getInt(operator.getHeaderSize() + newSlice.getByteBuffer().position()));
    }

    @Test
    public void exchangeTest() {
        assertEquals(TRUE, operator.put(null, new Chunk.LookUp(s, -1, -1), "123", new StringSerializer(), memoryManager));
        Map.Entry<NovaValueUtils.Result, String> readResult = operator.exchange(null, new Chunk.LookUp(s, -1, -1), "456", bb -> new StringSerializer().deserialize(bb), new StringSerializer(), memoryManager);
        assertEquals(TRUE, readResult.getKey());
        assertEquals("123", readResult.getValue());
        assertEquals("456", operator.transform(s, bb -> new StringSerializer().deserialize(bb), NovaValueUtils.NO_VERSION).getValue());
    }

    @Test
    public void cannotExchangeReadLockedTest() throws InterruptedException {
        assertEquals(TRUE, operator.put(null, new Chunk.LookUp(s, -1, -1), "123", new StringSerializer(), memoryManager));
        CyclicBarrier barrier = new CyclicBarrier(2);
        Thread exchanger = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            assertEquals(TRUE, operator.exchange(null, new Chunk.LookUp(s, -1, -1), "456", bb -> new StringSerializer().deserialize(bb), new StringSerializer(), memoryManager).getKey());
        });
        operator.lockRead(s, NovaValueUtils.NO_VERSION);
        exchanger.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        Map.Entry<NovaValueUtils.Result, String> readResult = operator.transform(s, bb -> new StringSerializer().deserialize(bb), NovaValueUtils.NO_VERSION);
        operator.unlockRead(s, NovaValueUtils.NO_VERSION);
        exchanger.join();
        assertEquals(TRUE, readResult.getKey());
        assertEquals("123", readResult.getValue());
    }

    @Test
    public void cannotExchangeWriteLockedTest() throws InterruptedException {
        assertEquals(TRUE, operator.put(null, new Chunk.LookUp(s, -1, -1), "123", new StringSerializer(), memoryManager));
        CyclicBarrier barrier = new CyclicBarrier(2);
        Thread exchanger = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            Map.Entry<NovaValueUtils.Result, String> readResult = operator.exchange(null, new Chunk.LookUp(s, -1, -1), "456", bb -> new StringSerializer().deserialize(bb), new StringSerializer(), memoryManager);
            assertEquals("789", readResult.getValue());
        });
        operator.lockWrite(s, NovaValueUtils.NO_VERSION);
        exchanger.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        new StringSerializer().serialize("789", operator.getActualValue(s));
        operator.unlockWrite(s);
        exchanger.join();
    }

    @Test
    public void cannotExchangeDeletedValueTest() {
        assertEquals(TRUE, operator.deleteValue(s, NovaValueUtils.NO_VERSION));
        assertEquals(FALSE, operator.exchange(null, new Chunk.LookUp(s, -1, -1), null, null, null, memoryManager).getKey());
    }

    @Test
    public void exchangeIsConsensusTest() throws InterruptedException {
        int numOfThreads = 16;
        putInt(operator.getHeaderSize(), 1);
        Thread[] threads = new Thread[numOfThreads];
        int[] consensusArray = new int[numOfThreads];
        CyclicBarrier barrier = new CyclicBarrier(numOfThreads);
        AtomicInteger consensusWinner = new AtomicInteger(-1);
        for (int i = 0; i < numOfThreads; i++) {
            int id = i + 1;
            consensusArray[i] = 0;
            threads[i] = new Thread(() -> {
                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                }
                int result = operator.exchange(null, new Chunk.LookUp(s, -1, -1), id,
                        byteBuffer -> byteBuffer.getInt(0), oakSerializer, memoryManager).getValue();
                consensusArray[id - 1] = result;
                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                }
                int winner;
                for (int j = 0; j < numOfThreads; j++) {
                    if (consensusArray[j] != 1) continue;
                    winner = consensusArray[j];
                    if (consensusWinner.get() == -1) {
                        consensusWinner.compareAndSet(-1, winner);
                    }
                    if (consensusWinner.get() != winner) fail();
                }
            });
            threads[i].start();
        }
        for (int i = 0; i < numOfThreads; i++) {
            threads[i].join();
        }
    }

    @Test
    public void successfulCASTest() {
        putInt(operator.getHeaderSize(), 0);
        assertEquals(TRUE, operator.compareExchange(null, new Chunk.LookUp(s, -1, -1), 0, 1, byteBuffer -> byteBuffer.getInt(0), oakSerializer, memoryManager));
        assertEquals(1, getInt(operator.getHeaderSize()));
    }

    @Test
    public void failedCASTest() {
        putInt(operator.getHeaderSize(), 2);
        assertEquals(FALSE, operator.compareExchange(null, new Chunk.LookUp(s, -1, -1), 0, 1, byteBuffer -> byteBuffer.getInt(0), oakSerializer, memoryManager));
        assertEquals(2, getInt(operator.getHeaderSize()));
    }

    @Test
    public void cannotCASReadLockedTest() throws InterruptedException {
        int initValue = new Random().nextInt();
        putInt(operator.getHeaderSize(), initValue);
        CyclicBarrier barrier = new CyclicBarrier(2);
        Thread casser = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            assertEquals(TRUE, operator.compareExchange(null, new Chunk.LookUp(s, -1, -1), initValue, initValue + 1, byteBuffer -> byteBuffer.getInt(0), oakSerializer, memoryManager));
        });
        operator.lockRead(s, NovaValueUtils.NO_VERSION);
        casser.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        int readValue = getInt(operator.getHeaderSize());
        operator.unlockRead(s, NovaValueUtils.NO_VERSION);
        casser.join();
        assertEquals(initValue, readValue);
    }

    @Test
    public void cannotCASWriteLockedTest() throws InterruptedException {
        putInt(operator.getHeaderSize(), 0);
        CyclicBarrier barrier = new CyclicBarrier(2);
        Thread casser = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            NovaValueUtils.Result result = operator.compareExchange(null, new Chunk.LookUp(s, -1, -1), 1, 2, byteBuffer -> byteBuffer.getInt(0), oakSerializer, memoryManager);
            assertEquals(TRUE, result);
        });
        operator.lockWrite(s, NovaValueUtils.NO_VERSION);
        casser.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        Thread.sleep(2000);
        putInt(operator.getHeaderSize(), 1);
        operator.unlockWrite(s);
        casser.join();
        assertEquals(2, getInt(operator.getHeaderSize()));
    }

    @Test
    public void cannotCASDeletedValueTest() {
        putInt(operator.getHeaderSize(), 0);
        operator.deleteValue(s, NovaValueUtils.NO_VERSION);
        assertEquals(FALSE, operator.compareExchange(null, new Chunk.LookUp(s, -1, -1), 0, 0, byteBuffer -> byteBuffer.getInt(0), oakSerializer, memoryManager));
    }

    @Test
    public void casIsConsensusTest() throws InterruptedException {
        int numOfThreads = 16;
        putInt(operator.getHeaderSize(), 0);
        Thread[] threads = new Thread[numOfThreads];
        CyclicBarrier barrier = new CyclicBarrier(numOfThreads);
        AtomicInteger consensusWinner = new AtomicInteger(-1);
        for (int i = 0; i < numOfThreads; i++) {
            int id = i + 1;
            threads[i] = new Thread(() -> {
                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                }
                NovaValueUtils.Result result = operator.compareExchange(null, new Chunk.LookUp(s, -1, -1), 0, id,
                        byteBuffer -> byteBuffer.getInt(0), oakSerializer, memoryManager);
                assertNotEquals(result, RETRY);
                if (result == TRUE)
                    assertTrue(consensusWinner.compareAndSet(-1, id));
                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                }
                assertEquals(consensusWinner.get(), getInt(operator.getHeaderSize()));
            });
            threads[i].start();
        }
        for (int i = 0; i < numOfThreads; i++) {
            threads[i].join();
        }
    }
}
