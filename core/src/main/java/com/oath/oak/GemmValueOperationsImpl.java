package com.oath.oak;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.oath.oak.Chunk.VALUE_BLOCK_SHIFT;
import static com.oath.oak.Chunk.VALUE_LENGTH_MASK;
import static com.oath.oak.GemmAllocator.GEMM_HEADER_SIZE;
import static com.oath.oak.GemmValueOperationsImpl.LockStates.*;
import static com.oath.oak.GemmValueUtils.Result.TRUE;
import static com.oath.oak.UnsafeUtils.intsToLong;
import static java.lang.Long.reverseBytes;

public class GemmValueOperationsImpl implements GemmValueOperations {
    enum LockStates {
        FREE(0), LOCKED(1), DELETED(2), MOVED(3);

        public final int value;

        LockStates(int value) {
            this.value = value;
        }
    }

    private static final int LOCK_MASK = 0x3;
    private static final int LOCK_SHIFT = 2;
    private static final int VALUE_HEADER_SIZE = 4;
    private static final GemmValueOperationsImpl instance = new GemmValueOperationsImpl();

    private static Unsafe unsafe = UnsafeUtils.unsafe;

    private boolean CAS(Slice s, int expectedLock, int newLock, int generation) {
        long expected = intsToLong(expectedLock, generation);
        long value = intsToLong(newLock, generation);
        return unsafe.compareAndSwapLong(null, ((DirectBuffer) s.getByteBuffer()).address(), reverseBytes(expected), reverseBytes(value));
    }

    @Override
    public void unsafeBufferToIntArrayCopy(ByteBuffer bb, int srcPosition, int[] dstArray, int countInts) {
        UnsafeUtils.unsafeCopyBufferToIntArray(bb, srcPosition, dstArray, countInts);
    }

    @Override
    public <T> AbstractMap.SimpleEntry<Result, T> transform(Slice s, Function<ByteBuffer, T> transformer, int generation) {
        Result result = lockRead(s, generation);
        if (result != TRUE) return new AbstractMap.SimpleEntry<>(result, null);

        T transformation = transformer.apply(getActualValue(s).asReadOnlyBuffer());
        unlockRead(s, generation);
        return new AbstractMap.SimpleEntry<>(TRUE, transformation);
    }

    @Override
    public <K, V> Result put(Chunk<K, V> chunk, Chunk.LookUp lookUp, V newVal, int generation, OakSerializer<V> serializer, GemmAllocator memoryManager) {
        Slice s = lookUp.valueSlice;
        Result result = lockWrite(s, generation);
        if (result != TRUE) return result;
        int capacity = serializer.calculateSize(newVal);
        if (capacity + getHeaderSize() < s.getByteBuffer().remaining()) {
            s = moveValue(chunk, lookUp, generation, capacity, memoryManager);
        }
        ByteBuffer bb = getActualValue(s);
        serializer.serialize(newVal, bb);
        unlockWrite(s);
        return TRUE;
    }

    private <K, V> Slice moveValue(Chunk<K, V> chunk, Chunk.LookUp lookUp, int generation, int capacity, GemmAllocator memoryManager) {
        Slice s = lookUp.valueSlice;
        putInt(s, GEMM_HEADER_SIZE, MOVED.value);
        memoryManager.releaseSlice(s);
        s = memoryManager.allocateSlice(capacity + getHeaderSize());
        putInt(s, 0, generation);
        putInt(s, GEMM_HEADER_SIZE, LOCKED.value);
        int valueBlockAndLength = (s.getBlockID() << VALUE_BLOCK_SHIFT) | ((capacity + VALUE_HEADER_SIZE) & VALUE_LENGTH_MASK);
        assert chunk.longCasEntriesArray(lookUp.entryIndex, Chunk.OFFSET.VALUE_STATS, lookUp.valueStats, UnsafeUtils.intsToLong(valueBlockAndLength, s.getByteBuffer().position()));
        return s;
    }

    @Override
    public Result compute(Slice s, int generation, Consumer<OakWBuffer> computer) {
        Result result = lockWrite(s, generation);
        if (result != TRUE) return result;
        computer.accept(new OakWBufferImpl(s.getByteBuffer(), instance));
        unlockWrite(s);
        return TRUE;
    }

    @Override
    public Result remove(Slice s, int generation, GemmAllocator memoryManager) {
        Result result = deleteValue(s, generation, memoryManager);
        if (result != TRUE) return result;
        // releasing the actual value and not the header
        memoryManager.releaseSlice(s);
        return TRUE;
    }

    @Override
    public int getHeaderSize() {
        return VALUE_HEADER_SIZE + GEMM_HEADER_SIZE;
    }

    @Override
    public ByteBuffer getActualValueThreadSafe(Slice s) {
        ByteBuffer bb = s.getByteBuffer();
        bb.position(bb.position() + getHeaderSize());
        ByteBuffer dup = bb.slice();
        bb.position(bb.position() - getHeaderSize());
        return dup;
    }

    @Override
    public ByteBuffer getActualValue(Slice s) {
        ByteBuffer dup = s.getByteBuffer().duplicate();
        dup.position(dup.position() + getHeaderSize());
        return dup.slice();
    }

    @Override
    public Result lockRead(Slice s, int generation) {
        int lockState;
        do {
            int oldGeneration = getInt(s, 0);
            if (oldGeneration != generation) return Result.RETRY;
            lockState = getInt(s, GEMM_HEADER_SIZE);
            lockState &= ~LOCK_MASK;
            if (oldGeneration != getInt(s, 0)) return Result.RETRY;
            if (!isValueThere(lockState)) return Result.FALSE;
        } while (!CAS(s, lockState, lockState + (1 << LOCK_SHIFT), generation));
        return TRUE;
    }

    @Override
    public Result unlockRead(Slice s, int generation) {
        int lockState;
        do {
            lockState = getInt(s, GEMM_HEADER_SIZE);
            lockState &= ~LOCK_MASK;
        } while (!CAS(s, lockState, lockState - (1 << LOCK_SHIFT), generation));
        return TRUE;
    }

    @Override
    public Result lockWrite(Slice s, int generation) {
        do {
            int oldGeneration = getInt(s, 0);
            if (oldGeneration != generation) return Result.RETRY;
            int lockState = getInt(s, GEMM_HEADER_SIZE);
            if (oldGeneration != getInt(s, 0)) return Result.RETRY;
            if (!isValueThere(lockState)) return Result.FALSE;
        } while (!CAS(s, FREE.value, LOCKED.value, generation));
        return TRUE;
    }

    @Override
    public Result unlockWrite(Slice s) {
        putInt(s, GEMM_HEADER_SIZE, FREE.value);
        return TRUE;
    }

    @Override
    public Result deleteValue(Slice s, int generation, GemmAllocator gemmAllocator) {
        do {
            int oldGeneration = getInt(s, 0);
            if (oldGeneration != generation) return Result.RETRY;
            int lockState = getInt(s, GEMM_HEADER_SIZE);
            if (oldGeneration != getInt(s, 0)) return Result.RETRY;
            if (!isValueThere(lockState)) return Result.FALSE;
        } while (!CAS(s, FREE.value, DELETED.value, generation));
        return TRUE;
    }

    private boolean isValueThere(int lockState) {
        return lockState != DELETED.value && lockState != MOVED.value;
    }

    @Override
    public Result isValueDeleted(Slice s, int generation) {
        int oldGeneration = getInt(s, 0);
        if (oldGeneration != generation) return Result.RETRY;
        int lockState = getInt(s, GEMM_HEADER_SIZE);
        if (oldGeneration != getInt(s, 0)) return Result.RETRY;
        if (lockState == MOVED.value) return Result.RETRY;
        if (lockState == DELETED.value) return TRUE;
        return Result.FALSE;
    }

    private int getInt(Slice s, int index) {
        return s.getByteBuffer().getInt(s.getByteBuffer().position() + index);
    }

    private void putInt(Slice s, int index, int value) {
        s.getByteBuffer().putInt(s.getByteBuffer().position() + index, value);
    }
}
