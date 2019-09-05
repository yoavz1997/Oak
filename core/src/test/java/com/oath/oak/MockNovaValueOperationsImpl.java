package com.oath.oak;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.oath.oak.NovaValueUtils.Result.*;

public class MockNovaValueOperationsImpl implements NovaValueOperations {
    private NovaValueUtils mock;

    MockNovaValueOperationsImpl(NovaValueUtils mock) {
        this.mock = mock;
    }

    private static final int VALUE_HEADER_SIZE = 4;

    @Override
    public void unsafeBufferToIntArrayCopy(ByteBuffer bb, int srcPosition, int[] dstArray, int countInts) {
        UnsafeUtils.unsafeCopyBufferToIntArray(bb, srcPosition, dstArray, countInts);
    }

    @Override
    public <T> AbstractMap.SimpleEntry<Result, T> transform(Slice s, Function<ByteBuffer, T> transformer, int version) {
        Result result = lockRead(s, version);
        if (result != TRUE) return new AbstractMap.SimpleEntry<>(result, null);

        T transformation = transformer.apply(getActualValue(s).asReadOnlyBuffer());
        unlockRead(s, version);
        return new AbstractMap.SimpleEntry<>(TRUE, transformation);
    }

    @Override
    public <K, V> Result put(Chunk<K, V> chunk, Chunk.LookUp lookUp, V newVal, OakSerializer<V> serializer, MemoryManager memoryManager) {
        Result result = lockWrite(lookUp.valueSlice, NO_VERSION);
        if (result != TRUE) return result;
        Slice s = innerPut(chunk, lookUp, newVal, serializer, memoryManager);
        if (s == null) {
            // rebalancing in progress
            unlockWrite(lookUp.valueSlice);
            return RETRY;
        }
        unlockWrite(s);
        return TRUE;
    }

    private <K, V> Slice innerPut(Chunk<K, V> chunk, Chunk.LookUp lookUp, V newVal, OakSerializer<V> serializer, MemoryManager memoryManager) {
        Slice s = lookUp.valueSlice;
        int capacity = serializer.calculateSize(newVal);
        if (capacity + getHeaderSize() > s.getByteBuffer().remaining()) {
            throw new UnsupportedOperationException();
        }
        ByteBuffer bb = getActualValue(s);
        serializer.serialize(newVal, bb);
        return s;
    }

    @Override
    public Result compute(Slice s, Consumer<OakWBuffer> computer, int version) {
        Result result = lockWrite(s, version);
        if (result != TRUE) return result;
        computer.accept(new OakWBufferImpl(s.getByteBuffer(), this));
        unlockWrite(s);
        return TRUE;
    }

    @Override
    public Result remove(Slice s, MemoryManager memoryManager, int version) {
        Result result = deleteValue(s, version);
        if (result != TRUE) return result;
        releaseSlice(s, memoryManager);
        return TRUE;
    }

    @Override
    public <K, V> AbstractMap.SimpleEntry<Result, V> exchange(Chunk<K, V> chunk, Chunk.LookUp lookUp, V value, Function<ByteBuffer, V> valueDeserializeTransformer, OakSerializer<V> serializer, MemoryManager memoryManager) {
        Result result = lockWrite(lookUp.valueSlice, NO_VERSION);
        if (result != TRUE) return new AbstractMap.SimpleEntry<>(result, null);
        V oldValue = valueDeserializeTransformer.apply(getActualValue(lookUp.valueSlice));
        Slice s = innerPut(chunk, lookUp, value, serializer, memoryManager);
        unlockWrite(s);
        return new AbstractMap.SimpleEntry<>(TRUE, oldValue);
    }

    @Override
    public <K, V> Result compareExchange(Chunk<K, V> chunk, Chunk.LookUp lookUp, V expected, V value, Function<ByteBuffer, V> valueDeserializeTransformer, OakSerializer<V> serializer, MemoryManager memoryManager) {
        Result result = lockWrite(lookUp.valueSlice, NO_VERSION);
        if (result != TRUE) return result;
        V oldValue = valueDeserializeTransformer.apply(getActualValue(lookUp.valueSlice));
        if (!oldValue.equals(expected)) {
            unlockWrite(lookUp.valueSlice);
            return FALSE;
        }
        Slice s = innerPut(chunk, lookUp, value, serializer, memoryManager);
        unlockWrite(s);
        return TRUE;
    }

    @Override
    public void releaseSlice(Slice slice, MemoryManager memoryManager) {
        // In this case, we do not free the header, because we do not employ NOVA yet
        Slice dup = slice.duplicate();
        dup.getByteBuffer().position(dup.getByteBuffer().position() + getHeaderSize());
        memoryManager.releaseSlice(dup);
    }

    @Override
    public int getHeaderSize() {
        return getLockSize() + getLockLocation();
    }

    @Override
    public int getLockLocation() {
        return 0;
    }

    @Override
    public int getLockSize() {
        return VALUE_HEADER_SIZE;
    }

    @Override
    public Result lockRead(Slice s, int version) {
        return mock.lockRead(s, version);
    }

    @Override
    public Result unlockRead(Slice s, int version) {
        return mock.unlockRead(s, version);
    }

    @Override
    public Result lockWrite(Slice s, int version) {
        return mock.lockWrite(s, version);
    }

    @Override
    public Result unlockWrite(Slice s) {
        return mock.unlockWrite(s);
    }

    @Override
    public Result deleteValue(Slice s, int version) {
        return mock.deleteValue(s, version);
    }

    @Override
    public Result isValueDeleted(Slice s, int version) {
        return mock.isValueDeleted(s, version);
    }
}
