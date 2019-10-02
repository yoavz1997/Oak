package com.oath.oak.MemoryManagment;

import com.oath.oak.MemoryManagment.ZC.ZcOffHeapSlice;
import com.oath.oak.OakSerializer;
import com.oath.oak.Slice;
import com.oath.oak.UnsafeUtils;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Map;
import java.util.function.Function;

import static com.oath.oak.MemoryManagment.Result.*;

public class OffHeapSliceImpl implements OffHeapSlice {
    private long reference;
    protected long version;
    final NovaValueUtilities utilities;
    private final NovaManager novaManager;

    private static final int LENGTH_MASK = 0x7fffff;
    private static final int BLOCK_ID_SHIFT = 23;

    OffHeapSliceImpl(long reference, long version, NovaValueUtilities utilities, NovaManager novaManager) {
        this.reference = reference;
        this.version = version;
        this.utilities = utilities;
        this.novaManager = novaManager;
    }

    OffHeapSliceImpl(int blockId, int position, int length, long version, NovaValueUtilities utilities,
                     NovaManager manager) {
        this(createReference(blockId, position, length), version, utilities, manager);
    }

    private int getBlockId() {
        return UnsafeUtils.longToInts(reference)[0] >>> BLOCK_ID_SHIFT;
    }

    ByteBuffer createMyByteBuffer() {
        int[] ints = UnsafeUtils.longToInts(reference);
        return novaManager.getByteBufferFromBlockID(ints[0] >>> BLOCK_ID_SHIFT, ints[1], ints[0] & LENGTH_MASK);
    }

    private static long createReference(int blockID, int position, int length) {
        return UnsafeUtils.intsToLong((blockID << BLOCK_ID_SHIFT) + (length & LENGTH_MASK), position);
    }

    @Override
    public Slice intoSlice() {
        return new Slice(getBlockId(), createMyByteBuffer());
    }

    @Override
    public <V> Result put(V newValue, OakSerializer<V> serializer) {
        ByteBuffer bb = createMyByteBuffer();
        Result result = utilities.lockWrite(bb, version);
        if (result != TRUE) {
            return result;
        }
        try {
            writeValue(bb, newValue, serializer);
            return TRUE;
        } finally {
            utilities.unlockWrite(bb);
        }
    }

    <V> void writeValue(ByteBuffer bb, V newValue, OakSerializer<V> serializer) {
        int newLength = serializer.calculateSize(newValue) + utilities.getHeaderSize();
        if (newLength > bb.remaining()) {
            throw new UnsupportedOperationException();
        }
        serializer.serialize(newValue, utilities.getActualBuffer(bb));
    }

    @Override
    public <V> Map.Entry<Result, V> get(OakSerializer<V> serializer) {
        ByteBuffer bb = createMyByteBuffer();
        Result result = utilities.lockRead(bb, version);
        if (result != TRUE) {
            return new AbstractMap.SimpleImmutableEntry<>(result, null);
        }
        try {
            return new AbstractMap.SimpleImmutableEntry<>(TRUE, readValue(bb, serializer));
        } finally {
            utilities.unlockRead(bb);
        }
    }

    private <V> V readValue(ByteBuffer bb, OakSerializer<V> serializer) {
        return serializer.deserialize(utilities.getActualReadOnlyBuffer(bb));
    }

    @Override
    public <V> Map.Entry<Result, V> exchange(V newValue, OakSerializer<V> serializer) {
        ByteBuffer bb = createMyByteBuffer();
        Result result = utilities.lockWrite(bb, version);
        if (result != TRUE) {
            return new AbstractMap.SimpleImmutableEntry<>(result, null);
        }
        try {
            V oldValue = readValue(bb, serializer);
            writeValue(bb, newValue, serializer);
            return new AbstractMap.SimpleImmutableEntry<>(TRUE, oldValue);
        } finally {
            utilities.unlockWrite(bb);
        }
    }

    @Override
    public <V> Result compareAndExchange(V expected, V newValue, OakSerializer<V> serializer,
                                         Comparator<V> comparator) {
        ByteBuffer bb = createMyByteBuffer();
        Result result = utilities.lockWrite(bb, version);
        if (result != TRUE) {
            return result;
        }
        try {
            V oldValue = readValue(bb, serializer);
            if (comparator.compare(oldValue, expected) != 0) {
                return FALSE;
            }
            writeValue(bb, newValue, serializer);
            return TRUE;
        } finally {
            utilities.unlockWrite(bb);
        }
    }

    @Override
    public <V> Map.Entry<Result, V> delete(OakSerializer<V> serializer) {
        ByteBuffer bb = createMyByteBuffer();
        Result result = utilities.deleteValue(bb, version);
        if (result != TRUE) {
            return new AbstractMap.SimpleImmutableEntry<>(result, null);
        }
        V oldValue = serializer == null ? null : readValue(bb, serializer);
        novaManager.freeSlice(this);
        return new AbstractMap.SimpleImmutableEntry<>(TRUE, oldValue);
    }

    @Override
    public <V> Result compute(Function<V, V> computer, OakSerializer<V> serializer) {
        ByteBuffer bb = createMyByteBuffer();
        Result result = utilities.lockWrite(bb, version);
        if (result != TRUE) {
            return result;
        }
        try {
            V oldValue = readValue(bb, serializer);
            V newValue = computer.apply(oldValue);
            writeValue(bb, newValue, serializer);
            return TRUE;
        } finally {
            utilities.unlockWrite(bb);
        }
    }

    @Override
    public ZcOffHeapSlice zc() {
        return new ZcOffHeapSliceImpl(this.reference, this.version, this.utilities, this.novaManager);
    }
}
