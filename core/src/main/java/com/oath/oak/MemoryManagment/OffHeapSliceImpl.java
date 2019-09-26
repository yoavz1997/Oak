package com.oath.oak.MemoryManagment;

import com.oath.oak.MemoryManagment.ZC.ZcOffHeapSlice;
import com.oath.oak.MemoryManagment.ZC.ZcOffHeapSliceImpl;
import com.oath.oak.OakSerializer;
import com.oath.oak.UnsafeUtils;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Map;
import java.util.function.Function;

import static com.oath.oak.MemoryManagment.Result.*;

public class OffHeapSliceImpl implements OffHeapSlice {
    protected long reference;
    protected long version;
    protected final NovaValueUtilities utilities;
    protected final NovaManager novaManager;

    private static final int LENGTH_MASK = 0x7fffff;
    private static final int BLOCK_ID_SHIFT = 23;

    protected OffHeapSliceImpl(long reference, long version, NovaValueUtilities utilities, NovaManager novaManager) {
        this.reference = reference;
        this.version = version;
        this.utilities = utilities;
        this.novaManager = novaManager;
    }

    protected ByteBuffer createMyByteBuffer() {
        int[] ints = UnsafeUtils.longToInts(reference);
        return novaManager.getByteBufferFromBlockID(ints[0] >>> BLOCK_ID_SHIFT, ints[1], ints[0] & LENGTH_MASK);
    }

    protected long createReference(int blockID, int position, int length) {
        return UnsafeUtils.intsToLong((blockID << BLOCK_ID_SHIFT) + (length & LENGTH_MASK), position);
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

    private <V> void writeValue(ByteBuffer bb, V newValue, OakSerializer<V> serializer) {
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
            utilities.unlockRead(bb, version);
        }
    }

    private <V> V readValue(ByteBuffer bb, OakSerializer<V> serializer) {
        return serializer.deserialize(utilities.getReadOnlyBuffer(bb));
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
    public Result delete() {
        ByteBuffer bb = createMyByteBuffer();
        Result result = utilities.deleteValue(bb, version);
        if (result != TRUE) {
            return result;
        }
        novaManager.freeSlice(this);
        return TRUE;
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
