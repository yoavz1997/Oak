/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.function.Function;

import static com.oath.oak.Chunk.VALUE_BLOCK_SHIFT;
import static com.oath.oak.Chunk.VALUE_LENGTH_MASK;
import static com.oath.oak.NovaValueUtils.Result.*;

// remove header
public class OakRValueBufferImpl implements OakRBuffer {
    private long valueStats;
    private final long keyStats;
    private int version;
    private final NovaValueOperations operator;
    private final NovaAllocator memoryManager;
    private final InternalOakMap<?, ?> internalOakMap;

    OakRValueBufferImpl(long valueStats, int valueVersion, long keyStats, NovaValueOperations operator,
                        NovaAllocator memoryManager, InternalOakMap<?, ?> internalOakMap) {
        this.valueStats = valueStats;
        this.keyStats = keyStats;
        this.version = valueVersion;
        this.operator = operator;
        this.memoryManager = memoryManager;
        this.internalOakMap = internalOakMap;
    }

    private Slice getValueSlice() {
        int[] valueArray = UnsafeUtils.longToInts(valueStats);
        return memoryManager.getSliceFromBlockID(valueArray[0] >>> VALUE_BLOCK_SHIFT, valueArray[1],
                valueArray[0] & VALUE_LENGTH_MASK);
    }

    private ByteBuffer getByteBuffer() {
        return getValueSlice().getByteBuffer();
    }

    private int valuePosition() {
        return UnsafeUtils.longToInts(valueStats)[1] + operator.getHeaderSize();
    }

    @Override
    public int capacity() {
        return (UnsafeUtils.longToInts(valueStats)[0] & VALUE_LENGTH_MASK) - operator.getHeaderSize();
    }

    @Override
    public byte get(int index) {
        Slice s = getValueSlice();
        start(s);
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        byte b = s.getByteBuffer().get(index + valuePosition());
        end(s);
        return b;
    }

    @Override
    public ByteOrder order() {
        ByteOrder order;
        Slice s = getValueSlice();
        start(s);
        order = s.getByteBuffer().order();
        end(s);
        return order;
    }

    @Override
    public char getChar(int index) {
        char c;
        Slice s = getValueSlice();
        start(s);
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        c = s.getByteBuffer().getChar(index + valuePosition());
        end(s);
        return c;
    }

    @Override
    public short getShort(int index) {
        short i;
        Slice s = getValueSlice();
        start(s);
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        i = s.getByteBuffer().getShort(index + valuePosition());
        end(s);
        return i;
    }

    @Override
    public int getInt(int index) {
        int i;
        Slice s = getValueSlice();
        start(s);
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        i = s.getByteBuffer().getInt(index + valuePosition());
        end(s);
        return i;
    }

    @Override
    public long getLong(int index) {
        long l;
        Slice s = getValueSlice();
        start(s);
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        l = s.getByteBuffer().getLong(index + valuePosition());
        end(s);
        return l;
    }

    @Override
    public float getFloat(int index) {
        float f;
        Slice s = getValueSlice();
        start(s);
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        f = s.getByteBuffer().getFloat(index + valuePosition());
        end(s);
        return f;
    }

    @Override
    public double getDouble(int index) {
        double d;
        Slice s = getValueSlice();
        start(s);
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        d = s.getByteBuffer().getDouble(index + valuePosition());
        end(s);
        return d;
    }

    /**
     * Returns a transformation of ByteBuffer content.
     *
     * @param transformer the function that executes the transformation
     * @return a transformation of the ByteBuffer content
     * @throws NullPointerException if the transformer is null
     */
    public <T> T transform(Function<ByteBuffer, T> transformer) {
        if (transformer == null) {
            throw new NullPointerException();
        }
        Map.Entry<NovaValueUtils.Result, T> result = operator.transform(getValueSlice(), transformer, version);
        if (result.getKey() == FALSE) {
            throw new ConcurrentModificationException();
        } else if (result.getKey() == RETRY) {
            throw new UnsupportedOperationException();
        }
        return result.getValue();
    }

    @Override
    public void unsafeCopyBufferToIntArray(int srcPosition, int[] dstArray, int countInts) {
        Slice s = getValueSlice();
        start(s);
        ByteBuffer dup = s.getByteBuffer().duplicate();
        dup.position(dup.position() + operator.getHeaderSize());
        operator.unsafeBufferToIntArrayCopy(dup, srcPosition, dstArray, countInts);
        end(s);
    }

    private void start(Slice valueSlice) {
        NovaValueUtils.Result res = operator.lockRead(valueSlice, version);
        if (res == FALSE) {
            throw new ConcurrentModificationException();
        }
        if (res == RETRY) {
            Chunk.LookUp lookUp = internalOakMap.getValueFromIndex(keyStats);
            if (lookUp == null || valueSlice == null) {
                throw new ConcurrentModificationException();
            }
            valueStats = lookUp.valueStats;
            version = lookUp.version;
            start(getValueSlice());
        }
    }

    private void end(Slice valueSlice) {
        operator.unlockRead(valueSlice, version);
    }

}
