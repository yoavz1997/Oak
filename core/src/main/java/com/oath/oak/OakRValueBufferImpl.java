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

import static com.oath.oak.GemmValueUtils.Result.*;

// remove header
public class OakRValueBufferImpl implements OakRBuffer {
    private final Slice valueSlice;
    private final ByteBuffer keyBuffer;
    private final ByteBuffer bb;
    private final int generation;
    private final GemmValueOperations operator;

    OakRValueBufferImpl(Slice valueSlice, int generation, ByteBuffer keyBuffer, GemmValueOperations operator) {
        this.valueSlice = valueSlice;
        this.keyBuffer = keyBuffer;
        this.bb = valueSlice.getByteBuffer();
        this.generation = generation;
        this.operator = operator;
    }

    private int valuePosition() {
        return bb.position() + operator.getHeaderSize();
    }

    @Override
    public int capacity() {
        start();
        int capacity = bb.remaining() - valuePosition();
        end();
        return capacity;
    }

    @Override
    public byte get(int index) {
        start();
        if (index < 0) throw new IndexOutOfBoundsException();
        byte b = bb.get(index + valuePosition());
        end();
        return b;
    }

    @Override
    public ByteOrder order() {
        ByteOrder order;
        start();
        order = bb.order();
        end();
        return order;
    }

    @Override
    public char getChar(int index) {
        char c;
        start();
        if (index < 0) throw new IndexOutOfBoundsException();
        c = bb.getChar(index + valuePosition());
        end();
        return c;
    }

    @Override
    public short getShort(int index) {
        short s;
        start();
        if (index < 0) throw new IndexOutOfBoundsException();
        s = bb.getShort(index + valuePosition());
        end();
        return s;
    }

    @Override
    public int getInt(int index) {
        int i;
        start();
        if (index < 0) throw new IndexOutOfBoundsException();
        i = bb.getInt(index + valuePosition());
        end();
        return i;
    }

    @Override
    public long getLong(int index) {
        long l;
        start();
        if (index < 0) throw new IndexOutOfBoundsException();
        l = bb.getLong(index + valuePosition());
        end();
        return l;
    }

    @Override
    public float getFloat(int index) {
        float f;
        start();
        if (index < 0) throw new IndexOutOfBoundsException();
        f = bb.getFloat(index + valuePosition());
        end();
        return f;
    }

    @Override
    public double getDouble(int index) {
        double d;
        start();
        if (index < 0) throw new IndexOutOfBoundsException();
        d = bb.getDouble(index + valuePosition());
        end();
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
        Map.Entry<GemmValueUtils.Result, T> result = operator.transform(valueSlice, transformer, generation);
        if (result.getKey() == FALSE)
            throw new ConcurrentModificationException();
        else if (result.getKey() == RETRY)
            throw new UnsupportedOperationException();
        return result.getValue();
    }

    @Override
    public void unsafeCopyBufferToIntArray(int srcPosition, int[] dstArray, int countInts) {
        start();
        ByteBuffer dup = bb.duplicate();
        dup.position(dup.position() + operator.getHeaderSize());
        operator.unsafeBufferToIntArrayCopy(dup, srcPosition, dstArray, countInts);
        end();
    }

    private void start() {
        GemmValueUtils.Result res = operator.lockRead(valueSlice, generation);
        if (res == FALSE)
            throw new ConcurrentModificationException();
        if (res == RETRY) {
            throw new UnsupportedOperationException();
            //getValueTransformation
        }
    }

    private void end() {
        operator.unlockRead(valueSlice, generation);
    }

}
