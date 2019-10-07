/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Function;

import static com.oath.oak.Chunk.KEY_BLOCK_SHIFT;
import static com.oath.oak.Chunk.KEY_LENGTH_MASK;
import static com.oath.oak.UnsafeUtils.longToInts;

public class OakRKeyBufferImpl implements OakRBuffer {

    private final long keyStats;
    private final NovaAllocator memoryManager;
    private final int initialPosition;

    OakRKeyBufferImpl(long keyStats, NovaAllocator memoryManager) {
        this.keyStats = keyStats;
        this.memoryManager = memoryManager;
        this.initialPosition = longToInts(keyStats)[1];
    }

    private ByteBuffer getKeyBuffer() {
        int[] keyArray = longToInts(keyStats);
        return memoryManager.getByteBufferFromBlockID(keyArray[0] >>> KEY_BLOCK_SHIFT, keyArray[1],
                keyArray[0] & KEY_LENGTH_MASK);
    }

    @Override
    public int capacity() {
        return longToInts(keyStats)[0] & KEY_LENGTH_MASK;
    }

    @Override
    public byte get(int index) {
        return getKeyBuffer().get(index + initialPosition);
    }

    @Override
    public ByteOrder order() {
        return getKeyBuffer().order();
    }

    @Override
    public char getChar(int index) {
        return getKeyBuffer().getChar(index + initialPosition);
    }

    @Override
    public short getShort(int index) {
        return getKeyBuffer().getShort(index + initialPosition);
    }

    @Override
    public int getInt(int index) {
        return getKeyBuffer().getInt(index + initialPosition);
    }

    @Override
    public long getLong(int index) {
        return getKeyBuffer().getLong(index + initialPosition);
    }

    @Override
    public float getFloat(int index) {
        return getKeyBuffer().getFloat(index + initialPosition);
    }

    @Override
    public double getDouble(int index) {
        return getKeyBuffer().getChar(index + initialPosition);
    }

    @Override
    public <T> T transform(Function<ByteBuffer, T> transformer) {
        return transformer.apply(getKeyBuffer().slice().asReadOnlyBuffer());
    }

    @Override
    public void unsafeCopyBufferToIntArray(int srcPosition, int[] dstArray, int countInts) {
        UnsafeUtils.unsafeCopyBufferToIntArray(getKeyBuffer().slice(), srcPosition, dstArray, countInts);
    }

}
