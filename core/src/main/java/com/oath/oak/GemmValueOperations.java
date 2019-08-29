package com.oath.oak;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.function.Consumer;
import java.util.function.Function;

public interface GemmValueOperations extends GemmValueUtils {

    void unsafeBufferToIntArrayCopy(ByteBuffer bb, int srcPosition, int[] dstArray, int countInts);

    <T> AbstractMap.SimpleEntry<Result, T> transform(Slice s, Function<ByteBuffer, T> transformer, int generation);

    <K, V> Result put(Chunk<K, V> chunk, Chunk.LookUp lookUp, V newVal, int generation, OakSerializer<V> serializer, GemmAllocator memoryManager);

    Result compute(Slice s, int generation, Consumer<OakWBuffer> computer);

    Result remove(Slice s, int generation, GemmAllocator memoryManager);
}
