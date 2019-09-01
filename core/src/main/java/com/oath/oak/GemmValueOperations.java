package com.oath.oak;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.function.Consumer;
import java.util.function.Function;

public interface GemmValueOperations extends GemmValueUtils {

    void unsafeBufferToIntArrayCopy(ByteBuffer bb, int srcPosition, int[] dstArray, int countInts);

    <T> AbstractMap.SimpleEntry<Result, T> transform(Slice s, Function<ByteBuffer, T> transformer, int generation);

    <K, V> Result put(Chunk<K, V> chunk, Chunk.LookUp lookUp, V newVal, OakSerializer<V> serializer, GemmAllocator memoryManager);

    Result compute(Slice s, Consumer<OakWBuffer> computer, int generation);

    Result remove(Slice s, GemmAllocator memoryManager, int generation);

    <K, V> AbstractMap.SimpleEntry<Result, V> exchange(Chunk<K, V> chunk, Chunk.LookUp lookUp, V value, Function<ByteBuffer, V> valueDeserializeTransformer, OakSerializer<V> serializer, GemmAllocator memoryManager);

    <K,V> Result compareExchange(Chunk<K, V> chunk, Chunk.LookUp lookUp, V expected, V value, Function<ByteBuffer, V> valueDeserializeTransformer, OakSerializer<V> serializer, GemmAllocator memoryManager);
}
