package com.oath.oak.MemoryManagment;

import com.oath.oak.MemoryManagment.ZC.ZcOffHeapSlice;
import com.oath.oak.OakSerializer;
import com.oath.oak.Slice;

import java.util.Comparator;
import java.util.Map;
import java.util.function.Function;

public interface OffHeapSlice {

    /**
     * This transform the offHeapSlice into an actual slice which can be directly used for accessing the off-heap
     * memory.
     * Should be used with caution since there is no way to enforce that the memory underneath this slice was not
     * reclaimed and then reused.
     *
     * @return A translation of the offHeapSlice (block ID and ByteBuffer)
     */
    Slice intoSlice();

    <V> Result put(V newValue, OakSerializer<V> serializer);

    <V> Map.Entry<Result, V> get(OakSerializer<V> serializer);

    <V> Map.Entry<Result, V> exchange(V newValue, OakSerializer<V> serializer);

    <V> Result compareAndExchange(V expected, V newValue, OakSerializer<V> serializer, Comparator<V> comparator);

    <V> Map.Entry<Result, V> delete(OakSerializer<V> serializer);

    <V> Result compute(Function<V, V> computer, OakSerializer<V> serializer);

    ZcOffHeapSlice zc();
}
