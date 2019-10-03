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

    <T> Result put(T newValue, OakSerializer<T> serializer);

    <T> Map.Entry<Result, T> get(OakSerializer<T> serializer);

    <T> Map.Entry<Result, T> exchange(T newValue, OakSerializer<T> serializer);

    <T> Result compareAndExchange(T expected, T newValue, OakSerializer<T> serializer, Comparator<T> comparator);

    <T> Map.Entry<Result, T> delete(OakSerializer<T> serializer);

    <T> Result compute(Function<T, T> computer, OakSerializer<T> serializer);

    ZcOffHeapSlice zc();
}
