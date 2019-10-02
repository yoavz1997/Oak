package com.oath.oak.MemoryManagment.ZC;

import com.oath.oak.MemoryManagment.Result;
import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;
import com.oath.oak.OakComparator;
import com.oath.oak.OakSerializer;
import com.oath.oak.Slice;

import java.util.Comparator;
import java.util.Map;

public interface ZcOffHeapSlice {

    Slice intoSlice();

    <V> Result put(V newValue, OakSerializer<V> serializer);

    Map.Entry<Result, OffHeapReadBuffer> get();

    <V> Result compareAndExchange(V expected, V newValue, OakSerializer<V> serializer,OakComparator<V> comparator);

    Result delete();

    Result compute(Comparator<OffHeapWriteBuffer> computer);
}
