package com.oath.oak.MemoryManagment.ZC;

import com.oath.oak.MemoryManagment.Result;
import com.oath.oak.OakSerializer;

import java.util.Comparator;
import java.util.Map;

public interface ZcOffHeapSlice {

    <V> Result put(V newValue, OakSerializer<V> serializer);

    Map.Entry<Result, OffHeapReadBuffer> get();

    <V> Result compareAndExchange(V expected, V newValue, Comparator<V> comparator);

    Result delete();

    <V> Result compute(Comparator<OffHeapWriteBuffer> computer, OakSerializer<V> serializer);
}
