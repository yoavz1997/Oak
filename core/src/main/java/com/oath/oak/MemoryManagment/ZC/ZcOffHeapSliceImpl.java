package com.oath.oak.MemoryManagment.ZC;

import com.oath.oak.MemoryManagment.NovaManager;
import com.oath.oak.MemoryManagment.NovaValueUtilities;
import com.oath.oak.MemoryManagment.OffHeapSliceImpl;
import com.oath.oak.MemoryManagment.Result;
import com.oath.oak.OakRValueBufferImpl;
import com.oath.oak.OakSerializer;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Map;

import static com.oath.oak.MemoryManagment.Result.TRUE;

public class ZcOffHeapSliceImpl extends OffHeapSliceImpl implements ZcOffHeapSlice {
    public ZcOffHeapSliceImpl(long reference, long version, NovaValueUtilities utilities, NovaManager novaManager) {
        super(reference, version, utilities, novaManager);
    }

    @Override
    public Map.Entry<Result, OffHeapReadBuffer> get() {
        ByteBuffer bb = createMyByteBuffer();
        Result result = utilities.lockRead(bb, version);
        if (result != TRUE) {
            return new AbstractMap.SimpleImmutableEntry<>(result, null);
        }
        try {
            return new AbstractMap.SimpleImmutableEntry<>(TRUE, new OffHeapReadBufferImpl());
        } finally {
            utilities.unlockRead(bb, version);
        }
    }

    @Override
    public <V> Result compareAndExchange(V expected, V newValue, Comparator<V> comparator) {
        return null;
    }

    @Override
    public <V> Result compute(Comparator<OffHeapWriteBuffer> computer, OakSerializer<V> serializer) {
        return null;
    }
}
