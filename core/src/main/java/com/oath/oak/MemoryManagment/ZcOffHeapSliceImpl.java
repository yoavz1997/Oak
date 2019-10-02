package com.oath.oak.MemoryManagment;

import com.oath.oak.MemoryManagment.ZC.OffHeapReadBuffer;
import com.oath.oak.MemoryManagment.ZC.OffHeapReadBufferImpl;
import com.oath.oak.MemoryManagment.ZC.OffHeapWriteBuffer;
import com.oath.oak.MemoryManagment.ZC.ZcOffHeapSlice;
import com.oath.oak.OakComparator;
import com.oath.oak.OakSerializer;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Map;

import static com.oath.oak.MemoryManagment.Result.FALSE;
import static com.oath.oak.MemoryManagment.Result.TRUE;

public class ZcOffHeapSliceImpl extends OffHeapSliceImpl implements ZcOffHeapSlice {
    ZcOffHeapSliceImpl(long reference, long version, NovaValueUtilities utilities, NovaManager novaManager) {
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
            utilities.unlockRead(bb);
        }
    }

    @Override
    public <V> Result compareAndExchange(V expected, V newValue, OakSerializer<V> serializer,
                                         OakComparator<V> comparator) {
        ByteBuffer bb = createMyByteBuffer();
        Result result = utilities.lockWrite(bb, version);
        if (result != TRUE) {
            return result;
        }
        try {
            if (comparator.compareSerializedKeyAndKey(utilities.getActualBuffer(bb), expected) != 0) {
                return FALSE;
            }
            writeValue(bb, newValue, serializer);
            return TRUE;
        } finally {
            utilities.unlockWrite(bb);
        }
    }

    @Override
    public Result delete() {
        return super.delete(null).getKey();
    }

    @Override
    public Result compute(Comparator<OffHeapWriteBuffer> computer) {
        return null;
    }
}
