package com.oath.oak;

import com.oath.oak.MemoryManagment.Result;

import java.nio.ByteBuffer;

public interface NovaValueUtils {
    int getHeaderSize();

    int getLockLocation();

    int getLockSize();

    ByteBuffer getActualValueThreadSafe(Slice s);

    ByteBuffer getActualValue(Slice s);

    Result lockRead(Slice s, int version);

    Result unlockRead(Slice s, int version);

    Result lockWrite(Slice s, int version);

    Result unlockWrite(Slice s);

    Result deleteValue(Slice s, int version);

    Result isValueDeleted(Slice s, int version);

    int getOffHeapVersion(Slice s);
}
