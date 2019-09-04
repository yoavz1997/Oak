package com.oath.oak;

import java.nio.ByteBuffer;

public interface OffHeapValueUtils {
    enum Result {
        TRUE, FALSE, RETRY
    }

    int getHeaderSize();

    int getLockLocation();

    int getLockSize();

    ByteBuffer getActualValueThreadSafe(Slice s);

    ByteBuffer getActualValue(Slice s);

    Result lockRead(Slice s, int generation);

    Result unlockRead(Slice s, int generation);

    Result lockWrite(Slice s, int generation);

    Result unlockWrite(Slice s);

    Result deleteValue(Slice s, int generation);

    Result isValueDeleted(Slice s, int generation);
}
