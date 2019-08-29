package com.oath.oak;

import java.nio.ByteBuffer;
import java.util.Optional;

public interface GemmValueUtils {

    enum Result {
        TRUE, FALSE, RETRY
    }

    int getHeaderSize();

    ByteBuffer getActualValueThreadSafe(Slice s);

    ByteBuffer getActualValue(Slice s);

    Result lockRead(Slice s, int generation);

    Result unlockRead(Slice s, int generation);

    Result lockWrite(Slice s, int generation);

    Result unlockWrite(Slice s);

    Result deleteValue(Slice s, int generation, GemmAllocator gemmAllocator);

    Result isValueDeleted(Slice s, int generation);
}
