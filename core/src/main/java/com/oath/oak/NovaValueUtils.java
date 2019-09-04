package com.oath.oak;

import java.nio.ByteBuffer;

public interface NovaValueUtils {
    enum Result {
        TRUE, FALSE, RETRY
    }

    int NO_VERSION = -1;

    int getHeaderSize();

    int getLockLocation();

    int getLockSize();

    default ByteBuffer getActualValueThreadSafe(Slice s) {
        ByteBuffer bb = s.getByteBuffer();
        bb.position(bb.position() + getHeaderSize());
        ByteBuffer dup = bb.slice();
        bb.position(bb.position() - getHeaderSize());
        return dup;
    }

    default ByteBuffer getActualValue(Slice s) {
        ByteBuffer dup = s.getByteBuffer().duplicate();
        dup.position(dup.position() + getHeaderSize());
        return dup.slice();
    }

    Result lockRead(Slice s, int version);

    Result unlockRead(Slice s, int version);

    Result lockWrite(Slice s, int version);

    Result unlockWrite(Slice s);

    Result deleteValue(Slice s, int version);

    Result isValueDeleted(Slice s, int version);
}
