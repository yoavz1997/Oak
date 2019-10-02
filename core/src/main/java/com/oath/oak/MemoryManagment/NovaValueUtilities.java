package com.oath.oak.MemoryManagment;

import java.nio.ByteBuffer;

public interface NovaValueUtilities {

    int getHeaderSize();

    int getLockLocation();

    int getLockSize();

    ByteBuffer getActualBuffer(ByteBuffer s);

    ByteBuffer getActualReadOnlyBuffer(ByteBuffer s);

    Result lockRead(ByteBuffer s, long version);

    Result unlockRead(ByteBuffer s);

    Result lockWrite(ByteBuffer s, long version);

    Result unlockWrite(ByteBuffer s);

    Result deleteValue(ByteBuffer s, long version);

    Result isValueDeleted(ByteBuffer s, long version);

    long getOffHeapVersion(ByteBuffer s);
}
