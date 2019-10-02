package com.oath.oak.MemoryManagment;

import java.io.Closeable;
import java.nio.ByteBuffer;

public interface NovaManager extends Closeable {

    ByteBuffer getByteBufferFromBlockID(int blockID, int position, int length);

    OffHeapSlice allocateSlice(int size);

    void freeSlice(OffHeapSlice slice);
}
