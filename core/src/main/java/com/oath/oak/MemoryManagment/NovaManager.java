package com.oath.oak.MemoryManagment;

import java.nio.ByteBuffer;

public interface NovaManager {

    ByteBuffer getByteBufferFromBlockID(int blockID, int position, int length);

    OffHeapSlice allocateSlice(int size);

    void freeSlice(OffHeapSlice slice);
}
