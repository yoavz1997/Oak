package com.oath.oak;

import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GemmAllocatorTest {

    @Test
    public void reuseTest() {
        GemmAllocator gemmAllocator = new GemmAllocator(new OakNativeMemoryAllocator(128));
        long oldGeneration = gemmAllocator.getCurrentGeneration();
        Slice[] allocatedSlices = new Slice[GemmAllocator.RELEASE_LIST_LIMIT];
        for (int i = 0; i < GemmAllocator.RELEASE_LIST_LIMIT; i++) {
            allocatedSlices[i] = gemmAllocator.allocateSlice(i + 5);
        }
        for (int i = 0; i < GemmAllocator.RELEASE_LIST_LIMIT; i++) {
            assertEquals(i + 5, allocatedSlices[i].getByteBuffer().remaining());
            gemmAllocator.releaseSlice(allocatedSlices[i]);
        }
        long newGeneration = gemmAllocator.getCurrentGeneration();
        assertEquals(oldGeneration + 1, newGeneration);
        for (int i = GemmAllocator.RELEASE_LIST_LIMIT - 1; i > -1; i--) {
            assertEquals(allocatedSlices[i], gemmAllocator.allocateSlice(i + 5));
        }
    }
}
