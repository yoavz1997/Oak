package com.oath.oak;

import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NovaAllocatorTest {

    @Test
    public void reuseTest() {
        NovaAllocator novaAllocator = new NovaAllocator(new OakNativeMemoryAllocator(128));
        long oldVersion = novaAllocator.getCurrentVersion();
        Slice[] allocatedSlices = new Slice[NovaAllocator.RELEASE_LIST_LIMIT];
        for (int i = 0; i < NovaAllocator.RELEASE_LIST_LIMIT; i++) {
            allocatedSlices[i] = novaAllocator.allocateSlice(i + 5).duplicate();
        }
        for (int i = 0; i < NovaAllocator.RELEASE_LIST_LIMIT; i++) {
            assertEquals(i + 5, allocatedSlices[i].getByteBuffer().remaining());
            novaAllocator.releaseSlice(allocatedSlices[i]);
        }
        long newVersion = novaAllocator.getCurrentVersion();
        assertEquals(oldVersion + 1, newVersion);
        for (int i = NovaAllocator.RELEASE_LIST_LIMIT - 1; i > -1; i--) {
            Slice s = novaAllocator.allocateSlice(i + 5);
            assertEquals(allocatedSlices[i].getBlockID(), s.getBlockID());
            assertEquals(allocatedSlices[i].getByteBuffer().position(), s.getByteBuffer().position());
            assertEquals(allocatedSlices[i].getByteBuffer().limit(), s.getByteBuffer().limit());
        }
    }
}
