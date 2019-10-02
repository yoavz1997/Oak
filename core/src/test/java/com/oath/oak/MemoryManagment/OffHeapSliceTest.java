package com.oath.oak.MemoryManagment;

import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;
import com.oath.oak.Slice;
import com.oath.oak.StringSerializer;
import org.junit.Test;

import java.util.Map;
import java.util.Random;

import static com.oath.oak.MemoryManagment.Result.FALSE;
import static com.oath.oak.MemoryManagment.Result.TRUE;
import static org.junit.Assert.assertEquals;

public class OffHeapSliceTest {
    private final NovaValueUtilitiesImpl utilities = new NovaValueUtilitiesImpl();
    private NovaManager manager = new NovaManagerImpl(new OakNativeMemoryAllocator(128), utilities);

    @Test
    public void writeToOffHeapSliceReadFromByteBufferTest() {
        int size = 32;
        OffHeapSlice offHeapSlice = manager.allocateSlice(size);
        Slice slice = offHeapSlice.intoSlice();
        assertEquals(size + utilities.getHeaderSize(), slice.getByteBuffer().remaining());
        int numOfChars = (size - Integer.BYTES) / Character.BYTES;
        Random random = new Random();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < numOfChars; i++) {
            builder.append((char) random.nextInt(Character.MAX_VALUE));
        }
        StringSerializer serializer = new StringSerializer();
        String string = builder.toString();
        assert serializer.calculateSize(string) == size;
        assertEquals(TRUE, offHeapSlice.put(string, serializer));
        String readResult = serializer.deserialize(utilities.getActualReadOnlyBuffer(slice.getByteBuffer()));
        assertEquals(string, readResult);
    }

    @Test
    public void writeToByteBufferReadFromOffHeapSliceTest() {
        int size = 32;
        OffHeapSlice offHeapSlice = manager.allocateSlice(size);
        Slice slice = offHeapSlice.intoSlice();
        assertEquals(size + utilities.getHeaderSize(), slice.getByteBuffer().remaining());
        int numOfChars = (size - Integer.BYTES) / Character.BYTES;
        Random random = new Random();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < numOfChars; i++) {
            builder.append((char) random.nextInt(Character.MAX_VALUE));
        }
        StringSerializer serializer = new StringSerializer();
        String string = builder.toString();
        assert serializer.calculateSize(string) == size;
        serializer.serialize(string, utilities.getActualBuffer(slice.getByteBuffer()));
        Map.Entry<Result, String> resultStringEntry = offHeapSlice.get(serializer);
        assertEquals(TRUE, resultStringEntry.getKey());
        assertEquals(string, resultStringEntry.getValue());
    }

    @Test
    public void cannotPutToDeletedSliceTest() {
        int size = 32;
        OffHeapSlice offHeapSlice = manager.allocateSlice(size);
        assertEquals(TRUE, offHeapSlice.delete(null).getKey());
        assertEquals(FALSE, offHeapSlice.put("hello", new StringSerializer()));
    }

    @Test
    public void exchangeTest() {
        int size = 32;
        OffHeapSlice offHeapSlice = manager.allocateSlice(size);
        int numOfChars = (size - Integer.BYTES) / Character.BYTES;
        Random random = new Random();
        StringBuilder builder1 = new StringBuilder();
        StringBuilder builder2 = new StringBuilder();
        for (int i = 0; i < numOfChars; i++) {
            builder1.append((char) random.nextInt(Character.MAX_VALUE));
            builder2.append((char) random.nextInt(Character.MAX_VALUE));
        }
        StringSerializer serializer = new StringSerializer();
        String string1 = builder1.toString();
        String string2 = builder2.toString();
        assertEquals(TRUE, offHeapSlice.put(string1, serializer));
        Map.Entry<Result, String> resultStringEntry = offHeapSlice.exchange(string2, serializer);
        assertEquals(TRUE, resultStringEntry.getKey());
        assertEquals(string1, resultStringEntry.getValue());
        resultStringEntry = offHeapSlice.get(serializer);
        assertEquals(TRUE, resultStringEntry.getKey());
        assertEquals(string2, resultStringEntry.getValue());
    }
}
