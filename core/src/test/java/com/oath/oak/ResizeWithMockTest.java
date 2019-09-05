package com.oath.oak;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static com.oath.oak.NovaValueUtils.Result.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;

public class ResizeWithMockTest {
    private OakMap<String, String> oak;
    private NovaValueUtils mock;

    @Before
    public void initStuff() {
        mock = mock(NovaValueUtils.class);
        OakMapBuilder<String, String> builder = new OakMapBuilder<String, String>()
                .setChunkMaxItems(100)
                .setChunkBytesPerItem(128)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .setComparator(new StringComparator())
                .setMinKey("").setOperator(new MockNovaValueOperationsImpl(mock));

        oak = builder.build();
    }

    @Test
    public void restartPutDueToRetryTest() {
        Mockito.when(mock.lockRead(any(), anyInt())).thenReturn(TRUE);
        Mockito.when(mock.unlockRead(any(), anyInt())).thenReturn(TRUE);
        Mockito.when(mock.unlockWrite(any())).thenReturn(TRUE);
        Mockito.when(mock.isValueDeleted(any(), anyInt())).thenReturn(FALSE);
        assertNull(oak.put("123", "456"));
        assertEquals("456", oak.get("123"));
        Mockito.when(mock.lockWrite(any(), anyInt())).thenReturn(RETRY, TRUE);
        assertNotNull(oak.put("123", "789"));
        assertEquals("789", oak.get("123"));
    }

    @Test
    public void restartPutDueToConcurrentDeletionTest() {
        Mockito.when(mock.lockRead(any(), anyInt())).thenReturn(TRUE);
        Mockito.when(mock.unlockRead(any(), anyInt())).thenReturn(TRUE);
        Mockito.when(mock.lockWrite(any(), anyInt())).thenReturn(FALSE, TRUE);
        Mockito.when(mock.unlockWrite(any())).thenReturn(TRUE);
        Mockito.when(mock.isValueDeleted(any(), anyInt())).thenReturn(FALSE);
        assertNull(oak.put("123", "456"));
        assertEquals("456", oak.get("123"));
        Mockito.when(mock.isValueDeleted(any(), anyInt())).thenReturn(FALSE, TRUE);
        assertNull(oak.put("123", "789"));
        Mockito.when(mock.isValueDeleted(any(), anyInt())).thenReturn(FALSE);
        assertEquals("789", oak.get("123"));
    }

    @Test
    public void restartPutIfAbsentDueToRetryTest() {
        Mockito.when(mock.lockRead(any(), anyInt())).thenReturn(TRUE);
        Mockito.when(mock.unlockRead(any(), anyInt())).thenReturn(TRUE);
        Mockito.when(mock.lockWrite(any(), anyInt())).thenReturn(RETRY, TRUE);
        Mockito.when(mock.unlockWrite(any())).thenReturn(TRUE);
        Mockito.when(mock.isValueDeleted(any(), anyInt())).thenReturn(FALSE);
        assertNull(oak.put("123", "456"));
        assertEquals("456", oak.get("123"));
        assertNotNull(oak.putIfAbsent("123", "789"));
        assertEquals("456", oak.get("123"));
    }

    @Test
    public void restartPutIfAbsentDueToConcurrentDeletionTest() {
        Mockito.when(mock.lockRead(any(), anyInt())).thenReturn(TRUE);
        Mockito.when(mock.unlockRead(any(), anyInt())).thenReturn(TRUE);
        Mockito.when(mock.lockWrite(any(), anyInt())).thenReturn(FALSE, TRUE);
        Mockito.when(mock.unlockWrite(any())).thenReturn(TRUE);
        Mockito.when(mock.isValueDeleted(any(), anyInt())).thenReturn(FALSE);
        assertNull(oak.put("123", "456"));
        assertEquals("456", oak.get("123"));
        Mockito.when(mock.lockRead(any(), anyInt())).thenReturn(FALSE, TRUE);
        Mockito.when(mock.isValueDeleted(any(), anyInt())).thenReturn(FALSE, TRUE);
        assertNull(oak.putIfAbsent("123", "789"));
        Mockito.when(mock.isValueDeleted(any(), anyInt())).thenReturn(FALSE);
        assertEquals("789", oak.get("123"));
    }

    @Test
    public void restartPutIfAbsentComputeIfPresentDueToRetryTest() {
        Mockito.when(mock.lockRead(any(), anyInt())).thenReturn(TRUE);
        Mockito.when(mock.unlockRead(any(), anyInt())).thenReturn(TRUE);
        Mockito.when(mock.lockWrite(any(), anyInt())).thenReturn(RETRY, TRUE);
        Mockito.when(mock.unlockWrite(any())).thenReturn(TRUE);
        Mockito.when(mock.isValueDeleted(any(), anyInt())).thenReturn(FALSE);
        assertNull(oak.put("123", "456"));
        assertEquals("456", oak.get("123"));
        assertNotNull(oak.putIfAbsent("123", "789"));
        assertEquals("456", oak.get("123"));
    }

    @Test
    public void restartRemoveDueToRetryTest() {
        Mockito.when(mock.lockRead(any(), anyInt())).thenReturn(TRUE);
        Mockito.when(mock.unlockRead(any(), anyInt())).thenReturn(TRUE);
        Mockito.when(mock.isValueDeleted(any(), anyInt())).thenReturn(FALSE);
        assertNull(oak.put("123", "456"));
        assertEquals("456", oak.get("123"));
        Mockito.when(mock.lockRead(any(), anyInt())).thenReturn(RETRY, TRUE);
        assertEquals("456", oak.remove("123"));
        assertNull(oak.get("123"));
        assertNull(oak.put("234", "567"));
        assertEquals("567", oak.get("234"));
        Mockito.when(mock.deleteValue(any(), anyInt())).thenReturn(RETRY, TRUE);
        assertEquals("567", oak.remove("234"));
        assertNull(oak.get("234"));
    }
}
