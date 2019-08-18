package com.oath.oak;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Consumer;
import java.util.function.Function;

public class ValueUtils {

    private static final int LOCK_MASK = 0x3;
    private static final int LOCK_FREE = 0;
    private static final int LOCK_LOCKED = 1;
    private static final int LOCK_DELETED = 2;
    public static final int VALUE_HEADER_SIZE = 4;

    private static Unsafe unsafe;

    private static void invariant(ByteBuffer bb) {
        int old = bb.getInt(bb.position());
        assert (old & LOCK_MASK) != 3;
    }

    private static int reverseBytes(int i) {
        int newI = 0;
        newI += (i & 0xff) << 24;
        newI += (i & 0xff00) << 16;
        newI += (i & 0xff0000) << 8;
        newI += (i & 0xff000000);
        return newI;
    }

    static {
        try {
            Constructor<Unsafe> unsafeConstructor = Unsafe.class.getDeclaredConstructor();
            unsafeConstructor.setAccessible(true);
            unsafe = unsafeConstructor.newInstance();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    //One duplication
    static ByteBuffer getActualValueBufferLessDuplications(ByteBuffer bb){
        bb.position(bb.position() + VALUE_HEADER_SIZE);
        ByteBuffer dup = bb.slice();
        bb.position(bb.position() - VALUE_HEADER_SIZE);
        return dup;
    }

    // Two duplications
    static ByteBuffer getActualValueBuffer(ByteBuffer bb) {
        ByteBuffer dup = bb.duplicate();
        dup.position(dup.position() + VALUE_HEADER_SIZE);
        return dup.slice();
    }

    private static boolean CAS(ByteBuffer bb, int expected, int value) {
        // assuming big endian
        assert bb.order() == ByteOrder.BIG_ENDIAN;
        return unsafe.compareAndSwapInt(null, ((DirectBuffer) bb).address() + bb.position(), reverseBytes(expected), reverseBytes(value));
    }

    /* Header Utils */

    static boolean isValueDeleted(ByteBuffer bb) {
        return isValueDeleted(bb.getInt(bb.position()));

    }

    private static boolean isValueDeleted(int header) {
        return header == LOCK_DELETED;
    }

    static boolean lockRead(ByteBuffer bb) {
        assert bb.isDirect();
        invariant(bb);
        int oldHeader;
        do {
            oldHeader = bb.getInt(bb.position());
            if (isValueDeleted(oldHeader)) return false;
            oldHeader &= ~LOCK_MASK;
        } while (!CAS(bb, oldHeader, oldHeader + 4));
        invariant(bb);
        return true;
    }

    static void unlockRead(ByteBuffer bb) {
        assert bb.isDirect();
        int oldHeader;
        invariant(bb);
        do {
            oldHeader = bb.getInt(bb.position());
        } while (!CAS(bb, oldHeader, oldHeader - 4));
        invariant(bb);
    }

    private static boolean lockWrite(ByteBuffer bb) {
        assert bb.isDirect();
        int oldHeader;
        invariant(bb);
        do {
            oldHeader = bb.getInt(bb.position());
            if (isValueDeleted(oldHeader)) return false;
        } while (!CAS(bb, LOCK_FREE, LOCK_LOCKED));
        invariant(bb);
        return true;
    }

    private static void unlockWrite(ByteBuffer bb) {
        invariant(bb);
        bb.putInt(bb.position(), LOCK_FREE);
        invariant(bb);
        // maybe a fence?
    }

    private static boolean deleteValue(ByteBuffer bb) {
        assert bb.isDirect();
        int oldHeader;
        invariant(bb);
        do {
            oldHeader = bb.getInt(bb.position());
            if (isValueDeleted(oldHeader)) return false;
        } while (!CAS(bb, LOCK_FREE, LOCK_DELETED));
        invariant(bb);
        return true;
    }

    /* Handle Methods */

    static void unsafeBufferToIntArrayCopy(ByteBuffer bb, int srcPosition, int[] dstArray, int countInts) {
        UnsafeUtils.unsafeCopyBufferToIntArray(bb, srcPosition, dstArray, countInts);
    }

    static <T> T transform(ByteBuffer bb, Function<ByteBuffer, T> transformer) {
        if (!lockRead(bb)) return null;

        T transformation = transformer.apply(getActualValueBuffer(bb).asReadOnlyBuffer());
        unlockRead(bb);
        return transformation;
    }

    public static <V> boolean put(ByteBuffer bb, V newVal, OakSerializer<V> serializer, MemoryManager memoryManager) {
        if (!lockWrite(bb)) return false;
        int capacity = serializer.calculateSize(newVal);
        ByteBuffer dup = getActualValueBuffer(bb);
        if (bb.remaining() < capacity) { // can not reuse the existing space
            memoryManager.release(dup);
            memoryManager.allocate(capacity + VALUE_HEADER_SIZE);
            throw new RuntimeException();
            // TODO: update the relevant entry
        }
        // It seems like I have to change bb here or else I expose the header to the user
        // Duplicating bb instead
        serializer.serialize(newVal, dup);
        unlockWrite(bb);
        return true;
    }

    // Why do we use finally here and not in put for example
    static boolean compute(ByteBuffer bb, Consumer<OakWBuffer> computer) {
        if (!lockWrite(bb)) return false;
        OakWBuffer wBuffer = new OakWBufferImpl(bb);
        computer.accept(wBuffer);
        unlockWrite(bb);
        return true;
    }

    static boolean remove(ByteBuffer bb, MemoryManager memoryManager) {
        if (!deleteValue(bb)) return false;
        // releasing the actual value and not the header
        memoryManager.release(getActualValueBuffer(bb));
        return true;
    }

    /**
     * Applies a transformation under writers locking
     *
     * @param transformer transformation to apply
     * @return Transformation result or null if value is deleted
     */
    static <T> T mutatingTransform(ByteBuffer bb, Function<ByteBuffer, T> transformer) {
        T result;
        if (!lockWrite(bb))
            // finally clause will handle unlock
            return null;
        result = transformer.apply(getActualValueBuffer(bb));
        unlockWrite(bb);
        return result;
    }
}
