package com.oath.oak.MemoryManagment;

import com.oath.oak.UnsafeUtils;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

import static com.oath.oak.MemoryManagment.Result.*;
import static com.oath.oak.NovaValueOperationsImpl.LockStates.*;
import static java.lang.Integer.reverseBytes;

public class NovaValueUtilitiesImpl implements NovaValueUtilities {
    private static Unsafe unsafe = UnsafeUtils.unsafe;
    private static final int LOCK_SHIFT = 2;
    private static final int LOCK_MASK = 0x3;

    private int getInt(ByteBuffer bb, int index) {
        return bb.getInt(bb.position() + index);
    }

    private void putInt(ByteBuffer bb, int index, int value) {
        bb.putInt(bb.position() + index, value);
    }

    private long getLong(ByteBuffer bb, int index) {
        return bb.getLong(bb.position() + index);
    }

    private boolean CAS(ByteBuffer s, int expectedLock, int newLock) {
        return unsafe.compareAndSwapInt(null,
                ((DirectBuffer) s).address() + s.position() + getLockLocation(), reverseBytes(expectedLock),
                reverseBytes(newLock));
    }

    @Override
    public int getHeaderSize() {
        return getLockLocation() + getLockSize();
    }

    @Override
    public int getLockLocation() {
        return Long.BYTES;
    }

    @Override
    public int getLockSize() {
        return Integer.BYTES;
    }

    @Override
    public ByteBuffer getActualBuffer(ByteBuffer s) {
        ByteBuffer value = s.duplicate();
        value.position(value.position() + getHeaderSize());
        return value;
    }

    @Override
    public ByteBuffer getActualReadOnlyBuffer(ByteBuffer s) {
        ByteBuffer value = s.asReadOnlyBuffer();
        value.position(value.position() + getHeaderSize());
        return value;
    }

    @Override
    public Result lockRead(ByteBuffer s, long version) {
        int lockState;
        do {
            if (getLong(s, 0) != version) {
                return RETRY;
            }
            lockState = getInt(s, getLockLocation());
            if (lockState == DELETED.value) {
                return FALSE;
            } else if (lockState == MOVED.value) {
                return RETRY;
            }
            lockState &= ~LOCK_MASK;
        } while (!CAS(s, lockState, lockState + (1 << LOCK_SHIFT)));
        if (getLong(s, 0) != version) {
            unlockRead(s);
            return RETRY;
        }
        return TRUE;
    }

    @Override
    public Result unlockRead(ByteBuffer s) {
        int lockState;
        do {
            lockState = getInt(s, getLockLocation());
            assert lockState >= (1 << LOCK_SHIFT);
        } while (!CAS(s, lockState, lockState - (1 << LOCK_SHIFT)));
        return TRUE;
    }

    @Override
    public Result lockWrite(ByteBuffer s, long version) {
        do {
            if (getLong(s, 0) != version) {
                return RETRY;
            }
            int lockState = getInt(s, getLockLocation());
            if (lockState == DELETED.value) {
                return FALSE;
            } else if (lockState == MOVED.value) {
                return RETRY;
            }
        } while (!CAS(s, FREE.value, LOCKED.value));
        if (getLong(s, 0) != version) {
            unlockWrite(s);
            return RETRY;
        }
        return TRUE;
    }

    @Override
    public Result unlockWrite(ByteBuffer s) {
        assert getInt(s, getLockLocation()) == LOCKED.value;
        s.putInt(getLockLocation(), FREE.value);
        return TRUE;
    }

    @Override
    public Result deleteValue(ByteBuffer s, long version) {
        final Result result = lockWrite(s, version);
        if (result != TRUE) {
            return result;
        }
        putInt(s, getLockLocation(), DELETED.value);
        return TRUE;
    }

    @Override
    public Result isValueDeleted(ByteBuffer s, long version) {
        long currentVersion = getOffHeapVersion(s);
        if (currentVersion != version) {
            return RETRY;
        }
        int lockState = getInt(s, getLockLocation());
        if (getOffHeapVersion(s) != currentVersion) {
            return RETRY;
        }
        // Now we have an atomic snapshot of both the version and the lock
        return lockState == DELETED.value ? TRUE : FALSE;
    }

    @Override
    public long getOffHeapVersion(ByteBuffer s) {
        return getLong(s, 0);
    }
}
