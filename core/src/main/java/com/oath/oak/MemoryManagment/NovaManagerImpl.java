package com.oath.oak.MemoryManagment;

import com.oath.oak.OakBlockMemoryAllocator;
import com.oath.oak.Slice;
import com.oath.oak.ThreadIndexCalculator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static com.oath.oak.NovaValueOperationsImpl.LockStates.FREE;

public class NovaManagerImpl implements NovaManager {
    static final int RELEASE_LIST_LIMIT = 1024;
    private OakBlockMemoryAllocator manager;
    private ThreadIndexCalculator threadIndexCalculator;
    private List<List<OffHeapSlice>> releaseLists;
    private AtomicInteger globalNovaNumber;
    private NovaValueUtilities utilities;

    public NovaManagerImpl(OakBlockMemoryAllocator manager, NovaValueUtilities utilities) {
        this.threadIndexCalculator = ThreadIndexCalculator.newInstance();
        this.releaseLists = new CopyOnWriteArrayList<>();
        for (int i = 0; i < ThreadIndexCalculator.MAX_THREADS; i++) {
            this.releaseLists.add(new ArrayList<>(RELEASE_LIST_LIMIT));
        }
        globalNovaNumber = new AtomicInteger(1);
        this.manager = manager;
        this.utilities = utilities;
    }

    @Override
    public void close() {
        manager.close();
    }

    int getCurrentVersion() {
        return globalNovaNumber.get();
    }

    @Override
    public ByteBuffer getByteBufferFromBlockID(int blockID, int position, int length) {
        return manager.readByteBufferFromBlockID(blockID, position, length);
    }

    @Override
    public OffHeapSlice allocateSlice(int size) {
        Slice s = manager.allocateSlice(size + utilities.getHeaderSize());
        assert s.getByteBuffer().remaining() >= size + utilities.getHeaderSize();
        long version = getCurrentVersion();
        s.getByteBuffer().putLong(s.getByteBuffer().position(), version);
        s.getByteBuffer().putInt(s.getByteBuffer().position() + utilities.getLockLocation(), FREE.value);
        return new OffHeapSliceImpl(s.getBlockID(), s.getByteBuffer().position(), s.getByteBuffer().remaining(),
                version, utilities, this);
    }

    @Override
    public void freeSlice(OffHeapSlice slice) {
        int idx = threadIndexCalculator.getIndex();
        List<OffHeapSlice> myReleaseList = this.releaseLists.get(idx);
        myReleaseList.add(slice);
        if (myReleaseList.size() >= RELEASE_LIST_LIMIT) {
            globalNovaNumber.incrementAndGet();
            for (OffHeapSlice released : myReleaseList) {
                Slice releasedSlice = released.intoSlice();
                manager.freeSlice(releasedSlice);
            }
            myReleaseList.clear();
        }
    }
}
