package com.oath.oak;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class NovaManager implements Closeable {
    static final int RELEASE_LIST_LIMIT = 1024;
    static final int NOVA_HEADER_SIZE = 4;
    static final int INVALID_VERSION = 0;
    private ThreadIndexCalculator threadIndexCalculator;
    private List<List<Slice>> releaseLists;
    private AtomicInteger globalNovaNumber;
    private OakBlockMemoryAllocator manager;
    public final AtomicInteger keysAllocated = new AtomicInteger(0);
    public final AtomicInteger valuesAllocated = new AtomicInteger(0);

    NovaManager(OakBlockMemoryAllocator manager) {
        this.threadIndexCalculator = ThreadIndexCalculator.newInstance();
        this.releaseLists = new CopyOnWriteArrayList<>();
        for (int i = 0; i < ThreadIndexCalculator.MAX_THREADS; i++) {
            this.releaseLists.add(new ArrayList<>(RELEASE_LIST_LIMIT));
        }
        globalNovaNumber = new AtomicInteger(1);
        this.manager = manager;
    }

    @Override
    public void close() {
        manager.close();
    }

    boolean isClosed() {
        return manager.isClosed();
    }

    int getCurrentVersion() {
        return globalNovaNumber.get();
    }

    public long allocated() {
        return manager.allocated();
    }

    Slice allocateSlice(int size) {
        Slice s = manager.allocateSlice(size);
        assert s.getByteBuffer().remaining() >= size;
        s.getByteBuffer().putInt(s.getByteBuffer().position(), getCurrentVersion());
        return s;
    }

    void releaseSlice(Slice s) {
        int idx = threadIndexCalculator.getIndex();
        List<Slice> myReleaseList = this.releaseLists.get(idx);
        myReleaseList.add(s.duplicate());
        if (myReleaseList.size() >= RELEASE_LIST_LIMIT) {
            globalNovaNumber.incrementAndGet();
            for (Slice releasedSlice : myReleaseList) {
                manager.freeSlice(releasedSlice);
            }
            myReleaseList.clear();
        }
    }

    Slice getSliceFromBlockID(Integer BlockID, int bufferPosition, int bufferLength) {
        return new Slice(BlockID, getByteBufferFromBlockID(BlockID, bufferPosition, bufferLength));
    }

    ByteBuffer getByteBufferFromBlockID(Integer BlockID, int bufferPosition, int bufferLength) {
        return manager.readByteBufferFromBlockID(BlockID, bufferPosition, bufferLength);
    }

}
