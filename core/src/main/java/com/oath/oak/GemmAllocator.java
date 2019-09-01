package com.oath.oak;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class GemmAllocator implements Closeable {
    static final int RELEASE_LIST_LIMIT = 1024;
    static final int GEMM_HEADER_SIZE = 4;
    public static final int INVALID_GENERATION = -1;
    private ThreadIndexCalculator threadIndexCalculator;
    private List<List<Slice>> releaseLists;
    private AtomicInteger globalGemmNumber;
    private OakBlockMemoryAllocator manager;

    GemmAllocator(OakBlockMemoryAllocator manager) {
        this.threadIndexCalculator = ThreadIndexCalculator.newInstance();
        this.releaseLists = new CopyOnWriteArrayList<>();
        for (int i = 0; i < ThreadIndexCalculator.MAX_THREADS; i++) {
            this.releaseLists.add(new ArrayList<>(RELEASE_LIST_LIMIT));
        }
        globalGemmNumber = new AtomicInteger(0);
        this.manager = manager;
    }

    @Override
    public void close() {
        manager.close();
    }

    public int getCurrentGeneration() {
        return globalGemmNumber.get();
    }

    public long allocated() {
        return manager.allocated();
    }

    // The returned slice's position is after the GEMM, but there is no need to slice it
    public Slice allocateSlice(int size) {
        Slice s = manager.allocateSlice(size + GEMM_HEADER_SIZE);
        s.getByteBuffer().putInt(s.getByteBuffer().position(), getCurrentGeneration());
        assert s.getByteBuffer().remaining() == size;
        return s;
    }

    public void releaseSlice(Slice s) {
        int idx = threadIndexCalculator.getIndex();
        List<Slice> myReleaseList = this.releaseLists.get(idx);
        myReleaseList.add(s);
        if (myReleaseList.size() >= RELEASE_LIST_LIMIT) {
            globalGemmNumber.incrementAndGet();
            myReleaseList.forEach(manager::freeSlice);
            myReleaseList.clear();
        }
    }

    public Slice getSliceFromBlockID(Integer BlockID, int bufferPosition, int bufferLength) {
        return new Slice(BlockID, manager.readByteBufferFromBlockID(BlockID, bufferPosition, bufferLength));
    }

    public ByteBuffer getByteBufferFromBlockID(Integer BlockID, int bufferPosition, int bufferLength) {
        return manager.readByteBufferFromBlockID(BlockID, bufferPosition, bufferLength);
    }

    public static boolean verifyGeneration(ByteBuffer bb, int generation) {
        long writtenGeneration = bb.getLong(bb.position() - GEMM_HEADER_SIZE);
        return writtenGeneration == generation;
    }
}
