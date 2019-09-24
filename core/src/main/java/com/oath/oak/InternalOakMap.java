/*
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.oath.oak;


import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.oath.oak.Chunk.*;
import static com.oath.oak.NovaAllocator.INVALID_VERSION;
import static com.oath.oak.NovaAllocator.NULL_VALUE;
import static com.oath.oak.NovaValueUtils.Result.*;
import static com.oath.oak.NativeAllocator.OakNativeMemoryAllocator.INVALID_BLOCK_ID;
import static com.oath.oak.UnsafeUtils.longToInts;

class InternalOakMap<K, V> {

    /*-------------- Members --------------*/

    final ConcurrentSkipListMap<Object, Chunk<K, V>> skiplist;    // skiplist of chunks for fast navigation
    private final AtomicReference<Chunk<K, V>> head;
    private final ByteBuffer minKey;
    private final Comparator<Object> comparator;
    private final NovaAllocator memoryManager;
    private final AtomicInteger size;
    private final OakSerializer<K> keySerializer;
    private final OakSerializer<V> valueSerializer;
    private final ThreadIndexCalculator threadIndexCalculator;
    // The reference count is used to count the upper objects wrapping this internal map:
    // OakMaps (including subMaps and Views) when all of the above are closed,
    // his map can be closed and memory released.
    private final AtomicInteger referenceCount = new AtomicInteger(1);
    private final NovaValueOperations operator;
    /*-------------- Constructors --------------*/

    /**
     * init with capacity = 2g
     */

    InternalOakMap(
            K minKey,
            OakSerializer<K> keySerializer,
            OakSerializer<V> valueSerializer,
            Comparator<Object> comparator,
            NovaAllocator memoryManager,
            int chunkMaxItems,
            int chunkBytesPerItem,
            ThreadIndexCalculator threadIndexCalculator, NovaValueOperations operator) {

        this.size = new AtomicInteger(0);
        this.memoryManager = memoryManager;

        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;

        this.comparator = comparator;

        this.minKey = ByteBuffer.allocate(this.keySerializer.calculateSize(minKey));
        this.minKey.position(0);
        this.keySerializer.serialize(minKey, this.minKey);

        this.skiplist = new ConcurrentSkipListMap<>(this.comparator);

        Chunk<K, V> head = new Chunk<>(this.minKey, null, this.comparator, memoryManager, chunkMaxItems,
                this.size, keySerializer, valueSerializer, operator);
        this.skiplist.put(head.minKey, head);    // add first chunk (head) into skiplist
        this.head = new AtomicReference<>(head);
        this.threadIndexCalculator = threadIndexCalculator;
        this.operator = operator;
    }

    /*-------------- Closable --------------*/

    /**
     * cleans off heap memory
     */
    void close() {
        int res = referenceCount.decrementAndGet();
        // once reference count is zeroed, the map meant to be deleted and should not be used.
        // reference count will never grow again
        if (res == 0) {
            memoryManager.close();
        }
    }

    // yet another object started to refer to this internal map
    void open() {
        while (true) {
            int res = referenceCount.get();
            // once reference count is zeroed, the map meant to be deleted and should not be used.
            // reference count should never grow again and the referral is not allowed
            if (res == 0) {
                throw new ConcurrentModificationException();
            }
            // although it is costly CAS is used here on purpose so we never increase
            // zeroed reference count
            if (referenceCount.compareAndSet(res, res + 1)) {
                break;
            }
        }
    }

    /*-------------- size --------------*/

    /**
     * @return current off heap memory usage in bytes
     */
    long memorySize() {
        return memoryManager.allocated();
    }

    int entries() {
        return size.get();
    }

    /*-------------- Methods --------------*/

    /**
     * finds and returns the chunk where key should be located, starting from given chunk
     */
    private Chunk<K, V> iterateChunks(Chunk<K, V> c, Object key) {
        // find chunk following given chunk (next)
        Chunk<K, V> next = c.next.getReference();

        // since skiplist isn't updated atomically in split/compaction, our key might belong in the next chunk
        // we need to iterate the chunks until we find the correct one
        while ((next != null) && (comparator.compare(next.minKey, key) <= 0)) {
            c = next;
            next = c.next.getReference();
        }

        return c;
    }


    private boolean rebalance(Chunk<K, V> c) {

        if (c == null) {
            // TODO: is this ok?
            return false;
        }
        Rebalancer<K, V> rebalancer = new Rebalancer<>(c, comparator, true, memoryManager, keySerializer,
                valueSerializer, threadIndexCalculator, operator);

        rebalancer = rebalancer.engageChunks(); // maybe we encountered a different rebalancer

        // freeze all the engaged range.
        // When completed, all update (put, next pointer update) operations on the engaged range
        // will be redirected to help the rebalance procedure
        rebalancer.freeze();

        boolean result = rebalancer.createNewChunks(); // split or compact
        // if returned true then this thread was responsible for the creation of the new chunks
        // and it inserted the put

        // lists may be generated by another thread
        List<Chunk<K, V>> newChunks = rebalancer.getNewChunks();
        List<Chunk<K, V>> engaged = rebalancer.getEngagedChunks();

        connectToChunkList(engaged, newChunks);

        updateIndexAndNormalize(engaged, newChunks);

        engaged.forEach(Chunk::release);

        return result;
    }

    private void checkRebalance(Chunk<K, V> c) {
        if (c.shouldRebalance()) {
            rebalance(c);
        }
    }

    private void connectToChunkList(List<Chunk<K, V>> engaged, List<Chunk<K, V>> children) {

        updateLastChild(engaged, children);
        int countIterations = 0;
        Chunk<K, V> firstEngaged = engaged.get(0);

        // replace in linked list - we now need to find previous chunk to our chunk
        // and CAS its next to point to c1, which is the same c1 for all threads who reached this point
        // since prev might be marked (in compact itself) - we need to repeat this until successful
        while (true) {
            countIterations++;
            assert (countIterations < 10000); // this loop is not supposed to be infinite

            // start with first chunk (i.e., head)
            Map.Entry<Object, Chunk<K, V>> lowerEntry = skiplist.lowerEntry(firstEngaged.minKey);

            Chunk<K, V> prev = lowerEntry != null ? lowerEntry.getValue() : null;
            Chunk<K, V> curr = (prev != null) ? prev.next.getReference() : null;

            // if didn't succeed to find prev through the skiplist - start from the head
            if (prev == null || curr != firstEngaged) {
                prev = null;
                curr = skiplist.firstEntry().getValue();    // TODO we can store&update head for a little efficiency
                // iterate until found chunk or reached end of list
                while ((curr != firstEngaged) && (curr != null)) {
                    prev = curr;
                    curr = curr.next.getReference();
                }
            }

            // chunk is head, we need to "add it to the list" for linearization point
            if (curr == firstEngaged && prev == null) {
                this.head.compareAndSet(firstEngaged, children.get(0));
                break;
            }
            // chunk is not in list (someone else already updated list), so we're done with this part
            if ((curr == null) || (prev == null)) {
                //TODO Never reached
                break;
            }

            // if prev chunk is marked - it is deleted, need to help split it and then continue
            if (prev.next.isMarked()) {
                rebalance(prev);
                continue;
            }

            // try to CAS prev chunk's next - from chunk (that we split) into c1
            // c1 is the old chunk's replacement, and is already connected to c2
            // c2 is already connected to old chunk's next - so all we need to do is this replacement
            if ((prev.next.compareAndSet(firstEngaged, children.get(0), false, false)) ||
                    (!prev.next.isMarked())) {
                // if we're successful, or we failed but prev is not marked - so it means someone else was successful
                // then we're done with loop
                break;
            }
        }

    }

    private void updateLastChild(List<Chunk<K, V>> engaged, List<Chunk<K, V>> children) {
        Chunk<K, V> lastEngaged = engaged.get(engaged.size() - 1);
        Chunk<K, V> nextToLast = lastEngaged.markAndGetNext(); // also marks last engaged chunk as deleted
        Chunk<K, V> lastChild = children.get(children.size() - 1);

        lastChild.next.compareAndSet(null, nextToLast, false, false);
    }

    private void updateIndexAndNormalize(List<Chunk<K, V>> engagedChunks, List<Chunk<K, V>> children) {
        Iterator<Chunk<K, V>> iterEngaged = engagedChunks.iterator();
        Iterator<Chunk<K, V>> iterChildren = children.iterator();

        Chunk<K, V> firstEngaged = iterEngaged.next();
        Chunk<K, V> firstChild = iterChildren.next();

        // need to make the new chunks available, before removing old chunks
        skiplist.replace(firstEngaged.minKey, firstEngaged, firstChild);

        // remove all old chunks from index.
        while (iterEngaged.hasNext()) {
            Chunk engagedToRemove = iterEngaged.next();
            skiplist.remove(engagedToRemove.minKey, engagedToRemove); // conditional remove is used
        }

        // now after removing old chunks we can start normalizing
        firstChild.normalize();

        // for simplicity -  naive lock implementation
        // can be implemented without locks using versions on next pointer in skiplist
        while (iterChildren.hasNext()) {
            Chunk<K, V> childToAdd;
            synchronized (childToAdd = iterChildren.next()) {
                if (childToAdd.state() == Chunk.State.INFANT) { // make sure it wasn't add before
                    skiplist.putIfAbsent(childToAdd.minKey, childToAdd);
                    childToAdd.normalize();
                }
                // has a built in fence, so no need to add one here
            }
        }
    }

    private boolean inTheMiddleOfRebalance(Chunk<K, V> c) {
        State state = c.state();
        if (state == State.INFANT) {
            // the infant is already connected so rebalancer won't add this put
            rebalance(c.creator());
            return true;
        }
        if (state == State.FROZEN || state == State.RELEASED) {
            rebalance(c);
            return true;
        }
        return false;
    }

    private boolean finalizeDeletion(Chunk<K, V> c, LookUp lookUp) {
        if (lookUp != null) {
            if (c.finalizeDeletion(lookUp) == RETRY) {
                rebalance(c);
                return true;
            }
        }
        return false;
    }

    /*-------------- OakMap Methods --------------*/

    void zcPut(K key, V value) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }
        while (true) {
            Chunk<K, V> c = findChunk(key); // find chunk matching key
            Chunk.LookUp lookUp = c.lookUp(key);

            if (lookUp != null && lookUp.valueSlice != null) {
                if (updateVersionAfterLinking(c, lookUp)) {
                    continue;
                }
                if (operator.put(c, lookUp, value, valueSerializer, memoryManager) == TRUE) {
                    return;
                }
                continue;
            }

            if (inTheMiddleOfRebalance(c) || finalizeDeletion(c, lookUp)) {
                continue;
            }
            int oldVersion = lookUp == null ? INVALID_VERSION : -Math.abs(lookUp.version);

            // Now the entry's version is invalid

            int ei = -1;
            if (lookUp != null) {
                ei = lookUp.entryIndex;
                assert ei > 0;
            }

            // Lookup is not valid anymore, only its entry index

            if (ei == -1) {
                ei = c.allocateEntryAndKey(key);
                if (ei == -1) {
                    rebalance(c);
                    continue;
                }
                int prevEi = c.linkEntry(ei, key);
                if (prevEi != ei) {
                    ei = prevEi;
                }
            }

            int[] version = new int[1];
            long newValueStats = c.writeValueOffHeap(value, version); // write value in place

            Chunk.OpData opData = new Chunk.OpData(ei, NULL_VALUE, newValueStats,
                    oldVersion, version[0]);

            if (!c.publish()) {
                memoryManager.releaseSlice(c.buildValueSlice(newValueStats));
                rebalance(c);
                continue;
            }

            if (c.linkValue(opData) != TRUE) {
                memoryManager.releaseSlice(c.buildValueSlice(newValueStats));
                c.unpublish();
            } else {
                c.unpublish();
                checkRebalance(c);
                return;
            }
        }
    }

    V nonZcPut(K key, V value, Function<ByteBuffer, V> transformer) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }

        while (true) {
            Chunk<K, V> c = findChunk(key); // find chunk matching key
            Chunk.LookUp lookUp = c.lookUp(key);
            if (lookUp != null && lookUp.valueSlice != null) {
                if (updateVersionAfterLinking(c, lookUp)) {
                    continue;
                }
                V v = null;
                if (transformer != null) {
                    // Todo: Not atomic!
                    AbstractMap.SimpleEntry<NovaValueUtils.Result, V> res = operator.transform(lookUp.valueSlice,
                            transformer, lookUp.version);
                    if (res.getKey() == RETRY) {
                        continue;
                    }
                    v = res.getValue();
                }
                if (operator.put(c, lookUp, value, valueSerializer, memoryManager) != TRUE) {
                    continue;
                }
                return v;
            }

            // if chunk is frozen or infant, we can't add to it
            // we need to help rebalancer first, then proceed
            if (inTheMiddleOfRebalance(c) || finalizeDeletion(c, lookUp)) {
                continue;
            }
            int oldVersion = lookUp == null ? INVALID_VERSION : -Math.abs(lookUp.version);

            int ei = -1;
            if (lookUp != null) {
                ei = lookUp.entryIndex;
                assert ei > 0;
            }

            if (ei == -1) {
                ei = c.allocateEntryAndKey(key);
                if (ei == -1) {
                    rebalance(c);
                    continue;
                }
                int prevEi = c.linkEntry(ei, key);
                if (prevEi != ei) {
                    ei = prevEi;
                }
            }

            int[] version = new int[1];
            long newValueStats = c.writeValueOffHeap(value, version); // write value in place

            Chunk.OpData opData = new Chunk.OpData(ei, NULL_VALUE, newValueStats, oldVersion,
                    version[0]);

            // publish put
            if (!c.publish()) {
                memoryManager.releaseSlice(c.buildValueSlice(newValueStats));
                rebalance(c);
                continue;
            }

            if (c.linkValue(opData) != TRUE) {
                memoryManager.releaseSlice(c.buildValueSlice(newValueStats));
                c.unpublish();
            } else {
                c.unpublish();
                checkRebalance(c);
                return null;
            }
        }
    }

    boolean zcPutIfAbsent(K key, V value) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }

        while (true) {
            Chunk<K, V> c = findChunk(key); // find chunk matching key
            Chunk.LookUp lookUp = c.lookUp(key);

            if (lookUp != null && lookUp.valueSlice != null) {
                if (c.completeLinking(lookUp) == INVALID_VERSION) {
                    rebalance(c);
                    continue;
                }
                return false;
            }

            if (inTheMiddleOfRebalance(c) || finalizeDeletion(c, lookUp)) {
                continue;
            }
            int oldVersion = lookUp == null ? INVALID_VERSION : -Math.abs(lookUp.version);

            // Now the entry's version is invalid

            int myEntryIndex = -1;
            if (lookUp != null) {
                myEntryIndex = lookUp.entryIndex;
                assert myEntryIndex > 0;
            }

            if (myEntryIndex == -1) {
                myEntryIndex = c.allocateEntryAndKey(key);
                if (myEntryIndex == -1) {
                    rebalance(c);
                    continue;
                }
                int prevEi = c.linkEntry(myEntryIndex, key);
                if (prevEi != myEntryIndex) {
                    if (c.getValueStats(prevEi) != NULL_VALUE) {
                        // Todo: maybe return false?
                        continue;
                    } else {
                        myEntryIndex = prevEi;
                    }
                }
            }

            int[] version = new int[1];
            long newValueStats = c.writeValueOffHeap(value, version); // write value in place

            Chunk.OpData opData = new Chunk.OpData(myEntryIndex, NULL_VALUE, newValueStats,
                    oldVersion, version[0]);

            if (!c.publish()) {
                memoryManager.releaseSlice(c.buildValueSlice(newValueStats));
                rebalance(c);
                continue;
            }

            if (c.linkValue(opData) != TRUE) {
                memoryManager.releaseSlice(c.buildValueSlice(newValueStats));
                c.unpublish();
            } else {
                c.unpublish();
                checkRebalance(c);
                return true;
            }
        }
    }

    V putIfAbsent(K key, V value, Function<ByteBuffer, V> transformer) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }

        while (true) {
            Chunk<K, V> c = findChunk(key); // find chunk matching key
            Chunk.LookUp lookUp = c.lookUp(key);

            if (lookUp != null && lookUp.valueSlice != null) {
                if (updateVersionAfterLinking(c, lookUp)) {
                    continue;
                }
                AbstractMap.SimpleEntry<NovaValueUtils.Result, V> res = operator.transform(lookUp.valueSlice,
                        transformer, lookUp.version);
                if (res.getKey() == TRUE) {
                    return res.getValue();
                }
                continue;
            }

            // if chunk is frozen or infant, we can't add to it
            // we need to help rebalancer first, then proceed
            if (inTheMiddleOfRebalance(c) || finalizeDeletion(c, lookUp)) {
                continue;
            }
            int oldVersion = lookUp == null ? INVALID_VERSION : -Math.abs(lookUp.version);


            int ei = -1;
            if (lookUp != null) {
                ei = lookUp.entryIndex;
                assert ei > 0;
            }

            // TODO - this can be the else clause of the previous if
            if (ei == -1) {
                ei = c.allocateEntryAndKey(key);
                if (ei == -1) {
                    rebalance(c);
                    return putIfAbsent(key, value, transformer);
                }
                int prevEi = c.linkEntry(ei, key);
                if (prevEi != ei) {
                    if (c.getValueStats(prevEi) != 0) {
                        continue;
                    } else {
                        ei = prevEi;
                    }
                }
            }

            int[] version = new int[1];
            long newValueStats = c.writeValueOffHeap(value, version); // write value in place
            Chunk.OpData opData = new Chunk.OpData(ei, NULL_VALUE, newValueStats,
                    oldVersion, version[0]);

            // publish put
            if (!c.publish()) {
                memoryManager.releaseSlice(c.buildValueSlice(newValueStats));
                rebalance(c);
                return putIfAbsent(key, value, transformer);
            }

            if (c.linkValue(opData) != TRUE) {
                memoryManager.releaseSlice(c.buildValueSlice(newValueStats));
                c.unpublish();
            } else {
                c.unpublish();
                checkRebalance(c);
                return null;
            }
        }
    }

    boolean putIfAbsentComputeIfPresent(K key, V value, Consumer<OakWBuffer> computer) {
        if (key == null || value == null || computer == null) {
            throw new NullPointerException();
        }

        while (true) {
            Chunk<K, V> c = findChunk(key); // find chunk matching key
            Chunk.LookUp lookUp = c.lookUp(key);
            if (lookUp != null && lookUp.valueSlice != null) {
                if (updateVersionAfterLinking((Chunk<K, V>) c, lookUp)) {
                    continue;
                }
                NovaValueUtils.Result res = operator.compute(lookUp.valueSlice, computer, lookUp.version);
                if (res == TRUE) {
                    // compute was successful and handle wasn't found deleted; in case
                    // this handle was already found as deleted, continue to construct another handle
                    return false;
                } else if (res == RETRY) {
                    continue;
                }
            }

            if (inTheMiddleOfRebalance(c) || finalizeDeletion(c, lookUp)) {
                continue;
            }
            int oldVersion = lookUp == null ? INVALID_VERSION : -Math.abs(lookUp.version);

            int ei = -1;
            if (lookUp != null) {
                ei = lookUp.entryIndex;
                assert ei > 0;
            }

            if (ei == -1) {
                ei = c.allocateEntryAndKey(key);
                if (ei == -1) {
                    rebalance(c);
                    continue;
                }
                int prevEi = c.linkEntry(ei, key);
                if (prevEi != ei) {
                    if (c.getValueStats(prevEi) != NULL_VALUE) {
                        continue;
                    } else {
                        ei = prevEi;
                    }
                }
            }

            int[] version = new int[1];
            long newValueStats = c.writeValueOffHeap(value, version); // write value in place

            Chunk.OpData opData = new Chunk.OpData(ei, NULL_VALUE, newValueStats,
                    oldVersion, version[0]);

            if (!c.publish()) {
                memoryManager.releaseSlice(c.buildValueSlice(newValueStats));
                rebalance(c);
                continue;
            }

            if (c.linkValue(opData) != TRUE) {
                memoryManager.releaseSlice(c.buildValueSlice(newValueStats));
                c.unpublish();
            } else {
                c.unpublish();
                checkRebalance(c);
                return true;
            }
        }
    }

    private boolean updateVersionAfterLinking(Chunk<K, V> c, LookUp lookUp) {
        int valueVersion = c.completeLinking(lookUp);
        if (valueVersion == INVALID_VERSION) {
            rebalance(c);
            return true;
        }
        lookUp.version = valueVersion;
        return false;
    }

    boolean zcRemove(K key) {
        if (key == null) {
            throw new NullPointerException();
        }

        while (true) {
            Chunk<K, V> c = findChunk(key); // find chunk matching key
            Chunk.LookUp lookUp = c.lookUp(key);

            if (lookUp == null) {
                return false;
            } else if (lookUp.valueSlice == null) {
                if (c.finalizeDeletion(lookUp) != RETRY) {
                    return false;
                }
                continue;
            }

            if (inTheMiddleOfRebalance(c) || updateVersionAfterLinking(c, lookUp)) {
                continue;
            }

            if (c.completeLinking(lookUp) == INVALID_VERSION) {
                rebalance(c);
                continue;
            }

            NovaValueUtils.Result result = operator.remove(lookUp.valueSlice, memoryManager, lookUp.version);
            if (result == RETRY) {
                continue;
            }
            if (c.finalizeDeletion(lookUp) == RETRY) {
                zcRemove(key);
            }
            return result == TRUE;
        }
    }

    V nonZcRemove(K key, V oldValue, Function<ByteBuffer, V> transformer) {
        if (key == null) {
            throw new NullPointerException();
        }

        while (true) {
            Chunk<K, V> c = findChunk(key); // find chunk matching key
            Chunk.LookUp lookUp = c.lookUp(key);

            if (lookUp == null) {
                return null;
            } else if (lookUp.valueSlice == null) {
                if (c.finalizeDeletion(lookUp) != RETRY) {
                    return null;
                }
                continue;
            }

            if (inTheMiddleOfRebalance(c) || updateVersionAfterLinking(c, lookUp)) {
                continue;
            }

            if (c.completeLinking(lookUp) == INVALID_VERSION) {
                rebalance(c);
                continue;
            }

            // Todo: Not Atomic!
            AbstractMap.SimpleEntry<NovaValueUtils.Result, V> resultVSimpleEntry =
                    operator.transform(lookUp.valueSlice, transformer, lookUp.version);
            if (resultVSimpleEntry.getKey() == RETRY) {
                continue;
            }
            V previousValue = resultVSimpleEntry.getValue();
            if (oldValue != null && !oldValue.equals(previousValue)) {
                return null;
            }

            NovaValueUtils.Result result = operator.remove(lookUp.valueSlice, memoryManager, lookUp.version);
            if (result == RETRY) {
                continue;
            }
            if (c.finalizeDeletion(lookUp) == RETRY) {
                rebalance(c);
                nonZcRemove(key, oldValue, transformer);
            }
            return result == TRUE ? previousValue : null;
        }
    }

    OakRBuffer zcGet(K key) {
        if (key == null) {
            throw new NullPointerException();
        }
        while (true) {
            Chunk<K, V> c = findChunk(key); // find chunk matching key
            Chunk.LookUp lookUp = c.lookUp(key);
            if (lookUp == null || lookUp.valueSlice == null) {
                return null;
            }
            if (c.completeLinking(lookUp) == INVALID_VERSION) {
                rebalance(c);
                continue;
            }
            long keyStats = c.readKeyStats(lookUp.entryIndex);
            return new OakRValueBufferImpl(lookUp.valueStats, lookUp.version, keyStats, operator, memoryManager, this);
        }
    }

    boolean computeIfPresent(K key, Consumer<OakWBuffer> computer) {
        if (key == null || computer == null) {
            throw new NullPointerException();
        }

        while (true) {
            Chunk<K, V> c = findChunk(key); // find chunk matching key
            Chunk.LookUp lookUp = c.lookUp(key);

            if (lookUp != null && lookUp.valueSlice != null) {
                if (c.completeLinking(lookUp) == INVALID_VERSION) {
                    rebalance(c);
                    continue;
                }
                NovaValueUtils.Result res = operator.compute(lookUp.valueSlice, computer, lookUp.version);
                if (res == TRUE) {
                    // compute was successful and handle wasn't found deleted; in case
                    // this handle was already found as deleted, continue to construct another handle
                    return true;
                } else if (res == RETRY) {
                    continue;
                }
            }
            return false;
        }
    }

    <T> T getValueTransformation(K key, Function<ByteBuffer, T> transformer) {
        if (key == null || transformer == null) {
            throw new NullPointerException();
        }

        while (true) {
            Chunk<K, V> c = findChunk(key); // find chunk matching key
            Chunk.LookUp lookUp = c.lookUp(key);
            if (lookUp == null || lookUp.valueSlice == null) {
                return null;
            }

            if (updateVersionAfterLinking(c, lookUp)) {
                continue;
            }
            AbstractMap.SimpleEntry<NovaValueUtils.Result, T> res = operator.transform(lookUp.valueSlice, transformer,
                    lookUp.version);
            if (res.getKey() == RETRY) {
                continue;
            }
            return res.getValue();
        }
    }

    LookUp getValueFromIndex(long keyStats) {
        K deserializedKey = keySerializer.deserialize(getKeyBufferFromStats(keyStats));
        while (true) {
            Chunk<K, V> c = findChunk(deserializedKey); // find chunk matching key
            Chunk.LookUp lookUp = c.lookUp(deserializedKey);
            if (lookUp == null || lookUp.valueSlice == null) {
                return null;
            }

            if (updateVersionAfterLinking(c, lookUp)) {
                continue;
            }
            return lookUp;
        }
    }

    private <T> T getValueTransformation(ByteBuffer key, Function<ByteBuffer, T> transformer) {
        K deserializedKey = keySerializer.deserialize(key);
        return getValueTransformation(deserializedKey, transformer);
    }

    <T> T getKeyTransformation(K key, Function<ByteBuffer, T> transformer) {
        if (key == null || transformer == null) {
            throw new NullPointerException();
        }

        Chunk<K, V> c = findChunk(key); // find chunk matching key
        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp == null || lookUp.valueSlice == null || lookUp.entryIndex == -1) {
            return null;
        }
        ByteBuffer serializedKey = c.readKey(lookUp.entryIndex).slice();
        return transformer.apply(serializedKey);
    }

    ByteBuffer getKey(K key) {
        if (key == null) {
            throw new NullPointerException();
        }

        Chunk<K, V> c = findChunk(key);
        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp == null || lookUp.valueSlice == null || lookUp.entryIndex == -1) {
            return null;
        }
        return c.readKey(lookUp.entryIndex).slice();
    }

    ByteBuffer getMinKey() {
        Chunk<K, V> c = skiplist.firstEntry().getValue();
        return c.readMinKey().slice();
    }

    <T> T getMinKeyTransformation(Function<ByteBuffer, T> transformer) {
        if (transformer == null) {
            throw new NullPointerException();
        }

        Chunk<K, V> c = skiplist.firstEntry().getValue();
        ByteBuffer serializedMinKey = c.readMinKey();

        return (serializedMinKey != null) ? transformer.apply(serializedMinKey) : null;
    }

    ByteBuffer getMaxKey() {
        Chunk<K, V> c = skiplist.lastEntry().getValue();
        Chunk<K, V> next = c.next.getReference();
        // since skiplist isn't updated atomically in split/compaction, the max key might belong in the next chunk
        // we need to iterate the chunks until we find the last one
        while (next != null) {
            c = next;
            next = c.next.getReference();
        }

        return c.readMaxKey().slice();
    }

    <T> T getMaxKeyTransformation(Function<ByteBuffer, T> transformer) {
        if (transformer == null) {
            throw new NullPointerException();
        }

        Chunk<K, V> c = skiplist.lastEntry().getValue();
        Chunk<K, V> next = c.next.getReference();
        // since skiplist isn't updated atomically in split/compaction, the max key might belong in the next chunk
        // we need to iterate the chunks until we find the last one
        while (next != null) {
            c = next;
            next = c.next.getReference();
        }
        ByteBuffer serializedMaxKey = c.readMaxKey();

        return (serializedMaxKey != null) ? transformer.apply(serializedMaxKey) : null;
    }

    // encapsulates finding of the chunk in the skip list and later chunk list traversal
    private Chunk<K, V> findChunk(Object key) {
        Chunk<K, V> c = skiplist.floorEntry(key).getValue();
        c = iterateChunks(c, key);
        return c;
    }

    V replace(K key, V value, Function<ByteBuffer, V> valueDeserializeTransformer) {
        Chunk<K, V> c = findChunk(key); // find chunk matching key
        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp == null || lookUp.valueSlice == null) {
            return null;
        }
        // will return null if handle was deleted between prior lookup and the next call
        AbstractMap.SimpleEntry<NovaValueUtils.Result, V> result = operator.exchange(c, lookUp, value,
                valueDeserializeTransformer, valueSerializer, memoryManager);
        if (result.getKey() == RETRY) {
            return replace(key, value, valueDeserializeTransformer);
        }
        return result.getValue();
    }

    boolean replace(K key, V oldValue, V newValue, Function<ByteBuffer, V> valueDeserializeTransformer) {
        Chunk<K, V> c = findChunk(key); // find chunk matching key
        Chunk.LookUp lookUp = c.lookUp(key);
        if (lookUp == null || lookUp.valueSlice == null) {
            return false;
        }

        // res can be null if handle was deleted between lookup and the next call
        NovaValueUtils.Result res = operator.compareExchange(c, lookUp, oldValue, newValue,
                valueDeserializeTransformer, valueSerializer, memoryManager);
        if (res == RETRY) {
            return replace(key, oldValue, newValue, valueDeserializeTransformer);
        }
        return res == TRUE;
    }

    Map.Entry<K, V> lowerEntry(K key) {
        Map.Entry<Object, Chunk<K, V>> lowerChunkEntry = skiplist.lowerEntry(key);
        if (lowerChunkEntry == null) {
            /* we were looking for the minimal key */
            return new AbstractMap.SimpleImmutableEntry<>(null, null);
        }

        Chunk<K, V> c = lowerChunkEntry.getValue();
        /* Iterate chunk to find prev(key) */
        Chunk.AscendingIter chunkIter = c.ascendingIter();
        int prevIndex = chunkIter.next();

        while (chunkIter.hasNext()) {
            int nextIndex = chunkIter.next();
            int cmp = comparator.compare(c.readKey(nextIndex), key);
            if (cmp >= 0) {
                break;
            }
            prevIndex = nextIndex;
        }

        /* Edge case: we're looking for the lowest key in the map and it's still greater than minkey
            (in which  case prevKey == key) */
        ByteBuffer prevKey = c.readKey(prevIndex);
        if (comparator.compare(prevKey, key) == 0) {
            return new AbstractMap.SimpleImmutableEntry<>(null, null);
        }

        // TODO: No lock?
        return new AbstractMap.SimpleImmutableEntry<>(
                keySerializer.deserialize(prevKey),
                valueSerializer.deserialize(operator.getActualValue(c.getValueSlice(prevIndex)).asReadOnlyBuffer()));
    }

    /*-------------- Iterators --------------*/

    private Slice getValueSliceFromStats(long valueStats) {
        int[] valueArray = longToInts(valueStats);
        if (Chunk.isValueThere(valueArray)) {
            return null;
        }
        return memoryManager.getSliceFromBlockID(valueArray[0] >>> VALUE_BLOCK_SHIFT, valueArray[1],
                valueArray[0] & VALUE_LENGTH_MASK);
    }

    private ByteBuffer getKeyBufferFromStats(long keyStats) {
        int[] keyArray = longToInts(keyStats);
        return memoryManager.getByteBufferFromBlockID(keyArray[0] >>> KEY_BLOCK_SHIFT, keyArray[1],
                keyArray[0] & KEY_LENGTH_MASK);
    }

    private static class IteratorState<K, V> {

        private Chunk<K, V> chunk;
        private Chunk.ChunkIter chunkIter;
        private int index;

        public void set(Chunk<K, V> chunk, Chunk.ChunkIter chunkIter, int index) {
            this.chunk = chunk;
            this.chunkIter = chunkIter;
            this.index = index;
        }

        private IteratorState(Chunk<K, V> nextChunk, Chunk.ChunkIter nextChunkIter, int nextIndex) {
            this.chunk = nextChunk;
            this.chunkIter = nextChunkIter;
            this.index = nextIndex;
        }

        Chunk<K, V> getChunk() {
            return chunk;
        }

        Chunk.ChunkIter getChunkIter() {
            return chunkIter;
        }

        public int getIndex() {
            return index;
        }


        public static <K, V> IteratorState<K, V> newInstance(Chunk<K, V> nextChunk, Chunk.ChunkIter nextChunkIter) {
            return new IteratorState<>(nextChunk, nextChunkIter, Chunk.NONE);
        }

    }

    /**
     * Base of iterator classes:
     */
    abstract class Iter<T> implements Iterator<T> {

        private K lo;

        /**
         * upper bound key, or null if to end
         */
        private K hi;
        /**
         * inclusion flag for lo
         */
        private boolean loInclusive;
        /**
         * inclusion flag for hi
         */
        private boolean hiInclusive;
        /**
         * direction
         */
        private final boolean isDescending;

        /**
         * the next node to return from next();
         */
        private IteratorState<K, V> state;

        /**
         * Initializes ascending iterator for entire range.
         */
        Iter(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
            if (lo != null && hi != null &&
                    comparator.compare(lo, hi) > 0) {
                throw new IllegalArgumentException("inconsistent range");
            }

            this.lo = lo;
            this.loInclusive = loInclusive;
            this.hi = hi;
            this.hiInclusive = hiInclusive;
            this.isDescending = isDescending;

            initState(isDescending, lo, loInclusive, hi, hiInclusive);

        }

        boolean tooLow(Object key) {
            int c;
            return (lo != null && ((c = comparator.compare(key, lo)) < 0 ||
                    (c == 0 && !loInclusive)));
        }

        boolean tooHigh(Object key) {
            int c;
            return (hi != null && ((c = comparator.compare(key, hi)) > 0 ||
                    (c == 0 && !hiInclusive)));
        }


        boolean inBounds(Object key) {
            if (!isDescending) {
                return !tooHigh(key);
            } else {
                return !tooLow(key);
            }
        }

        public final boolean hasNext() {
            return (state != null);
        }


        private void initAfterRebalance() {
            //TODO - refactor to use ByeBuffer without deserializing.
            K nextKey = keySerializer.deserialize(state.getChunk().readKey(state.getIndex()).slice());

            if (isDescending) {
                hiInclusive = true;
                hi = nextKey;
            } else {
                loInclusive = true;
                lo = nextKey;
            }

            // Update the state to point to last returned key.
            initState(isDescending, lo, loInclusive, hi, hiInclusive);

            if (state == null) {
                throw new ConcurrentModificationException();
            }
        }


        // the actual next()
        abstract public T next();

        /**
         * Advances next to higher entry.
         * Return previous index
         *
         * @return
         */
        Map.Entry<Long, Map.Entry<Integer, Long>> advance(boolean needsValue) {

            if (state == null) {
                throw new NoSuchElementException();
            }

            Chunk.State chunkState = state.getChunk().state();

            if (chunkState == Chunk.State.RELEASED) {
                initAfterRebalance();
            }

            // Key is duplicated
            long keyStats = state.getChunk().readKeyStats(state.getIndex());
            long valueStats;
            int valueVersion;
            Map.Entry<Integer, Long> value = null;
            if (needsValue) {
                do {
                    valueVersion = state.getChunk().getVersion(state.getIndex());
                    valueStats = state.getChunk().getValueStats(state.getIndex());
                } while (valueVersion != state.getChunk().getVersion(state.getIndex()));
                if (valueStats != NULL_VALUE) {
                    valueVersion = state.getChunk().completeLinking(new LookUp(null, valueStats, state.getIndex(),
                            valueVersion));
                    if (valueVersion == INVALID_VERSION) {
                        advanceState();
                        return advance(true);
                    }
                }
                value = new AbstractMap.SimpleImmutableEntry<>(valueVersion, valueStats);
            }
            advanceState();
            return new AbstractMap.SimpleImmutableEntry<>(keyStats, value);
        }

        private void initState(boolean isDescending, K lowerBound, boolean lowerInclusive,
                               K upperBound, boolean upperInclusive) {

            Chunk.ChunkIter nextChunkIter;
            Chunk<K, V> nextChunk;

            if (!isDescending) {
                if (lowerBound != null) {
                    nextChunk = skiplist.floorEntry(lowerBound).getValue();
                } else {
                    nextChunk = skiplist.floorEntry(minKey).getValue();
                }
                if (nextChunk != null) {
                    nextChunkIter = lowerBound != null ?
                            nextChunk.ascendingIter(lowerBound, lowerInclusive) : nextChunk.ascendingIter();
                } else {
                    state = null;
                    return;
                }
            } else {
                nextChunk = upperBound != null ? skiplist.floorEntry(upperBound).getValue()
                        : skiplist.lastEntry().getValue();
                if (nextChunk != null) {
                    nextChunkIter = upperBound != null ?
                            nextChunk.descendingIter(upperBound, upperInclusive) : nextChunk.descendingIter();
                } else {
                    state = null;
                    return;
                }
            }

            //Init state, not valid yet, must move forward
            state = IteratorState.newInstance(nextChunk, nextChunkIter);
            advanceState();
        }

        private Chunk<K, V> getNextChunk(Chunk<K, V> current) {
            if (!isDescending) {
                return current.next.getReference();
            } else {
                ByteBuffer serializedMinKey = current.minKey;
                Map.Entry<Object, Chunk<K, V>> entry = skiplist.lowerEntry(serializedMinKey);
                if (entry == null) {
                    return null;
                } else {
                    return entry.getValue();
                }
            }
        }

        private Chunk.ChunkIter getChunkIter(Chunk<K, V> current) {
            if (!isDescending) {
                return current.ascendingIter();
            } else {
                return current.descendingIter();
            }
        }

        private void advanceState() {

            Chunk<K, V> chunk = state.getChunk();
            Chunk.ChunkIter chunkIter = state.getChunkIter();

            while (!chunkIter.hasNext()) { // chunks can have only removed keys
                chunk = getNextChunk(chunk);
                if (chunk == null) {
                    //End of iteration
                    state = null;
                    return;
                }
                chunkIter = getChunkIter(chunk);
            }

            int nextIndex = chunkIter.next();
            state.set(chunk, chunkIter, nextIndex);

            ByteBuffer key = state.getChunk().readKey(state.getIndex());
            if (!inBounds(key)) {
                state = null;
                return;
            }
        }
    }

    class ValueIterator extends Iter<OakRBuffer> {

        private final InternalOakMap<K, V> internalOakMap;

        ValueIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending, InternalOakMap<K,
                V> internalOakMap) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
            this.internalOakMap = internalOakMap;
        }

        @Override
        public OakRBuffer next() {
            Map.Entry<Long, Map.Entry<Integer, Long>> nextItem = advance(true);
            long keyStats = nextItem.getKey();
            long valueStats = nextItem.getValue().getValue();
            int version = nextItem.getValue().getKey();
            if (longToInts(valueStats)[0] == INVALID_BLOCK_ID) {
                return null;
            }

            return new OakRValueBufferImpl(valueStats, version, keyStats, operator, memoryManager, internalOakMap);
        }
    }

    class ValueTransformIterator<T> extends Iter<T> {

        final Function<ByteBuffer, T> transformer;

        ValueTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending,
                               Function<ByteBuffer, T> transformer) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
            this.transformer = transformer;
        }

        public T next() {
            Map.Entry<Long, Map.Entry<Integer, Long>> nextItem = advance(true);
            long keyStats = nextItem.getKey();
            long valueStats = nextItem.getValue().getValue();
            Slice valueSlice = getValueSliceFromStats(valueStats);
            int version = nextItem.getValue().getKey();
            if (valueSlice == null) {
                return null;
            }
            AbstractMap.SimpleEntry<NovaValueUtils.Result, T> res = operator.transform(valueSlice, transformer,
                    version);
            return (res.getKey() == RETRY) ? getValueTransformation(getKeyBufferFromStats(keyStats), transformer)
                    : res.getValue();
        }
    }

    class EntryIterator extends Iter<Map.Entry<OakRBuffer, OakRBuffer>> {

        private final InternalOakMap<K, V> internalOakMap;

        EntryIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending, InternalOakMap<K,
                V> internalOakMap) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
            this.internalOakMap = internalOakMap;
        }

        public Map.Entry<OakRBuffer, OakRBuffer> next() {
            Map.Entry<Long, Map.Entry<Integer, Long>> nextItem = advance(true);
            long keyStats = nextItem.getKey();
            long valueStats = nextItem.getValue().getValue();
            int version = nextItem.getValue().getKey();
            if (Chunk.isValueThere(longToInts(valueStats))) {
                return null;
            }
            return new AbstractMap.SimpleImmutableEntry<>(
                    new OakRKeyBufferImpl(keyStats, memoryManager),
                    new OakRValueBufferImpl(valueStats, version, keyStats, operator, memoryManager, internalOakMap));
        }
    }

    class EntryTransformIterator<T> extends Iter<T> {

        final Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer;

        EntryTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending,
                               Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
            assert (transformer != null);
            this.transformer = transformer;
        }

        public T next() {
            Map.Entry<Long, Map.Entry<Integer, Long>> nextItem = advance(true);
            long keyStats = nextItem.getKey();
            long valueStats = nextItem.getValue().getValue();
            Slice valueSlice = getValueSliceFromStats(valueStats);
            int version = nextItem.getValue().getKey();
            if (valueSlice == null) {
                return null;
            }
            NovaValueUtils.Result res = operator.lockRead(valueSlice, version);
            ByteBuffer serializedValue;
            if (res == FALSE) {
                return null;
            } else if (res == RETRY) {
                while (true) {
                    LookUp lookUp = getValueFromIndex(keyStats);
                    if (lookUp == null || lookUp.valueSlice == null) {
                        return null;
                    }
                    res = operator.lockRead(lookUp.valueSlice, lookUp.version);
                    if (res == TRUE) {
                        valueStats = lookUp.valueStats;
                        valueSlice = lookUp.valueSlice;
                        version = lookUp.version;
                        break;
                    } else if (res == FALSE) {
                        return null;
                    }
                }
            }
            serializedValue = operator.getActualValue(valueSlice.readOnly());
            Map.Entry<ByteBuffer, ByteBuffer> entry =
                    new AbstractMap.SimpleEntry<>(getKeyBufferFromStats(keyStats).asReadOnlyBuffer(), serializedValue);

            T transformation = transformer.apply(entry);
            valueSlice = getValueSliceFromStats(valueStats);
            operator.unlockRead(valueSlice, version);
            return transformation;
        }
    }

    class KeyIterator extends Iter<OakRBuffer> {

        KeyIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
        }

        @Override
        public OakRBuffer next() {

            Map.Entry<Long, Map.Entry<Integer, Long>> pair = advance(false);
            return new OakRKeyBufferImpl(pair.getKey(), memoryManager);

        }
    }

    class KeyTransformIterator<T> extends Iter<T> {

        final Function<ByteBuffer, T> transformer;

        KeyTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending,
                             Function<ByteBuffer, T> transformer) {
            super(lo, loInclusive, hi, hiInclusive, isDescending);
            this.transformer = transformer;
        }

        public T next() {
            Map.Entry<Long, Map.Entry<Integer, Long>> pair = advance(false);
            return transformer.apply(getKeyBufferFromStats(pair.getKey()).asReadOnlyBuffer());
        }
    }

    // Factory methods for iterators

    Iterator<OakRBuffer> valuesBufferViewIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive,
                                                  boolean isDescending) {
        return new ValueIterator(lo, loInclusive, hi, hiInclusive, isDescending, this);
    }

    Iterator<Map.Entry<OakRBuffer, OakRBuffer>> entriesBufferViewIterator(K lo, boolean loInclusive, K hi,
                                                                          boolean hiInclusive, boolean isDescending) {
        return new EntryIterator(lo, loInclusive, hi, hiInclusive, isDescending, this);
    }

    Iterator<OakRBuffer> keysBufferViewIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive,
                                                boolean isDescending) {
        return new KeyIterator(lo, loInclusive, hi, hiInclusive, isDescending);
    }

    <T> Iterator<T> valuesTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive,
                                            boolean isDescending, Function<ByteBuffer, T> transformer) {
        return new ValueTransformIterator<>(lo, loInclusive, hi, hiInclusive, isDescending, transformer);
    }

    <T> Iterator<T> entriesTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive,
                                             boolean isDescending,
                                             Function<Map.Entry<ByteBuffer, ByteBuffer>, T> transformer) {
        return new EntryTransformIterator<>(lo, loInclusive, hi, hiInclusive, isDescending, transformer);
    }

    <T> Iterator<T> keysTransformIterator(K lo, boolean loInclusive, K hi, boolean hiInclusive, boolean isDescending,
                                          Function<ByteBuffer, T> transformer) {
        return new KeyTransformIterator<>(lo, loInclusive, hi, hiInclusive, isDescending, transformer);
    }

}
