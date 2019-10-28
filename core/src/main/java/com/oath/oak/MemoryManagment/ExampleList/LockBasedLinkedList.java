package com.oath.oak.MemoryManagment.ExampleList;

import com.oath.oak.MemoryManagment.NovaManager;
import com.oath.oak.MemoryManagment.OffHeapSlice;
import com.oath.oak.MemoryManagment.Result;
import com.oath.oak.OakComparator;
import com.oath.oak.OakSerializer;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.oath.oak.MemoryManagment.Result.TRUE;

public class LockBasedLinkedList<K, V> {
    static class Node<K, V> {
        private OffHeapSlice key;
        private K actualKey;
        private OffHeapSlice value;
        private AtomicReference<Node<K, V>> next;
        boolean deleted;
        private Lock nodeLock = new ReentrantLock();

        Node(K key, OakSerializer<K> keySerializer, Node<K, V> next, NovaManager manager) {
            this.actualKey = key;
            this.key = manager.allocateSlice(keySerializer.calculateSize(key));
            this.key.put(key, keySerializer);
            this.value = null;
            this.next = new AtomicReference<>(next);
            this.deleted = false;
        }

        Node(K key, OakSerializer<K> keySerializer, V value, OakSerializer<V> valueSerializer, Node<K, V> next,
             NovaManager manager) {
            this.actualKey = key;
            this.key = manager.allocateSlice(keySerializer.calculateSize(key));
            this.key.put(key, keySerializer);
            this.value = manager.allocateSlice(valueSerializer.calculateSize(value));
            this.value.put(value, valueSerializer);
            this.next = new AtomicReference<>(next);
            this.deleted = false;
        }

        void lock() {
            this.nodeLock.lock();
        }

        void unlock() {
            this.nodeLock.unlock();
        }

        public OffHeapSlice getKey() {
            return key;
        }

        public OffHeapSlice getValue() {
            return value;
        }

        public Node<K, V> getNext() {
            return this.next.get();
        }

        public void setNext(Node<K, V> next) {
            this.next.set(next);
        }
    }

    private static class Window<K, V> {
        Node<K, V> pred;
        Node<K, V> curr;
        boolean result;

        Window(Node<K, V> pred, Node<K, V> curr, boolean result) {
            this.curr = curr;
            this.pred = pred;
            this.result = result;
        }
    }

    private Window<K, V> find(K key) {
        Node<K, V> pred = this.head;
        pred.lock();
        while (true) {
            Node<K, V> curr = pred.getNext();
            curr.lock();

            // Read curr key
            Map.Entry<Result, K> resultKEntry = curr.getKey().get(keySerializer);
            if (resultKEntry.getKey() != TRUE) {
                assert false;
            }
            int comparison = keyComparator.compareKeys(resultKEntry.getValue(), key);
            // The key of curr is greater or equal to the given key, no need to advance any more.
            if (comparison >= 0) {
                return new Window<>(pred, curr, comparison == 0);
            }

            // Advance
            pred.unlock();
            pred = curr;
        }
    }

    public boolean put(K key, V value) {
        Window<K, V> window = find(key);
        try {
            // The key exists, update value
            if (window.result) {
                assert window.curr.getValue().put(value, valueSerializer) == TRUE;
                return false;
            }
            // The key does not exist
            Node<K, V> node = new Node<>(key, keySerializer, value, valueSerializer, window.curr, manager);
            window.pred.setNext(node);
            return true;
        } finally {
            window.pred.unlock();
            window.curr.unlock();
        }
    }

    public boolean remove(K key) {
        Window<K, V> window = find(key);
        try {
            // The key exists, update value
            if (window.result) {
                Node<K, V> succ = window.curr.getNext();
                window.pred.setNext(succ);
                window.curr.deleted = true;
            } else {
                // The key does not exist
                return false;
            }
        } finally {
            window.pred.unlock();
            window.curr.unlock();
        }

        assert window.curr.getKey().delete(keySerializer).getKey() == TRUE;
        assert window.curr.getValue().delete(valueSerializer).getKey() == TRUE;
        return true;
    }

    public boolean containsKey(K key) {
        Window<K, V> window = find(key);
        window.pred.unlock();
        window.curr.unlock();
        return window.result;
    }

    public LockBasedLinkedList(NovaManager manager, OakSerializer<K> keySerializer, K minKey, K maxKey,
                               OakSerializer<V> valueSerializer, OakComparator<K> keyComparator) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        Node<K, V> max = new Node<>(maxKey, keySerializer, null, manager);
        this.head = new Node<>(minKey, keySerializer, max, manager);
        this.keyComparator = keyComparator;
        this.manager = manager;
    }

    private Node<K, V> head;
    private OakSerializer<K> keySerializer;
    private OakSerializer<V> valueSerializer;
    private OakComparator<K> keyComparator;
    private NovaManager manager;
}
