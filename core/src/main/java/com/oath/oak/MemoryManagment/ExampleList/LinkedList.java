package com.oath.oak.MemoryManagment.ExampleList;

import com.oath.oak.MemoryManagment.NovaManager;
import com.oath.oak.MemoryManagment.OffHeapSlice;
import com.oath.oak.MemoryManagment.Result;
import com.oath.oak.OakComparator;
import com.oath.oak.OakSerializer;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.function.Function;

import static com.oath.oak.MemoryManagment.Result.TRUE;

public class LinkedList<K, V> {

    static class Node<K, V> {
        private OffHeapSlice key;
        private OffHeapSlice value;
        private AtomicMarkableReference<Node<K, V>> next;

        Node(K key, OakSerializer<K> keySerializer, Node<K, V> next, NovaManager manager) {
            this.key = manager.allocateSlice(keySerializer.calculateSize(key));
            this.key.put(key, keySerializer);
            this.value = null;
            this.next = new AtomicMarkableReference<>(next, false);
        }

        Node(K key, OakSerializer<K> keySerializer, V value, OakSerializer<V> valueSerializer,
             Node<K, V> next, NovaManager manager) {
            this.key = manager.allocateSlice(keySerializer.calculateSize(key));
            this.key.put(key, keySerializer);
            this.value = manager.allocateSlice(valueSerializer.calculateSize(value));
            this.value.put(value, valueSerializer);
            this.next = new AtomicMarkableReference<>(next, false);
        }

        boolean mark() {
            Node<K, V> oldNext = this.next.getReference();
            return this.next.compareAndSet(oldNext, oldNext, false, true);
        }

        public boolean casNext(Node<K, V> exp, Node<K, V> next) {
            return this.next.compareAndSet(exp, next, false, false);
        }

        public OffHeapSlice getKey() {
            return key;
        }

        public OffHeapSlice getValue() {
            return value;
        }

        public Node<K, V> getNext() {
            return this.next.getReference();
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
        outer:
        while (true) {
            Node<K, V> pred = this.head;
            Node<K, V> curr = pred.getNext();

            while (true) {
                boolean[] marking = new boolean[1];
                Node<K, V> succ = curr.next.get(marking);
                while (marking[0]) {
                    if (!pred.next.compareAndSet(curr, succ, false, false)) {
                        continue outer;
                    }
                    curr = succ;
                    succ = curr.next.get(marking);
                }
                Map.Entry<Result, K> resultKEntry = curr.getKey().get(keySerializer);
                if (resultKEntry.getKey() != TRUE) {
                    continue outer;
                }
                int comparison = keyComparator.compareKeys(key, resultKEntry.getValue());
                if (comparison <= 0) {
                    return new Window<>(pred, curr, comparison == 0);
                }
                pred = curr;
                curr = succ;
            }
        }
    }

    public int size() {
        return size.get();
    }

    public V put(K key, V value) {
        while (true) {
            Window<K, V> window = find(key);
            if (window.result) {
                Map.Entry<Result, V> resultVEntry = window.curr.getValue().exchange(value, valueSerializer);
                if (resultVEntry.getKey() != TRUE) {
                    continue;
                }
                return resultVEntry.getValue();
            }

            if (linkNewNode(key, value, window)) {
                continue;
            }
            return null;
        }
    }

    private boolean linkNewNode(K key, V value, Window<K, V> window) {
        Node<K, V> newNode = new Node<>(key, keySerializer, value, valueSerializer, window.curr, manager);
        if (!window.pred.next.compareAndSet(window.curr, newNode, false, false)) {
            return true;
        }
        size.incrementAndGet();
        return false;
    }

    public V putIfAbsent(K key, V value) {
        while (true) {
            Window<K, V> window = find(key);
            if (window.result) {
                Map.Entry<Result, V> resultVEntry = window.curr.getValue().get(valueSerializer);
                if (resultVEntry.getKey() != TRUE) {
                    continue;
                }
                return resultVEntry.getValue();
            }

            if (linkNewNode(key, value, window)) {
                continue;
            }
            return null;
        }
    }

    public boolean computeIfPresent(K key, Function<V, V> computer) {
        while (true) {
            Window<K, V> window = find(key);
            if (window.result) {
                Result result = window.curr.getValue().compute(computer, valueSerializer);
                if (result != TRUE) {
                    continue;
                }
                return true;
            }
            return false;
        }
    }

    public V remove(Object key) {
        try {
            while (true) {
                Window<K, V> window = find((K) key);
                if (window.result) {
                    if (!window.curr.mark()) {
                        continue;
                    }
                    size.decrementAndGet();
                    Map.Entry<Result, V> resultVEntry = window.curr.getValue().delete(valueSerializer);
                    assert resultVEntry.getKey() == TRUE;
                    window.pred.next.compareAndSet(window.curr, window.curr.getNext(), false, false);
                    return resultVEntry.getValue();
                }
                return null;
            }
        } catch (ClassCastException e) {
            return null;
        }
    }

    public V get(Object key) {
        try {
            outer:
            while (true) {
                boolean[] marked = {false};
                Node<K, V> curr = head;
                Map.Entry<Result, K> resultKEntry = curr.getKey().get(keySerializer);
                if (resultKEntry.getKey() != TRUE) {
                    continue;
                }
                int comparison = keyComparator.compareKeys((K) key, resultKEntry.getValue());
                while (comparison > 0) {
                    curr = curr.next.get(marked);
                    resultKEntry = curr.getKey().get(keySerializer);
                    if (resultKEntry.getKey() != TRUE) {
                        continue outer;
                    }
                    comparison = keyComparator.compareKeys((K) key, resultKEntry.getValue());
                }
                if (comparison == 0 && !marked[0]) {
                    Map.Entry<Result, V> resultVEntry = curr.getValue().get(valueSerializer);
                    if (resultVEntry.getKey() != TRUE) {
                        continue;
                    }
                    return resultVEntry.getValue();
                }
                return null;
            }
        } catch (ClassCastException e) {
            return null;
        }
    }

    public LinkedList(NovaManager manager, OakSerializer<K> keySerializer, K minKey, K maxKey,
                      OakSerializer<V> valueSerializer, OakComparator<K> keyComparator) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        Node<K, V> max = new Node<>(maxKey, keySerializer, null, manager);
        this.head = new Node<>(minKey, keySerializer, max, manager);
        this.keyComparator = keyComparator;
        this.manager = manager;
        this.size = new AtomicInteger(0);
    }

    private NovaManager manager;
    private Node<K, V> head;
    private AtomicInteger size;
    private OakSerializer<K> keySerializer;
    private OakSerializer<V> valueSerializer;
    private OakComparator<K> keyComparator;
}
