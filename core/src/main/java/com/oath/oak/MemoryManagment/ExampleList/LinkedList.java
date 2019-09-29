package com.oath.oak.MemoryManagment.ExampleList;

import com.oath.oak.MemoryManagment.NovaManager;
import com.oath.oak.MemoryManagment.OffHeapSlice;
import com.oath.oak.MemoryManagment.Result;
import com.oath.oak.OakComparator;
import com.oath.oak.OakSerializer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
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

        public boolean mark() {
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
                AtomicMarkableReference<Node<K, V>> succ = curr.next;
                boolean[] marking = new boolean[1];
                Node<K, V> oldSucc = succ.get(marking);
                if (marking[0]) {
                    if (!pred.next.compareAndSet(curr, oldSucc, false, false)) {
                        continue outer;
                    }
                    curr = oldSucc;
                    continue;
                }
                Map.Entry<Result, K> resultKEntry = curr.getKey().get(keySerializer);
                if (resultKEntry.getKey() != TRUE) {
                    continue outer;
                }
                int comparison = keyComparator.compareKeys(key, resultKEntry.getValue());
                if (comparison >= 0) {
                    return new Window<>(pred, curr, comparison == 0);
                }
                pred = curr;
                curr = oldSucc;
            }
        }
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

            Node<K, V> newNode = new Node<>(key, keySerializer, value, valueSerializer, window.curr, manager);
            if (!window.pred.next.compareAndSet(window.curr, newNode, false, false)) {
                continue;
            }
            return null;
        }
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

            Node<K, V> newNode = new Node<>(key, keySerializer, value, valueSerializer, window.curr, manager);
            if (!window.pred.next.compareAndSet(window.curr, newNode, false, false)) {
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

    public V remove(K key) {
        while (true) {
            Window<K, V> window = find(key);
            if (window.result) {
                if (!window.curr.mark()) {
                    continue;
                }
                Map.Entry<Result, V> resultVEntry = window.curr.getValue().delete(valueSerializer);
                assert resultVEntry.getKey() == TRUE;
                return resultVEntry.getValue();
            }
            return null;
        }
    }

    public V get(K key) {
        while (true) {
            Window<K, V> window = find(key);
            if (window.result) {
                Map.Entry<Result, V> resultVEntry = window.curr.getValue().get(valueSerializer);
                if (resultVEntry.getKey() != TRUE) {
                    continue;
                }
                return resultVEntry.getValue();
            }
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
    }

    private NovaManager manager;
    private Node<K, V> head;
    private OakSerializer<K> keySerializer;
    private OakSerializer<V> valueSerializer;
    private OakComparator<K> keyComparator;
}
