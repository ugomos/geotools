/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2006-2016, Open Source Geospatial Foundation (OSGeo)
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    version 2.1 of the License.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 */
package org.geotools.util;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.resources.i18n.ErrorKeys;
import org.geotools.resources.i18n.Errors;
import org.geotools.util.logging.Logging;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

import java.util.*;

/**
 * A hash map implementation that uses {@linkplain SoftReference soft references}, leaving memory when an entry is not used anymore and memory is low.
 * <p>
 * This map implementation actually maintains some of the first entries as hard references. Only oldest entries are retained by soft references, in
 * order to avoid too aggressive garbage collection. The amount of entries to retain by hard reference is specified at
 * {@linkplain #SoftValueHashMap2(int) construction time}.
 * <p>
 * This map is thread-safe. It accepts the null key, but doesn't accepts null values. Usage of {@linkplain #values value}, {@linkplain #keySet key} or
 * {@linkplain #entrySet entry} collections are supported except for direct usage of their iterators, which may throw
 * {@link java.util.ConcurrentModificationException} randomly depending on the garbage collector activity.
 *
 * @param <K> The type of keys in the map.
 * @param <V> The type of values in the map.
 *
 * @since 2.3
 *
 *
 * @source $URL$
 * @version $Id$
 * @author Simone Giannecchini
 * @author Martin Desruisseaux
 * @author Ugo Moschini
 */
public class SoftValueHashMap2<K, V> extends AbstractMap<K, V> {
    static final Logger LOGGER = Logging.getLogger(SoftValueHashMap2.class);

    /**
     * The default value for {@link #hardReferencesCount}.
     */
    private static final int DEFAULT_HARD_REFERENCE_COUNT = 20;

    /**
     * The map of hard or soft references. Values are either direct reference to the objects, or wrapped in a {@code Reference} object.
     */
    private final Map<K, Object> hash = new ConcurrentHashMap<K, Object>();

    /**
     * The FIFO list of keys to hard references. Newest elements are first, and latest elements are last. This list should never be longer than
     * {@link #hardReferencesCount}.
     */
    private final Queue<V> hardCache = new ConcurrentLinkedQueue<V>();

    /**
     * Reference queue for cleared SoftReference objects.
     */
    private final ReferenceQueue<? super V> queue = new ReferenceQueue<V>();

    /**
     * The number of hard references to hold internally.
     */
    private final int hardReferencesCount;

    /**
     * The entries to be returned by {@link #entrySet()}, or {@code null} if not yet created.
     */
    private transient Set<Map.Entry<K, V>> entries;

    /**
     * The eventual cleaner
     */
    protected ValueCleaner cleaner;

    /**
     * The lock to perform atomic add+poll operations on {@link #hardCache}.
     */
    private final ReentrantLock hardReferencesLock = new ReentrantLock();

    /**
     * Creates a map with the default hard references count.
     */
    public SoftValueHashMap2() {
        this.cleaner = null;
        hardReferencesCount = DEFAULT_HARD_REFERENCE_COUNT;
    }

    /**
     * Creates a map with the specified hard references count.
     *
     * @param hardReferencesCount The maximal number of hard references to keep.
     */
    public SoftValueHashMap2(final int hardReferencesCount) {
        this.cleaner = null;
        this.hardReferencesCount = hardReferencesCount;
    }

    /**
     * Creates a map with the specified hard references count.
     *
     * @param hardReferencesCount The maximal number of hard references to keep.
     */
    public SoftValueHashMap2(final int hardReferencesCount, ValueCleaner cleaner) {
        this.cleaner = cleaner;
        this.hardReferencesCount = hardReferencesCount;
    }

    /**
     * Returns the number of hard references kept in this cache
     * 
     * @return
     */
    public int getHardReferencesCount() {
        return this.hardReferencesCount;
    }

    /**
     * Ensures that the specified value is non-null.
     */
    private static void ensureNotNull(final Object value) throws IllegalArgumentException {
        if (value == null) {
            throw new IllegalArgumentException(Errors.format(ErrorKeys.NULL_ARGUMENT_$1, "value"));
        }
    }

    /**
     * Performs a consistency check on this map. This method is used for tests and assertions only.
     */
/*    final boolean isValid() {
        int count = 0, size = 0;
        synchronized (hash) {
            for (final Map.Entry<K, ?> entry : hash.entrySet()) {
                if (entry.getValue() instanceof Reference) {
                    count++;
                } else {
                    assert hardCache.contains(entry.getKey());
                }
                size++;
            }
            assert size == hash.size();
            assert hardCache.size() == Math.min(size, hardReferencesCount);
        }
        return count == Math.max(size - hardReferencesCount, 0);
  -  }
  */

    /**
     * Returns the number of entries in this map.
     */
    @Override
    public int size() {
        return hash.size();
    }

    /**
     * Returns {@code true} if this map contains a mapping for the specified key.
     */
    @Override
    public boolean containsKey(final Object key) {
        synchronized (hash) {
            return hash.containsKey(key);
        }
    }

    /**
     * Returns {@code true} if this map maps one or more keys to this value.
     */
    @Override
    public boolean containsValue(final Object value) {
        ensureNotNull(value);
        synchronized (hash) {
            /*
             * We must rely on the super-class default implementation, not on HashMap implementation, because some references are wrapped into
             * SoftReferences.
             */
            return super.containsValue(value);
        }
    }

    /**
     * Returns the value to which this map maps the specified key. Returns {@code null} if the map contains no mapping for this key, or the value has
     * been garbage collected.
     *
     * @param key key whose associated value is to be returned.
     * @return the value to which this map maps the specified key, or {@code null} if none.
     */
    @Override
    public V get(final Object key) {
        processQueue();

        V result = null;
        @SuppressWarnings("unchecked")
        SoftValue<V, K> value = (SoftValue<V, K>) hash.get(key);

        if (value != null) {
            // unwrap the 'real' value from the SoftReference
            result = value.get();
            if (result == null) {
                // The wrapped value was garbage collected, so remove this entry from the backing map:
                hash.remove(key);
            } else {
                // Add this value to the beginning of the strong reference queue (FIFO).
                addToStrongReferences(result);
            }
        }
        return result;
    }

    private void addToStrongReferences(V result) {
        hardReferencesLock.lock();
        try {
            hardCache.add(result);
            trimStrongReferencesIfNecessary();
        } finally {
            hardReferencesLock.unlock();
        }
    }

    // Guarded by the strongReferencesLock in the addToStrongReferences method
    private void trimStrongReferencesIfNecessary() {
        // trim the strong ref queue if necessary:
        while (hardCache.size() > hardReferencesCount) {
            hardCache.poll();
        }
    }

    /**
     * Traverses the ReferenceQueue and removes garbage-collected SoftValue objects from the backing map by looking them up using the SoftValue.key
     * data member.
     */
    private void processQueue() {
        SoftValue sv;
        while ((sv = (SoftValue) hardCache.poll()) != null) {
            hash.remove(sv.key);
        }
    }

    /**
     * Associates the specified value with the specified key in this map.
     *
     * @param key Key with which the specified value is to be associated.
     * @param value Value to be associated with the specified key. The value can't be null.
     *
     * @return Previous value associated with specified key, or {@code null} if there was no mapping for key.
     */
    /**
     * Creates a new entry, but wraps the value in a SoftValue instance to enable auto garbage collection.
     */
    @Override
    public V put(K key, V value) {
        ensureNotNull(value);
        processQueue(); // throw out garbage collected values first
        SoftValue<V, K> sv = new SoftValue<V, K>(value, key, queue, cleaner);
        SoftValue<V, K> previous = (SoftValue<V, K>) hash.put(key, sv);
        addToStrongReferences(value);
        return previous != null ? previous.get() : null;
    }

    /**
     * Copies all of the mappings from the specified map to this map.
     *
     * @param map Mappings to be stored in this map.
     */
    @Override
    public void putAll(final Map<? extends K, ? extends V> map) {
        synchronized (hash) {
            super.putAll(map);
        }
    }

    /**
     * Removes the mapping for this key from this map if present.
     *
     * @param key Key whose mapping is to be removed from the map.
     * @return previous value associated with specified key, or {@code null} if there was no entry for key.
     */
    @Override
    public V remove(Object key) {
        processQueue(); // throw out garbage collected values first
        SoftValue<V, K> raw = (SoftValue<V, K>) hash.remove(key);
        return raw != null ? raw.get() : null;
    }

    /**
     * Removes all mappings from this map.
     */
    @Override
    public void clear() {
        hardReferencesLock.lock();
        try {
            hardCache.clear();
        } finally {
            hardReferencesLock.unlock();
        }
        processQueue(); // throw out garbage collected values
        hash.clear();
    }

    /**
     * Returns a set view of the mappings contained in this map.
     */
    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        synchronized (hash) {
            if (entries == null) {
                entries = new Entries();
            }
            return entries;
        }
    }

    /**
     * Compares the specified object with this map for equality.
     *
     * @param object The object to compare with this map for equality.
     */
    @Override
    public boolean equals(final Object object) {
        synchronized (hash) {
            return super.equals(object);
        }
    }

    /**
     * Returns the hash code value for this map.
     */
    @Override
    public int hashCode() {
        synchronized (hash) {
            return super.hashCode();
        }
    }

    /**
     * Returns a string representation of this map.
     */
    @Override
    public String toString() {
        synchronized (hash) {
            return super.toString();
        }
    }
    
    
    /**
     * Implementation of the entries set to be returned by {@link #entrySet()}.
     */
    private final class Entries extends AbstractSet<Map.Entry<K, V>> {
        /**
         * Returns an iterator over the elements contained in this collection.
         */
        public Iterator<Map.Entry<K, V>> iterator() {
            synchronized (hash) {
                return new Iter<K, V>(hash);
            }
        }

        /**
         * Returns the number of elements in this collection.
         */
        public int size() {
            return SoftValueHashMap2.this.size();
        }

        /**
         * Returns {@code true} if this collection contains the specified element.
         */
        @Override
        public boolean contains(final Object entry) {
            synchronized (hash) {
                return super.contains(entry);
            }
        }

        /**
         * Returns an array containing all of the elements in this collection.
         */
        @Override
        public Object[] toArray() {
            synchronized (hash) {
                return super.toArray();
            }
        }

        /**
         * Returns an array containing all of the elements in this collection.
         */
        @Override
        public <T> T[] toArray(final T[] array) {
            synchronized (hash) {
                return super.toArray(array);
            }
        }

        /**
         * Removes a single instance of the specified element from this collection, if it is present.
         */
        @Override
        public boolean remove(final Object entry) {
            synchronized (hash) {
                return super.remove(entry);
            }
        }

        /**
         * Returns {@code true} if this collection contains all of the elements in the specified collection.
         */
        @Override
        public boolean containsAll(final Collection<?> collection) {
            synchronized (hash) {
                return super.containsAll(collection);
            }
        }

        /**
         * Adds all of the elements in the specified collection to this collection.
         */
        @Override
        public boolean addAll(final Collection<? extends Map.Entry<K, V>> collection) {
            synchronized (hash) {
                return super.addAll(collection);
            }
        }

        /**
         * Removes from this collection all of its elements that are contained in the specified collection.
         */
        @Override
        public boolean removeAll(final Collection<?> collection) {
            synchronized (hash) {
                return super.removeAll(collection);
            }
        }

        /**
         * Retains only the elements in this collection that are contained in the specified collection.
         */
        @Override
        public boolean retainAll(final Collection<?> collection) {
            synchronized (hash) {
                return super.retainAll(collection);
            }
        }

        /**
         * Removes all of the elements from this collection.
         */
        @Override
        public void clear() {
            SoftValueHashMap2.this.clear();
        }

        /**
         * Returns a string representation of this collection.
         */
        @Override
        public String toString() {
            synchronized (hash) {
                return super.toString();
            }
        }
    }


    /**
     * The iterator to be returned by {@link Entries}.
     */
    private static final class Iter<K, V> implements Iterator<Map.Entry<K, V>> {
        /**
         * A copy of the {@link SoftValueHashMap2#hash} field.
         */
        private final Map<K, Object> hash;

        /**
         * The iterator over the {@link #hash} entries.
         */
        private final Iterator<Map.Entry<K, Object>> iterator;

        /**
         * The next entry to be returned by the {@link #next} method, or {@code null} if not yet computed of if the iteration is finished.
         */
        private transient Map.Entry<K, V> entry;

        /**
         * Creates an iterator for the specified {@link SoftValueHashMap2#hash} field.
         */
        Iter(final Map<K, Object> hash) {
            this.hash = hash;
            this.iterator = hash.entrySet().iterator();
        }

        /**
         * Set {@link #entry} to the next entry to iterate. Returns {@code true} if an entry has been found, or {@code false} if the iteration is
         * finished.
         */
        @SuppressWarnings("unchecked")
        private boolean findNext() {
            assert Thread.holdsLock(hash);
            while (iterator.hasNext()) {
                final Map.Entry<K, Object> candidate = iterator.next();
                Object value = candidate.getValue();
                if (value instanceof SoftValue) {
                    value = ((SoftValue) value).get();
                    entry = new MapEntry<K, V>(candidate.getKey(), (V) value);
                    return true;
                }
                if (value != null) {
                    entry = (Map.Entry<K, V>) candidate;
                    return true;
                }
            }
            return false;
        }

        /**
         * Returns {@code true} if this iterator can return more value.
         */
        public boolean hasNext() {
            synchronized (hash) {
                return entry != null || findNext();
            }
        }

        /**
         * Returns the next value. If some value were garbage collected after the iterator was created, they will not be returned. Note however that a
         * {@link ConcurrentModificationException} may be throw if the iteration is not synchronized on {@link #hash}.
         */
        public Map.Entry<K, V> next() {
            synchronized (hash) {
                if (entry == null && !findNext()) {
                    throw new NoSuchElementException();
                }
                final Map.Entry<K, V> next = entry;
                entry = null; // Flags that a new entry will need to be lazily
                              // fetched.
                return next;
            }
        }

        /**
         * Removes the last entry.
         */
        public void remove() {
            synchronized (hash) {
                iterator.remove();
            }
        }
    }

    /**
     * We define our own subclass of SoftReference which contains not only the value but also the key to make it easier to find the entry in the
     * HashMap after it's been garbage collected.
     */
    private static class SoftValue<V, K> extends SoftReference<V> {

        private final K key;

        /**
         * The eventual value cleaner
         */
        private ValueCleaner cleaner;

        /**
         * Constructs a new instance, wrapping the value, key, and queue, as required by the superclass.
         *
         * @param value the map value
         * @param key the map key
         * @param queue the soft reference queue to poll to determine if the entry had been reaped by the GC.
         */
        private SoftValue(V value, K key, ReferenceQueue<? super V> queue,
                final ValueCleaner cleaner) {
            super(value, queue);
            this.key = key;
            this.cleaner = cleaner;
        }

        @Override
        public void clear() {
            if (cleaner != null) {
                final Object value = get();
                if (value != null) {
                    try {
                        cleaner.clean(key, value);
                    } catch (Throwable t) {
                        // never let a bad implementation break soft reference
                        // cleaning
                        LOGGER.log(Level.SEVERE,
                                "Exception occurred while cleaning soft referenced object", t);
                    }
                }
            }
            super.clear();
        }

    }

    /**
     * A delegate that can be used to perform clean up operation, such as resource closing, before the values cached in soft part of the cache gets
     * disposed of
     * 
     * @author Andrea Aime - OpenGeo
     *
     */
    public static interface ValueCleaner {
        /**
         * Cleans the specified object
         * 
         * @param object
         */
        public void clean(Object key, Object object);
    }
}
