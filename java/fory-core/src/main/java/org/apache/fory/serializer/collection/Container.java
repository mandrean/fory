/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fory.serializer.collection;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.apache.fory.context.CopyContext;
import org.apache.fory.memory.Platform;
import org.apache.fory.reflect.ReflectionUtils;
import org.apache.fory.util.GraalvmSupport;

class Container {}

final class SortedContainerBulkAccess {
  private static final boolean GRAALVM_NATIVE_IMAGE = GraalvmSupport.IN_GRAALVM_NATIVE_IMAGE;
  private static final String ADD_METHOD_NAME = "add";
  private static final String ADD_ALL_METHOD_NAME = "addAll";
  private static final String PUT_METHOD_NAME = "put";
  private static final String PUT_ALL_METHOD_NAME = "putAll";
  private static final int OBJECT_ARRAY_REFERENCE_SIZE =
      Platform.UNSAFE.arrayIndexScale(Object[].class);

  private SortedContainerBulkAccess() {}

  static boolean canBulkReadSortedSet(Class<?> type) {
    return GRAALVM_NATIVE_IMAGE
        ? NativeImageSortedContainerBulkAccess.canBulkReadSortedSet(type)
        : JvmSortedContainerBulkAccess.canBulkReadSortedSet(type);
  }

  static boolean canBulkReadSortedMap(Class<?> type) {
    return GRAALVM_NATIVE_IMAGE
        ? NativeImageSortedContainerBulkAccess.canBulkReadSortedMap(type)
        : JvmSortedContainerBulkAccess.canBulkReadSortedMap(type);
  }

  static boolean canBulkBufferSortedSet(int numElements, int bufferLimitBytes) {
    return bufferLimitBytes > 0
        && numElements > 0
        && estimateSortedSetBufferBytes(numElements) <= bufferLimitBytes;
  }

  static boolean canBulkBufferSortedMap(int numElements, int bufferLimitBytes) {
    return bufferLimitBytes > 0
        && numElements > 0
        && estimateSortedMapBufferBytes(numElements) <= bufferLimitBytes;
  }

  static long estimateSortedSetBufferBytes(int numElements) {
    return (long) numElements * OBJECT_ARRAY_REFERENCE_SIZE;
  }

  static long estimateSortedMapBufferBytes(int numElements) {
    return (long) numElements * OBJECT_ARRAY_REFERENCE_SIZE * 2L;
  }

  static boolean computeSetBulkReadSupported(Class<?> type) {
    return TreeSet.class.isAssignableFrom(type)
        && hasInheritedMutator(type, TreeSet.class, ADD_METHOD_NAME, Object.class)
        && hasInheritedMutator(type, TreeSet.class, ADD_ALL_METHOD_NAME, Collection.class);
  }

  static boolean computeMapBulkReadSupported(Class<?> type) {
    return TreeMap.class.isAssignableFrom(type)
        && hasInheritedMutator(type, TreeMap.class, PUT_METHOD_NAME, Object.class, Object.class)
        && hasInheritedMutator(type, TreeMap.class, PUT_ALL_METHOD_NAME, Map.class);
  }

  private static boolean hasInheritedMutator(
      Class<?> type, Class<?> rootType, String methodName, Class<?>... parameterTypes) {
    try {
      return type.getMethod(methodName, parameterTypes).getDeclaringClass() == rootType;
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(
          "Missing " + methodName + " on sorted container type " + type, e);
    }
  }
}

final class JvmSortedContainerBulkAccess {
  private static final ClassValue<Boolean> TREE_SET_BULK_READ_SUPPORTED =
      new ClassValue<Boolean>() {
        @Override
        protected Boolean computeValue(Class<?> type) {
          return SortedContainerBulkAccess.computeSetBulkReadSupported(type);
        }
      };

  private static final ClassValue<Boolean> TREE_MAP_BULK_READ_SUPPORTED =
      new ClassValue<Boolean>() {
        @Override
        protected Boolean computeValue(Class<?> type) {
          return SortedContainerBulkAccess.computeMapBulkReadSupported(type);
        }
      };

  private JvmSortedContainerBulkAccess() {}

  static boolean canBulkReadSortedSet(Class<?> type) {
    return TREE_SET_BULK_READ_SUPPORTED.get(type);
  }

  static boolean canBulkReadSortedMap(Class<?> type) {
    return TREE_MAP_BULK_READ_SUPPORTED.get(type);
  }
}

final class NativeImageSortedContainerBulkAccess {
  private static final ConcurrentHashMap<Class<?>, Boolean> TREE_SET_BULK_READ_SUPPORTED =
      new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<Class<?>, Boolean> TREE_MAP_BULK_READ_SUPPORTED =
      new ConcurrentHashMap<>();

  private NativeImageSortedContainerBulkAccess() {}

  static boolean canBulkReadSortedSet(Class<?> type) {
    return TREE_SET_BULK_READ_SUPPORTED.computeIfAbsent(
        type, SortedContainerBulkAccess::computeSetBulkReadSupported);
  }

  static boolean canBulkReadSortedMap(Class<?> type) {
    return TREE_MAP_BULK_READ_SUPPORTED.computeIfAbsent(
        type, SortedContainerBulkAccess::computeMapBulkReadSupported);
  }
}

/** A collection container to hold collection elements by array. */
class CollectionContainer<T> extends AbstractCollection<T> {
  final Object[] elements;
  int size;

  public CollectionContainer(int capacity) {
    elements = new Object[capacity];
  }

  @Override
  public Iterator<T> iterator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean add(Object o) {
    elements[size++] = o;
    return true;
  }
}

/** A sorted collection container to hold collection elements and comparator. */
class SortedCollectionContainer<T> extends CollectionContainer<T> {
  Comparator<T> comparator;

  public SortedCollectionContainer(Comparator<T> comparator, int capacity) {
    super(capacity);
    this.comparator = comparator;
  }
}

/** A map container to hold map key and value elements by arrays. */
class MapContainer<K, V> extends AbstractMap<K, V> {
  final Object[] keyArray;
  final Object[] valueArray;
  int size;

  public MapContainer(int capacity) {
    keyArray = new Object[capacity];
    valueArray = new Object[capacity];
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public V put(K key, V value) {
    keyArray[size] = key;
    valueArray[size++] = value;
    return null;
  }
}

/** A sorted map container to hold map data and comparator. */
class SortedMapContainer<K, V> extends MapContainer<K, V> {

  final Comparator<K> comparator;

  public SortedMapContainer(Comparator<K> comparator, int capacity) {
    super(capacity);
    this.comparator = comparator;
  }
}

/** A map container to hold map key and value elements in one array. */
class JDKImmutableMapContainer<K, V> extends AbstractMap<K, V> {
  final Object[] array;
  private int offset;

  JDKImmutableMapContainer(int mapCapacity) {
    array = new Object[mapCapacity << 1];
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public V put(K key, V value) {
    array[offset++] = key;
    array[offset++] = value;
    return null;
  }

  public int size() {
    return offset >> 1;
  }
}

/**
 * Builder that buffers sorted-set elements until the JDK bulk-construction fast path is proven
 * safe. If the stream order is invalid, the builder falls back to normal tree insertion so the
 * resulting set keeps the same semantics as the old incremental read path.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
class SortedSetAccumulator<E> extends AbstractSet<E> implements SortedSet<E> {
  final SortedSet<E> target;
  final Object[] elements;
  int size;
  private boolean fallbackToTarget;

  SortedSetAccumulator(SortedSet<E> target, int capacity) {
    this.target = target;
    this.elements = new Object[capacity];
  }

  @Override
  public boolean add(Object o) {
    E element = (E) o;
    if (fallbackToTarget) {
      return target.add(element);
    }
    if (size == 0) {
      // TreeSet.addAll on an empty target can bulk-build without validating a singleton element.
      compareElements(element, element);
      elements[size++] = element;
      return true;
    }
    try {
      if (compareElements((E) elements[size - 1], element) >= 0) {
        flushToTarget();
        fallbackToTarget = true;
        return target.add(element);
      }
    } catch (RuntimeException e) {
      flushToTarget();
      fallbackToTarget = true;
      return target.add(element);
    }
    elements[size++] = element;
    return true;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public Comparator<? super E> comparator() {
    return target.comparator();
  }

  @Override
  public Iterator<E> iterator() {
    return new Iterator<E>() {
      int idx = 0;

      @Override
      public boolean hasNext() {
        return idx < size;
      }

      @Override
      public E next() {
        if (idx >= size) {
          throw new NoSuchElementException();
        }
        return (E) elements[idx++];
      }
    };
  }

  @Override
  public E first() {
    if (size == 0) {
      throw new NoSuchElementException();
    }
    return (E) elements[0];
  }

  @Override
  public E last() {
    if (size == 0) {
      throw new NoSuchElementException();
    }
    return (E) elements[size - 1];
  }

  @Override
  public SortedSet<E> subSet(E fromElement, E toElement) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SortedSet<E> headSet(E toElement) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SortedSet<E> tailSet(E fromElement) {
    throw new UnsupportedOperationException();
  }

  SortedSet<E> build() {
    if (!fallbackToTarget && size > 0) {
      target.addAll(this);
      size = 0;
    }
    return target;
  }

  private int compareElements(E left, E right) {
    Comparator comparator = target.comparator();
    if (comparator != null) {
      return comparator.compare(left, right);
    }
    return ((Comparable) left).compareTo(right);
  }

  private void flushToTarget() {
    for (int i = 0; i < size; i++) {
      target.add((E) elements[i]);
    }
    size = 0;
  }
}

/**
 * Builder that buffers sorted-map entries until the JDK bulk-construction fast path is proven safe.
 * If the stream order is invalid, the builder falls back to normal tree insertion so the resulting
 * map keeps the same semantics as the old incremental read path.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
class SortedMapAccumulator<K, V> extends AbstractMap<K, V> implements SortedMap<K, V> {
  final SortedMap<K, V> target;
  final Object[] keys;
  final Object[] values;
  int size;
  private final ReusableEntrySet entrySetView;
  private boolean fallbackToTarget;

  SortedMapAccumulator(SortedMap<K, V> target, int capacity) {
    this.target = target;
    this.keys = new Object[capacity];
    this.values = new Object[capacity];
    this.entrySetView = new ReusableEntrySet();
  }

  @Override
  public V put(K key, V value) {
    if (fallbackToTarget) {
      return target.put(key, value);
    }
    if (size == 0) {
      // TreeMap.putAll on an empty target can bulk-build without validating a singleton key.
      compareKeys(key, key);
      keys[size] = key;
      values[size++] = value;
      return null;
    }
    try {
      if (compareKeys((K) keys[size - 1], key) >= 0) {
        flushToTarget();
        fallbackToTarget = true;
        return target.put(key, value);
      }
    } catch (RuntimeException e) {
      flushToTarget();
      fallbackToTarget = true;
      return target.put(key, value);
    }
    keys[size] = key;
    values[size++] = value;
    return null;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public Comparator<? super K> comparator() {
    return target.comparator();
  }

  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    return entrySetView;
  }

  @Override
  public K firstKey() {
    if (size == 0) {
      throw new NoSuchElementException();
    }
    return (K) keys[0];
  }

  @Override
  public K lastKey() {
    if (size == 0) {
      throw new NoSuchElementException();
    }
    return (K) keys[size - 1];
  }

  @Override
  public SortedMap<K, V> subMap(K fromKey, K toKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SortedMap<K, V> headMap(K toKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SortedMap<K, V> tailMap(K fromKey) {
    throw new UnsupportedOperationException();
  }

  SortedMap<K, V> build() {
    if (!fallbackToTarget && size > 0) {
      target.putAll(this);
      size = 0;
    }
    return target;
  }

  /** Cached entry set that reuses a single mutable entry to avoid per-element allocation. */
  private class ReusableEntrySet extends AbstractSet<Map.Entry<K, V>> {
    @Override
    public int size() {
      return size;
    }

    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
      return new ReusableEntryIterator();
    }
  }

  /**
   * Iterator that returns a single reusable entry. Safe because {@code TreeMap.buildFromSorted}
   * extracts key/value immediately and does not hold entry references.
   */
  private class ReusableEntryIterator implements Iterator<Map.Entry<K, V>>, Map.Entry<K, V> {
    int idx = 0;
    K key;
    V value;

    @Override
    public boolean hasNext() {
      return idx < size;
    }

    @Override
    public Map.Entry<K, V> next() {
      if (idx >= size) {
        throw new NoSuchElementException();
      }
      key = (K) keys[idx];
      value = (V) values[idx];
      idx++;
      return this;
    }

    @Override
    public K getKey() {
      return key;
    }

    @Override
    public V getValue() {
      return value;
    }

    @Override
    public V setValue(V v) {
      throw new UnsupportedOperationException();
    }
  }

  private int compareKeys(K left, K right) {
    Comparator comparator = target.comparator();
    if (comparator != null) {
      return comparator.compare(left, right);
    }
    return ((Comparable) left).compareTo(right);
  }

  private void flushToTarget() {
    for (int i = 0; i < size; i++) {
      target.put((K) keys[i], (V) values[i]);
    }
    size = 0;
  }
}

final class StateTransferringCollection<T extends Collection> extends AbstractCollection<Object> {
  private final T target;
  private final Collection delegate;
  private final Class<?> rootType;

  StateTransferringCollection(T target, Collection delegate, Class<?> rootType) {
    this.target = target;
    this.delegate = delegate;
    this.rootType = rootType;
  }

  @Override
  public Iterator<Object> iterator() {
    return delegate.iterator();
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public boolean add(Object value) {
    return delegate.add(value);
  }

  T finish() {
    Object source =
        delegate instanceof SortedSetAccumulator
            ? ((SortedSetAccumulator<?>) delegate).build()
            : delegate;
    RootStateTransferrer.transfer(rootType, source, target);
    return target;
  }
}

final class StateTransferringMap<T extends Map> extends AbstractMap<Object, Object> {
  private final T target;
  private final Map delegate;
  private final Class<?> rootType;

  StateTransferringMap(T target, Map delegate, Class<?> rootType) {
    this.target = target;
    this.delegate = delegate;
    this.rootType = rootType;
  }

  @Override
  public Set<Entry<Object, Object>> entrySet() {
    return delegate.entrySet();
  }

  @Override
  public Object put(Object key, Object value) {
    return delegate.put(key, value);
  }

  T finish() {
    Object source =
        delegate instanceof SortedMapAccumulator
            ? ((SortedMapAccumulator<?, ?>) delegate).build()
            : delegate;
    RootStateTransferrer.transfer(rootType, source, target);
    return target;
  }
}

final class ContainerTransfer {
  private ContainerTransfer() {}

  // Sorted-container source constructors need two-stage materialization: allocate and register the
  // final leaf first, then populate a JDK root container and transfer only the inherited
  // root-container state into the leaf. Direct constructors skip the transfer wrapper entirely.

  static <T extends Collection> Collection wrapCollection(
      T target, Collection delegate, Class<?> rootType) {
    return new StateTransferringCollection<>(target, delegate, rootType);
  }

  static <T extends Map> Map wrapMap(T target, Map delegate, Class<?> rootType) {
    return new StateTransferringMap<>(target, delegate, rootType);
  }

  static <T extends Collection> Collection readCollection(
      Class<T> type,
      ContainerConstructors.CollectionConstruction<T> construction,
      Consumer<T> registrar,
      Consumer<T> initializer) {
    return readCollection(type, construction, registrar, initializer, 0, 0);
  }

  static <T extends Collection> Collection readCollection(
      Class<T> type,
      ContainerConstructors.CollectionConstruction<T> construction,
      Consumer<T> registrar,
      Consumer<T> initializer,
      int numElements,
      int bulkReadBufferLimitBytes) {
    if (construction.getKind() == ContainerConstructors.Kind.DIRECT) {
      ContainerConstructors.DirectCollectionConstruction<T> directConstruction =
          (ContainerConstructors.DirectCollectionConstruction<T>) construction;
      T collection = directConstruction.newCollection();
      registrar.accept(collection);
      initializer.accept(collection);
      return maybeWrapSortedSetReadTarget(collection, numElements, bulkReadBufferLimitBytes);
    }
    ContainerConstructors.RootTransferCollectionConstruction<T> rootTransferConstruction =
        (ContainerConstructors.RootTransferCollectionConstruction<T>) construction;
    T target = Platform.newInstance(type);
    registrar.accept(target);
    initializer.accept(target);
    Collection rootCollection =
        maybeWrapSortedSetReadTarget(
            rootTransferConstruction.newRootCollection(), numElements, bulkReadBufferLimitBytes);
    return wrapCollection(target, rootCollection, rootTransferConstruction.getRootType());
  }

  static <T extends Map> Map readMap(
      Class<T> type,
      ContainerConstructors.MapConstruction<T> construction,
      Consumer<T> registrar,
      Consumer<T> initializer) {
    return readMap(type, construction, registrar, initializer, 0, 0);
  }

  static <T extends Map> Map readMap(
      Class<T> type,
      ContainerConstructors.MapConstruction<T> construction,
      Consumer<T> registrar,
      Consumer<T> initializer,
      int numElements,
      int bulkReadBufferLimitBytes) {
    if (construction.getKind() == ContainerConstructors.Kind.DIRECT) {
      ContainerConstructors.DirectMapConstruction<T> directConstruction =
          (ContainerConstructors.DirectMapConstruction<T>) construction;
      T map = directConstruction.newMap();
      registrar.accept(map);
      initializer.accept(map);
      return maybeWrapSortedMapReadTarget(map, numElements, bulkReadBufferLimitBytes);
    }
    ContainerConstructors.RootTransferMapConstruction<T> rootTransferConstruction =
        (ContainerConstructors.RootTransferMapConstruction<T>) construction;
    T target = Platform.newInstance(type);
    registrar.accept(target);
    initializer.accept(target);
    Map rootMap =
        maybeWrapSortedMapReadTarget(
            rootTransferConstruction.newRootMap(), numElements, bulkReadBufferLimitBytes);
    return wrapMap(target, rootMap, rootTransferConstruction.getRootType());
  }

  static <T extends Collection> T finishCollection(Collection collection) {
    if (collection instanceof StateTransferringCollection) {
      return ((StateTransferringCollection<T>) collection).finish();
    }
    if (collection instanceof SortedSetAccumulator) {
      return (T) ((SortedSetAccumulator<?>) collection).build();
    }
    return (T) collection;
  }

  static <T extends Map> T finishMap(Map map) {
    if (map instanceof StateTransferringMap) {
      return ((StateTransferringMap<T>) map).finish();
    }
    if (map instanceof SortedMapAccumulator) {
      return (T) ((SortedMapAccumulator<?, ?>) map).build();
    }
    return (T) map;
  }

  static void transferRootState(Class<?> rootType, Object source, Object target) {
    RootStateTransferrer.transfer(rootType, source, target);
  }

  static <T extends Collection> T copyCollection(
      Class<T> type,
      T originCollection,
      CopyContext copyContext,
      ContainerConstructors.CollectionConstruction<T> construction,
      Consumer<T> initializer,
      Consumer<Collection> contentCopier) {
    if (construction.getKind() == ContainerConstructors.Kind.DIRECT) {
      ContainerConstructors.DirectCollectionConstruction<T> directConstruction =
          (ContainerConstructors.DirectCollectionConstruction<T>) construction;
      T target = directConstruction.newCollection();
      copyContext.reference(originCollection, target);
      initializer.accept(target);
      contentCopier.accept(target);
      return target;
    }
    ContainerConstructors.RootTransferCollectionConstruction<T> rootTransferConstruction =
        (ContainerConstructors.RootTransferCollectionConstruction<T>) construction;
    T target = Platform.newInstance(type);
    copyContext.reference(originCollection, target);
    initializer.accept(target);
    Collection rootCollection = rootTransferConstruction.newRootCollection();
    contentCopier.accept(rootCollection);
    transferRootState(rootTransferConstruction.getRootType(), rootCollection, target);
    return target;
  }

  static <T extends Map> T copyMap(
      Class<T> type,
      T originMap,
      CopyContext copyContext,
      ContainerConstructors.MapConstruction<T> construction,
      Consumer<T> initializer,
      Consumer<Map> entryCopier) {
    if (construction.getKind() == ContainerConstructors.Kind.DIRECT) {
      ContainerConstructors.DirectMapConstruction<T> directConstruction =
          (ContainerConstructors.DirectMapConstruction<T>) construction;
      T target = directConstruction.newMap();
      copyContext.reference(originMap, target);
      initializer.accept(target);
      entryCopier.accept(target);
      return target;
    }
    ContainerConstructors.RootTransferMapConstruction<T> rootTransferConstruction =
        (ContainerConstructors.RootTransferMapConstruction<T>) construction;
    T target = Platform.newInstance(type);
    copyContext.reference(originMap, target);
    initializer.accept(target);
    Map rootMap = rootTransferConstruction.newRootMap();
    entryCopier.accept(rootMap);
    transferRootState(rootTransferConstruction.getRootType(), rootMap, target);
    return target;
  }

  static Collection maybeWrapSortedSetReadTarget(
      Collection collection, int numElements, int bulkReadBufferLimitBytes) {
    if (SortedContainerBulkAccess.canBulkReadSortedSet(collection.getClass())
        && collection instanceof TreeSet
        && SortedContainerBulkAccess.canBulkBufferSortedSet(
            numElements, bulkReadBufferLimitBytes)) {
      return new SortedSetAccumulator((SortedSet) collection, numElements);
    }
    return collection;
  }

  static Map maybeWrapSortedMapReadTarget(Map map, int numElements, int bulkReadBufferLimitBytes) {
    if (SortedContainerBulkAccess.canBulkReadSortedMap(map.getClass())
        && map instanceof TreeMap
        && SortedContainerBulkAccess.canBulkBufferSortedMap(
            numElements, bulkReadBufferLimitBytes)) {
      return new SortedMapAccumulator((SortedMap) map, numElements);
    }
    return map;
  }
}

final class RootStateTransferrer {
  private static final ClassValue<RootField[]> rootFieldsCache =
      new ClassValue<RootField[]>() {
        @Override
        protected RootField[] computeValue(Class<?> type) {
          List<RootField> rootFields = new ArrayList<>();
          for (Field field : ReflectionUtils.getFields(type, true)) {
            if (!Modifier.isStatic(field.getModifiers())) {
              rootFields.add(new RootField(field));
            }
          }
          return rootFields.toArray(new RootField[0]);
        }
      };

  private RootStateTransferrer() {}

  // Source constructors rebuild the JDK root portion on a temporary delegate. We then copy only
  // the inherited non-static fields from that root hierarchy into the final leaf instance, leaving
  // any subclass-specific fields on the leaf untouched.
  static void transfer(Class<?> rootType, Object source, Object target) {
    for (RootField rootField : rootFieldsCache.get(rootType)) {
      rootField.copy(source, target);
    }
  }

  private static final class RootField {
    private final long offset;
    private final Class<?> fieldType;

    private RootField(Field field) {
      offset = Platform.objectFieldOffset(field);
      fieldType = field.getType();
    }

    private void copy(Object source, Object target) {
      if (fieldType == boolean.class) {
        Platform.putBoolean(target, offset, Platform.getBoolean(source, offset));
      } else if (fieldType == byte.class) {
        Platform.putByte(target, offset, Platform.getByte(source, offset));
      } else if (fieldType == short.class) {
        Platform.putShort(target, offset, Platform.getShort(source, offset));
      } else if (fieldType == char.class) {
        Platform.putChar(target, offset, Platform.getChar(source, offset));
      } else if (fieldType == int.class) {
        Platform.putInt(target, offset, Platform.getInt(source, offset));
      } else if (fieldType == long.class) {
        Platform.putLong(target, offset, Platform.getLong(source, offset));
      } else if (fieldType == float.class) {
        Platform.putFloat(target, offset, Platform.getFloat(source, offset));
      } else if (fieldType == double.class) {
        Platform.putDouble(target, offset, Platform.getDouble(source, offset));
      } else {
        Platform.putObject(target, offset, Platform.getObject(source, offset));
      }
    }
  }
}
