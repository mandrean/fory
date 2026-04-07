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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.fory.context.CopyContext;
import org.apache.fory.memory.Platform;
import org.apache.fory.reflect.ReflectionUtils;

class Container {}

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
    RootStateTransferrer.transfer(rootType, delegate, target);
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
    RootStateTransferrer.transfer(rootType, delegate, target);
    return target;
  }
}

final class ContainerTransfer {
  private ContainerTransfer() {}

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
    if (!construction.needsStateTransfer()) {
      T collection = construction.newCollection();
      registrar.accept(collection);
      initializer.accept(collection);
      return collection;
    }
    T target = Platform.newInstance(type);
    registrar.accept(target);
    initializer.accept(target);
    return wrapCollection(target, construction.newRootCollection(), construction.getRootType());
  }

  static <T extends Map> Map readMap(
      Class<T> type,
      ContainerConstructors.MapConstruction<T> construction,
      Consumer<T> registrar,
      Consumer<T> initializer) {
    if (!construction.needsStateTransfer()) {
      T map = construction.newMap();
      registrar.accept(map);
      initializer.accept(map);
      return map;
    }
    T target = Platform.newInstance(type);
    registrar.accept(target);
    initializer.accept(target);
    return wrapMap(target, construction.newRootMap(), construction.getRootType());
  }

  static <T extends Collection> T finishCollection(Collection collection) {
    if (collection instanceof StateTransferringCollection) {
      return ((StateTransferringCollection<T>) collection).finish();
    }
    return (T) collection;
  }

  static <T extends Map> T finishMap(Map map) {
    if (map instanceof StateTransferringMap) {
      return ((StateTransferringMap<T>) map).finish();
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
    if (!construction.needsStateTransfer()) {
      T target = construction.newCollection();
      copyContext.reference(originCollection, target);
      initializer.accept(target);
      contentCopier.accept(target);
      return target;
    }
    T target = Platform.newInstance(type);
    copyContext.reference(originCollection, target);
    initializer.accept(target);
    Collection rootCollection = construction.newRootCollection();
    contentCopier.accept(rootCollection);
    transferRootState(construction.getRootType(), rootCollection, target);
    return target;
  }

  static <T extends Map> T copyMap(
      Class<T> type,
      T originMap,
      CopyContext copyContext,
      ContainerConstructors.MapConstruction<T> construction,
      Consumer<T> initializer,
      Consumer<Map> entryCopier) {
    if (!construction.needsStateTransfer()) {
      T target = construction.newMap();
      copyContext.reference(originMap, target);
      initializer.accept(target);
      entryCopier.accept(target);
      return target;
    }
    T target = Platform.newInstance(type);
    copyContext.reference(originMap, target);
    initializer.accept(target);
    Map rootMap = construction.newRootMap();
    entryCopier.accept(rootMap);
    transferRootState(construction.getRootType(), rootMap, target);
    return target;
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
