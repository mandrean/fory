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

import java.lang.invoke.MethodHandle;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.fory.reflect.ReflectionUtils;
import org.apache.fory.util.Preconditions;

@SuppressWarnings({"rawtypes", "unchecked"})
final class ContainerConstructors {
  private ContainerConstructors() {}

  interface CollectionConstruction<T extends Collection> {
    boolean needsStateTransfer();

    T newCollection();

    Collection newRootCollection();

    Class<?> getRootType();
  }

  interface MapConstruction<T extends Map> {
    boolean needsStateTransfer();

    T newMap();

    Map newRootMap();

    Class<?> getRootType();
  }

  static Class<? extends SortedSet> getSortedSetRootType(Class<?> cls) {
    if (ConcurrentSkipListSet.class.isAssignableFrom(cls)) {
      return ConcurrentSkipListSet.class;
    }
    return TreeSet.class;
  }

  static Class<? extends SortedMap> getSortedMapRootType(Class<?> cls) {
    if (ConcurrentSkipListMap.class.isAssignableFrom(cls)) {
      return ConcurrentSkipListMap.class;
    }
    return TreeMap.class;
  }

  static <T extends Collection> SortedSetFactory<T> sortedSetFactory(
      Class<T> type, Class<? extends SortedSet> rootType) {
    return new SortedSetFactory<>(type, rootType);
  }

  static <T extends Map> SortedMapFactory<T> sortedMapFactory(
      Class<T> type, Class<? extends SortedMap> rootType) {
    return new SortedMapFactory<>(type, rootType);
  }

  static <T extends PriorityQueue> PriorityQueueFactory<T> priorityQueueFactory(Class<T> type) {
    return new PriorityQueueFactory<>(type);
  }

  private static MethodHandle findCtrHandle(Class<?> type, Class<?>... parameterTypes) {
    try {
      return ReflectionUtils.getCtrHandle(type, parameterTypes);
    } catch (Exception e) {
      return null;
    }
  }

  static final class SortedSetFactory<T extends Collection> {
    private enum Mode {
      DIRECT_COMPARATOR(false),
      ROOT_TRANSFER_SORTED_SET(true),
      DIRECT_NO_ARG(false),
      ROOT_TRANSFER_COLLECTION(true);

      private final boolean rootTransfer;

      Mode(boolean rootTransfer) {
        this.rootTransfer = rootTransfer;
      }
    }

    private final Class<T> type;
    private final Class<? extends SortedSet> rootType;
    private final MethodHandle comparatorConstructor;
    private final MethodHandle sortedSetConstructor;
    private final MethodHandle noArgConstructor;
    private final MethodHandle collectionConstructor;
    private final String unsupportedRequirement;

    private SortedSetFactory(Class<T> type, Class<? extends SortedSet> rootType) {
      this.type = type;
      this.rootType = rootType;
      comparatorConstructor = findCtrHandle(type, Comparator.class);
      sortedSetConstructor = findCtrHandle(type, SortedSet.class);
      noArgConstructor = findCtrHandle(type);
      collectionConstructor = findCtrHandle(type, Collection.class);
      unsupportedRequirement =
          comparatorConstructor == null
                  && sortedSetConstructor == null
                  && noArgConstructor == null
                  && collectionConstructor == null
              ? "a supported constructor among (), (Comparator), (Collection), or (SortedSet)"
              : null;
    }

    boolean isSupported() {
      return unsupportedRequirement == null;
    }

    void checkSupported() {
      if (!isSupported()) {
        throw unsupported(type, unsupportedRequirement);
      }
    }

    CollectionConstruction<T> newConstruction(Comparator comparator) {
      checkSupported();
      return new SortedSetConstruction(comparator);
    }

    boolean needsStateTransfer(Comparator comparator) {
      return newConstruction(comparator).needsStateTransfer();
    }

    T newCollection(Comparator comparator) {
      return newConstruction(comparator).newCollection();
    }

    SortedSet newRootCollection(Comparator comparator) {
      return (SortedSet) newConstruction(comparator).newRootCollection();
    }

    Class<? extends SortedSet> getRootType() {
      return rootType;
    }

    private Mode resolveMode(Comparator comparator) {
      if (comparatorConstructor != null) {
        return Mode.DIRECT_COMPARATOR;
      }
      if (sortedSetConstructor != null) {
        return Mode.ROOT_TRANSFER_SORTED_SET;
      }
      if (comparator != null) {
        throw unsupported(
            type, "a comparator-preserving constructor among (Comparator) or (SortedSet)");
      }
      if (noArgConstructor != null) {
        return Mode.DIRECT_NO_ARG;
      }
      return Mode.ROOT_TRANSFER_COLLECTION;
    }

    private SortedSet emptySortedSet(Comparator comparator) {
      if (rootType == ConcurrentSkipListSet.class) {
        return new ConcurrentSkipListSet(comparator);
      }
      return new TreeSet(comparator);
    }

    private final class SortedSetConstruction implements CollectionConstruction<T> {
      private final Comparator comparator;
      private final Mode mode;

      private SortedSetConstruction(Comparator comparator) {
        this.comparator = comparator;
        mode = resolveMode(comparator);
      }

      @Override
      public boolean needsStateTransfer() {
        return mode.rootTransfer;
      }

      @Override
      public T newCollection() {
        Preconditions.checkArgument(!mode.rootTransfer);
        try {
          if (mode == Mode.DIRECT_COMPARATOR) {
            return (T) comparatorConstructor.invoke(comparator);
          }
          return (T) noArgConstructor.invoke();
        } catch (RuntimeException e) {
          throw e;
        } catch (Throwable e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public Collection newRootCollection() {
        Preconditions.checkArgument(mode.rootTransfer);
        return emptySortedSet(comparator);
      }

      @Override
      public Class<?> getRootType() {
        return rootType;
      }
    }
  }

  static final class SortedMapFactory<T extends Map> {
    private enum Mode {
      DIRECT_COMPARATOR(false),
      ROOT_TRANSFER_SORTED_MAP(true),
      DIRECT_NO_ARG(false),
      ROOT_TRANSFER_MAP(true);

      private final boolean rootTransfer;

      Mode(boolean rootTransfer) {
        this.rootTransfer = rootTransfer;
      }
    }

    private final Class<T> type;
    private final Class<? extends SortedMap> rootType;
    private final MethodHandle comparatorConstructor;
    private final MethodHandle sortedMapConstructor;
    private final MethodHandle noArgConstructor;
    private final MethodHandle mapConstructor;
    private final String unsupportedRequirement;

    private SortedMapFactory(Class<T> type, Class<? extends SortedMap> rootType) {
      this.type = type;
      this.rootType = rootType;
      comparatorConstructor = findCtrHandle(type, Comparator.class);
      sortedMapConstructor = findCtrHandle(type, SortedMap.class);
      noArgConstructor = findCtrHandle(type);
      mapConstructor = findCtrHandle(type, Map.class);
      unsupportedRequirement =
          comparatorConstructor == null
                  && sortedMapConstructor == null
                  && noArgConstructor == null
                  && mapConstructor == null
              ? "a supported constructor among (), (Comparator), (Map), or (SortedMap)"
              : null;
    }

    boolean isSupported() {
      return unsupportedRequirement == null;
    }

    void checkSupported() {
      if (!isSupported()) {
        throw unsupported(type, unsupportedRequirement);
      }
    }

    MapConstruction<T> newConstruction(Comparator comparator) {
      checkSupported();
      return new SortedMapConstruction(comparator);
    }

    boolean needsStateTransfer(Comparator comparator) {
      return newConstruction(comparator).needsStateTransfer();
    }

    T newMap(Comparator comparator) {
      return newConstruction(comparator).newMap();
    }

    SortedMap newRootMap(Comparator comparator) {
      return (SortedMap) newConstruction(comparator).newRootMap();
    }

    Class<? extends SortedMap> getRootType() {
      return rootType;
    }

    private Mode resolveMode(Comparator comparator) {
      if (comparatorConstructor != null) {
        return Mode.DIRECT_COMPARATOR;
      }
      if (sortedMapConstructor != null) {
        return Mode.ROOT_TRANSFER_SORTED_MAP;
      }
      if (comparator != null) {
        throw unsupported(
            type, "a comparator-preserving constructor among (Comparator) or (SortedMap)");
      }
      if (noArgConstructor != null) {
        return Mode.DIRECT_NO_ARG;
      }
      return Mode.ROOT_TRANSFER_MAP;
    }

    private SortedMap emptySortedMap(Comparator comparator) {
      if (rootType == ConcurrentSkipListMap.class) {
        return new ConcurrentSkipListMap(comparator);
      }
      return new TreeMap(comparator);
    }

    private final class SortedMapConstruction implements MapConstruction<T> {
      private final Comparator comparator;
      private final Mode mode;

      private SortedMapConstruction(Comparator comparator) {
        this.comparator = comparator;
        mode = resolveMode(comparator);
      }

      @Override
      public boolean needsStateTransfer() {
        return mode.rootTransfer;
      }

      @Override
      public T newMap() {
        Preconditions.checkArgument(!mode.rootTransfer);
        try {
          if (mode == Mode.DIRECT_COMPARATOR) {
            return (T) comparatorConstructor.invoke(comparator);
          }
          return (T) noArgConstructor.invoke();
        } catch (RuntimeException e) {
          throw e;
        } catch (Throwable e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public Map newRootMap() {
        Preconditions.checkArgument(mode.rootTransfer);
        return emptySortedMap(comparator);
      }

      @Override
      public Class<?> getRootType() {
        return rootType;
      }
    }
  }

  static final class PriorityQueueFactory<T extends PriorityQueue> {
    private enum Mode {
      DIRECT_CAPACITY_COMPARATOR(false),
      DIRECT_COMPARATOR(false),
      ROOT_TRANSFER_PRIORITY_QUEUE(true),
      ROOT_TRANSFER_SORTED_SET(true),
      DIRECT_NO_ARG(false),
      DIRECT_CAPACITY(false),
      ROOT_TRANSFER_COLLECTION(true);

      private final boolean rootTransfer;

      Mode(boolean rootTransfer) {
        this.rootTransfer = rootTransfer;
      }
    }

    private final Class<T> type;
    private final MethodHandle comparatorConstructor;
    private final MethodHandle capacityComparatorConstructor;
    private final MethodHandle priorityQueueConstructor;
    private final MethodHandle sortedSetConstructor;
    private final MethodHandle noArgConstructor;
    private final MethodHandle capacityConstructor;
    private final MethodHandle collectionConstructor;
    private final String unsupportedRequirement;

    private PriorityQueueFactory(Class<T> type) {
      this.type = type;
      comparatorConstructor = findCtrHandle(type, Comparator.class);
      capacityComparatorConstructor = findCtrHandle(type, int.class, Comparator.class);
      priorityQueueConstructor = findCtrHandle(type, PriorityQueue.class);
      sortedSetConstructor = findCtrHandle(type, SortedSet.class);
      noArgConstructor = findCtrHandle(type);
      capacityConstructor = findCtrHandle(type, int.class);
      collectionConstructor = findCtrHandle(type, Collection.class);
      unsupportedRequirement =
          comparatorConstructor == null
                  && capacityComparatorConstructor == null
                  && priorityQueueConstructor == null
                  && sortedSetConstructor == null
                  && noArgConstructor == null
                  && capacityConstructor == null
                  && collectionConstructor == null
              ? "a supported constructor among (), (int), (Comparator), (int, Comparator),"
                  + " (Collection), (PriorityQueue), or (SortedSet)"
              : null;
    }

    boolean isSupported() {
      return unsupportedRequirement == null;
    }

    void checkSupported() {
      if (!isSupported()) {
        throw unsupported(type, unsupportedRequirement);
      }
    }

    CollectionConstruction<T> newConstruction(Comparator comparator, int sizeHint) {
      checkSupported();
      return new PriorityQueueConstruction(comparator, sizeHint);
    }

    boolean needsStateTransfer(Comparator comparator, int sizeHint) {
      return newConstruction(comparator, sizeHint).needsStateTransfer();
    }

    T newCollection(Comparator comparator, int sizeHint) {
      return newConstruction(comparator, sizeHint).newCollection();
    }

    PriorityQueue newRootCollection(Comparator comparator, int sizeHint) {
      return (PriorityQueue) newConstruction(comparator, sizeHint).newRootCollection();
    }

    Class<? extends PriorityQueue> getRootType() {
      return PriorityQueue.class;
    }

    private Mode resolveMode(Comparator comparator, int sizeHint) {
      if (capacityComparatorConstructor != null) {
        return Mode.DIRECT_CAPACITY_COMPARATOR;
      }
      if (comparatorConstructor != null) {
        return Mode.DIRECT_COMPARATOR;
      }
      if (priorityQueueConstructor != null) {
        return Mode.ROOT_TRANSFER_PRIORITY_QUEUE;
      }
      if (sortedSetConstructor != null) {
        return Mode.ROOT_TRANSFER_SORTED_SET;
      }
      if (comparator != null && collectionConstructor != null) {
        return Mode.ROOT_TRANSFER_COLLECTION;
      }
      if (comparator != null) {
        throw unsupported(
            type,
            "a comparator-preserving constructor among (Comparator), (int, Comparator),"
                + " (PriorityQueue), or (SortedSet)");
      }
      if (noArgConstructor != null) {
        return Mode.DIRECT_NO_ARG;
      }
      if (capacityConstructor != null) {
        return Mode.DIRECT_CAPACITY;
      }
      return Mode.ROOT_TRANSFER_COLLECTION;
    }

    private final class PriorityQueueConstruction implements CollectionConstruction<T> {
      private final Comparator comparator;
      private final int initialCapacity;
      private final Mode mode;

      private PriorityQueueConstruction(Comparator comparator, int sizeHint) {
        this.comparator = comparator;
        initialCapacity = Math.max(1, sizeHint);
        mode = resolveMode(comparator, sizeHint);
      }

      @Override
      public boolean needsStateTransfer() {
        return mode.rootTransfer;
      }

      @Override
      public T newCollection() {
        Preconditions.checkArgument(!mode.rootTransfer);
        try {
          if (mode == Mode.DIRECT_CAPACITY_COMPARATOR) {
            return (T) capacityComparatorConstructor.invoke(initialCapacity, comparator);
          }
          if (mode == Mode.DIRECT_COMPARATOR) {
            return (T) comparatorConstructor.invoke(comparator);
          }
          if (mode == Mode.DIRECT_NO_ARG) {
            return (T) noArgConstructor.invoke();
          }
          return (T) capacityConstructor.invoke(initialCapacity);
        } catch (RuntimeException e) {
          throw e;
        } catch (Throwable e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public Collection newRootCollection() {
        Preconditions.checkArgument(mode.rootTransfer);
        return new PriorityQueue(initialCapacity, comparator);
      }

      @Override
      public Class<?> getRootType() {
        return PriorityQueue.class;
      }
    }
  }

  private static UnsupportedOperationException unsupported(Class<?> type, String requirement) {
    return new UnsupportedOperationException(
        "Class "
            + type.getName()
            + " requires "
            + requirement
            + " for optimized container support");
  }
}
