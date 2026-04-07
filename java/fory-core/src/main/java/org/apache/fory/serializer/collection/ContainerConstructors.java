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

@SuppressWarnings({"rawtypes", "unchecked"})
final class ContainerConstructors {
  private ContainerConstructors() {}

  enum Kind {
    DIRECT,
    ROOT_TRANSFER
  }

  /**
   * Resolved construction plan for an optimized collection serializer.
   *
   * <p>Direct plans allocate the final leaf instance immediately. Root-transfer plans populate a
   * JDK root container first and then transfer only the inherited root-container state into the
   * leaf instance.
   */
  abstract static class CollectionConstruction<T extends Collection> {
    private final Kind kind;

    private CollectionConstruction(Kind kind) {
      this.kind = kind;
    }

    final Kind getKind() {
      return kind;
    }
  }

  abstract static class DirectCollectionConstruction<T extends Collection>
      extends CollectionConstruction<T> {
    private DirectCollectionConstruction() {
      super(Kind.DIRECT);
    }

    abstract T newCollection();
  }

  abstract static class RootTransferCollectionConstruction<T extends Collection>
      extends CollectionConstruction<T> {
    private final Class<?> rootType;

    private RootTransferCollectionConstruction(Class<?> rootType) {
      super(Kind.ROOT_TRANSFER);
      this.rootType = rootType;
    }

    abstract Collection newRootCollection();

    final Class<?> getRootType() {
      return rootType;
    }
  }

  /**
   * Resolved construction plan for an optimized map serializer.
   *
   * <p>Direct plans allocate the final leaf instance immediately. Root-transfer plans populate a
   * JDK root map first and then transfer only the inherited root-map state into the leaf instance.
   */
  abstract static class MapConstruction<T extends Map> {
    private final Kind kind;

    private MapConstruction(Kind kind) {
      this.kind = kind;
    }

    final Kind getKind() {
      return kind;
    }
  }

  abstract static class DirectMapConstruction<T extends Map> extends MapConstruction<T> {
    private DirectMapConstruction() {
      super(Kind.DIRECT);
    }

    abstract T newMap();
  }

  abstract static class RootTransferMapConstruction<T extends Map> extends MapConstruction<T> {
    private final Class<?> rootType;

    private RootTransferMapConstruction(Class<?> rootType) {
      super(Kind.ROOT_TRANSFER);
      this.rootType = rootType;
    }

    abstract Map newRootMap();

    final Class<?> getRootType() {
      return rootType;
    }
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
      DIRECT_COMPARATOR,
      ROOT_TRANSFER_SORTED_SET,
      DIRECT_NO_ARG,
      ROOT_TRANSFER_COLLECTION
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
      Mode mode = resolveMode(comparator);
      switch (mode) {
        case DIRECT_COMPARATOR:
        case DIRECT_NO_ARG:
          return new DirectSortedSetConstruction(comparator, mode);
        case ROOT_TRANSFER_SORTED_SET:
        case ROOT_TRANSFER_COLLECTION:
          return new RootTransferSortedSetConstruction(comparator);
        default:
          throw new IllegalStateException("Unexpected mode " + mode);
      }
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

    private final class DirectSortedSetConstruction extends DirectCollectionConstruction<T> {
      private final Comparator comparator;
      private final Mode mode;

      private DirectSortedSetConstruction(Comparator comparator, Mode mode) {
        this.comparator = comparator;
        this.mode = mode;
      }

      @Override
      public T newCollection() {
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
    }

    private final class RootTransferSortedSetConstruction
        extends RootTransferCollectionConstruction<T> {
      private final Comparator comparator;

      private RootTransferSortedSetConstruction(Comparator comparator) {
        super(rootType);
        this.comparator = comparator;
      }

      @Override
      public Collection newRootCollection() {
        return emptySortedSet(comparator);
      }
    }
  }

  static final class SortedMapFactory<T extends Map> {
    private enum Mode {
      DIRECT_COMPARATOR,
      ROOT_TRANSFER_SORTED_MAP,
      DIRECT_NO_ARG,
      ROOT_TRANSFER_MAP
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
      Mode mode = resolveMode(comparator);
      switch (mode) {
        case DIRECT_COMPARATOR:
        case DIRECT_NO_ARG:
          return new DirectSortedMapConstruction(comparator, mode);
        case ROOT_TRANSFER_SORTED_MAP:
        case ROOT_TRANSFER_MAP:
          return new RootTransferSortedMapConstruction(comparator);
        default:
          throw new IllegalStateException("Unexpected mode " + mode);
      }
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

    private final class DirectSortedMapConstruction extends DirectMapConstruction<T> {
      private final Comparator comparator;
      private final Mode mode;

      private DirectSortedMapConstruction(Comparator comparator, Mode mode) {
        this.comparator = comparator;
        this.mode = mode;
      }

      @Override
      public T newMap() {
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
    }

    private final class RootTransferSortedMapConstruction extends RootTransferMapConstruction<T> {
      private final Comparator comparator;

      private RootTransferSortedMapConstruction(Comparator comparator) {
        super(rootType);
        this.comparator = comparator;
      }

      @Override
      public Map newRootMap() {
        return emptySortedMap(comparator);
      }
    }
  }

  static final class PriorityQueueFactory<T extends PriorityQueue> {
    private enum Mode {
      DIRECT_CAPACITY_COMPARATOR,
      DIRECT_COMPARATOR,
      ROOT_TRANSFER_PRIORITY_QUEUE,
      ROOT_TRANSFER_SORTED_SET,
      DIRECT_NO_ARG,
      DIRECT_CAPACITY,
      ROOT_TRANSFER_COLLECTION
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
      Mode mode = resolveMode(comparator, sizeHint);
      switch (mode) {
        case DIRECT_CAPACITY_COMPARATOR:
        case DIRECT_COMPARATOR:
        case DIRECT_NO_ARG:
        case DIRECT_CAPACITY:
          return new DirectPriorityQueueConstruction(comparator, sizeHint, mode);
        case ROOT_TRANSFER_PRIORITY_QUEUE:
        case ROOT_TRANSFER_SORTED_SET:
        case ROOT_TRANSFER_COLLECTION:
          return new RootTransferPriorityQueueConstruction(comparator, sizeHint);
        default:
          throw new IllegalStateException("Unexpected mode " + mode);
      }
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

    private final class DirectPriorityQueueConstruction extends DirectCollectionConstruction<T> {
      private final Comparator comparator;
      private final int initialCapacity;
      private final Mode mode;

      private DirectPriorityQueueConstruction(Comparator comparator, int sizeHint, Mode mode) {
        this.comparator = comparator;
        initialCapacity = Math.max(1, sizeHint);
        this.mode = mode;
      }

      @Override
      public T newCollection() {
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
    }

    private final class RootTransferPriorityQueueConstruction
        extends RootTransferCollectionConstruction<T> {
      private final Comparator comparator;
      private final int initialCapacity;

      private RootTransferPriorityQueueConstruction(Comparator comparator, int sizeHint) {
        super(PriorityQueue.class);
        this.comparator = comparator;
        initialCapacity = Math.max(1, sizeHint);
      }

      @Override
      public Collection newRootCollection() {
        return new PriorityQueue(initialCapacity, comparator);
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
