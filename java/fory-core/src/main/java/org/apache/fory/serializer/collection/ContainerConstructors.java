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

    private SortedSetFactory(Class<T> type, Class<? extends SortedSet> rootType) {
      this.type = type;
      this.rootType = rootType;
      comparatorConstructor = findCtrHandle(type, Comparator.class);
      sortedSetConstructor = findCtrHandle(type, SortedSet.class);
      noArgConstructor = findCtrHandle(type);
      collectionConstructor = findCtrHandle(type, Collection.class);
      if (comparatorConstructor == null
          && sortedSetConstructor == null
          && noArgConstructor == null
          && collectionConstructor == null) {
        throw unsupported(
            type, "a supported constructor among (), (Comparator), (Collection), or (SortedSet)");
      }
    }

    boolean needsStateTransfer(Comparator comparator) {
      return resolveMode(comparator).rootTransfer;
    }

    T newCollection(Comparator comparator) {
      Mode mode = resolveMode(comparator);
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

    SortedSet newRootCollection(Comparator comparator) {
      Mode mode = resolveMode(comparator);
      Preconditions.checkArgument(mode.rootTransfer);
      return emptySortedSet(comparator);
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

    private SortedMapFactory(Class<T> type, Class<? extends SortedMap> rootType) {
      this.type = type;
      this.rootType = rootType;
      comparatorConstructor = findCtrHandle(type, Comparator.class);
      sortedMapConstructor = findCtrHandle(type, SortedMap.class);
      noArgConstructor = findCtrHandle(type);
      mapConstructor = findCtrHandle(type, Map.class);
      if (comparatorConstructor == null
          && sortedMapConstructor == null
          && noArgConstructor == null
          && mapConstructor == null) {
        throw unsupported(
            type, "a supported constructor among (), (Comparator), (Map), or (SortedMap)");
      }
    }

    boolean needsStateTransfer(Comparator comparator) {
      return resolveMode(comparator).rootTransfer;
    }

    T newMap(Comparator comparator) {
      Mode mode = resolveMode(comparator);
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

    SortedMap newRootMap(Comparator comparator) {
      Mode mode = resolveMode(comparator);
      Preconditions.checkArgument(mode.rootTransfer);
      return emptySortedMap(comparator);
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

    private PriorityQueueFactory(Class<T> type) {
      this.type = type;
      comparatorConstructor = findCtrHandle(type, Comparator.class);
      capacityComparatorConstructor = findCtrHandle(type, int.class, Comparator.class);
      priorityQueueConstructor = findCtrHandle(type, PriorityQueue.class);
      sortedSetConstructor = findCtrHandle(type, SortedSet.class);
      noArgConstructor = findCtrHandle(type);
      capacityConstructor = findCtrHandle(type, int.class);
      collectionConstructor = findCtrHandle(type, Collection.class);
      if (comparatorConstructor == null
          && capacityComparatorConstructor == null
          && priorityQueueConstructor == null
          && sortedSetConstructor == null
          && noArgConstructor == null
          && capacityConstructor == null
          && collectionConstructor == null) {
        throw unsupported(
            type,
            "a supported constructor among (), (int), (Comparator), (int, Comparator),"
                + " (Collection), (PriorityQueue), or (SortedSet)");
      }
    }

    boolean needsStateTransfer(Comparator comparator, int sizeHint) {
      return resolveMode(comparator, sizeHint).rootTransfer;
    }

    T newCollection(Comparator comparator, int sizeHint) {
      int initialCapacity = Math.max(1, sizeHint);
      Mode mode = resolveMode(comparator, sizeHint);
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

    PriorityQueue newRootCollection(Comparator comparator, int sizeHint) {
      Mode mode = resolveMode(comparator, sizeHint);
      Preconditions.checkArgument(mode.rootTransfer);
      return new PriorityQueue(Math.max(1, sizeHint), comparator);
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
