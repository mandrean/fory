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

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.fory.memory.Platform;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ContainerTest {
  private static final int DEFAULT_SORTED_BULK_READ_BUFFER_LIMIT_BYTES = 256 * 1024;

  @Test
  public void testSortedContainerBulkBufferDefaultThreshold() {
    int referenceSize = Platform.UNSAFE.arrayIndexScale(Object[].class);
    int maxSetElements = DEFAULT_SORTED_BULK_READ_BUFFER_LIMIT_BYTES / referenceSize;
    int maxMapElements = DEFAULT_SORTED_BULK_READ_BUFFER_LIMIT_BYTES / (referenceSize * 2);

    Assert.assertTrue(
        SortedContainerBulkAccess.canBulkBufferSortedSet(
            maxSetElements, DEFAULT_SORTED_BULK_READ_BUFFER_LIMIT_BYTES));
    Assert.assertEquals(
        SortedContainerBulkAccess.estimateSortedSetBufferBytes(maxSetElements),
        (long) DEFAULT_SORTED_BULK_READ_BUFFER_LIMIT_BYTES);
    Assert.assertFalse(
        SortedContainerBulkAccess.canBulkBufferSortedSet(
            maxSetElements + 1, DEFAULT_SORTED_BULK_READ_BUFFER_LIMIT_BYTES));

    Assert.assertTrue(
        SortedContainerBulkAccess.canBulkBufferSortedMap(
            maxMapElements, DEFAULT_SORTED_BULK_READ_BUFFER_LIMIT_BYTES));
    Assert.assertEquals(
        SortedContainerBulkAccess.estimateSortedMapBufferBytes(maxMapElements),
        (long) DEFAULT_SORTED_BULK_READ_BUFFER_LIMIT_BYTES);
    Assert.assertFalse(
        SortedContainerBulkAccess.canBulkBufferSortedMap(
            maxMapElements + 1, DEFAULT_SORTED_BULK_READ_BUFFER_LIMIT_BYTES));
  }

  @Test
  public void testSortedContainerBulkBufferCustomAndZeroThreshold() {
    int setLimit = (int) SortedContainerBulkAccess.estimateSortedSetBufferBytes(2);
    int mapLimit = (int) SortedContainerBulkAccess.estimateSortedMapBufferBytes(2);

    Assert.assertTrue(SortedContainerBulkAccess.canBulkBufferSortedSet(2, setLimit));
    Assert.assertFalse(SortedContainerBulkAccess.canBulkBufferSortedSet(3, setLimit));
    Assert.assertTrue(SortedContainerBulkAccess.canBulkBufferSortedMap(2, mapLimit));
    Assert.assertFalse(SortedContainerBulkAccess.canBulkBufferSortedMap(3, mapLimit));

    Assert.assertFalse(SortedContainerBulkAccess.canBulkBufferSortedSet(1, 0));
    Assert.assertFalse(SortedContainerBulkAccess.canBulkBufferSortedMap(1, 0));
  }

  @Test
  public void testSortedSetAccumulatorFallsBackOnOutOfOrderInput() {
    SortedSetAccumulator<String> accumulator = new SortedSetAccumulator<>(new TreeSet<>(), 4);

    accumulator.add("b");
    accumulator.add("a");
    accumulator.add("c");

    Assert.assertEquals(new ArrayList<>(accumulator.build()), Arrays.asList("a", "b", "c"));
  }

  @Test
  public void testSortedSetAccumulatorFallsBackOnComparatorFailure() {
    SortedSetAccumulator<String> accumulator =
        new SortedSetAccumulator<>(new ThrowingComparatorSortedSet(), 4);

    accumulator.add("alpha");
    accumulator.add("boom");
    accumulator.add("charlie");

    Assert.assertEquals(
        new ArrayList<>(accumulator.build()), Arrays.asList("alpha", "boom", "charlie"));
  }

  @Test
  public void testSortedMapAccumulatorFallsBackOnOutOfOrderInput() {
    SortedMapAccumulator<String, Integer> accumulator =
        new SortedMapAccumulator<>(new TreeMap<>(), 4);

    accumulator.put("b", 2);
    accumulator.put("a", 1);
    accumulator.put("c", 3);

    SortedMap<String, Integer> map = accumulator.build();
    Assert.assertEquals(new ArrayList<>(map.keySet()), Arrays.asList("a", "b", "c"));
    Assert.assertEquals(map.get("a"), Integer.valueOf(1));
    Assert.assertEquals(map.get("b"), Integer.valueOf(2));
    Assert.assertEquals(map.get("c"), Integer.valueOf(3));
  }

  @Test
  public void testSortedMapAccumulatorFallsBackOnComparatorFailure() {
    SortedMapAccumulator<String, Integer> accumulator =
        new SortedMapAccumulator<>(new ThrowingComparatorSortedMap(), 4);

    accumulator.put("alpha", 1);
    accumulator.put("boom", 2);
    accumulator.put("charlie", 3);

    SortedMap<String, Integer> map = accumulator.build();
    Assert.assertEquals(new ArrayList<>(map.keySet()), Arrays.asList("alpha", "boom", "charlie"));
    Assert.assertEquals(map.get("alpha"), Integer.valueOf(1));
    Assert.assertEquals(map.get("boom"), Integer.valueOf(2));
    Assert.assertEquals(map.get("charlie"), Integer.valueOf(3));
  }

  @Test(timeOut = 60_000)
  public void testJvmSortedContainerBulkAccessThreadSafety() throws Exception {
    assertConcurrentBulkAccess(
        JvmSortedContainerBulkAccess::canBulkReadSortedSet,
        InheritedTreeSet.class,
        OverrideAddTreeSet.class,
        OverrideAddAllTreeSet.class);
    assertConcurrentBulkAccess(
        JvmSortedContainerBulkAccess::canBulkReadSortedMap,
        InheritedTreeMap.class,
        OverridePutTreeMap.class,
        OverridePutAllTreeMap.class);
  }

  @Test(timeOut = 60_000)
  public void testNativeImageSortedContainerBulkAccessThreadSafety() throws Exception {
    assertConcurrentBulkAccess(
        NativeImageSortedContainerBulkAccess::canBulkReadSortedSet,
        InheritedTreeSet.class,
        OverrideAddTreeSet.class,
        OverrideAddAllTreeSet.class);
    assertConcurrentBulkAccess(
        NativeImageSortedContainerBulkAccess::canBulkReadSortedMap,
        InheritedTreeMap.class,
        OverridePutTreeMap.class,
        OverridePutAllTreeMap.class);
  }

  private static void assertConcurrentBulkAccess(
      BulkReadChecker checker, Class<?> positiveType, Class<?>... negativeTypes) throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(8);
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int task = 0; task < 32; task++) {
        futures.add(
            executor.submit(
                () -> {
                  for (int i = 0; i < 2_000; i++) {
                    Assert.assertTrue(checker.test(positiveType));
                    for (Class<?> negativeType : negativeTypes) {
                      Assert.assertFalse(checker.test(negativeType));
                    }
                  }
                }));
      }
      for (Future<?> future : futures) {
        future.get();
      }
    } finally {
      executor.shutdownNow();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  private interface BulkReadChecker {
    boolean test(Class<?> type);
  }

  private static final class ThrowingComparatorSortedSet extends AbstractSet<String>
      implements SortedSet<String> {
    private final TreeSet<String> delegate = new TreeSet<>();

    @Override
    public Comparator<? super String> comparator() {
      return (left, right) -> {
        if ("boom".equals(right)) {
          throw new IllegalStateException("boom");
        }
        return left.compareTo(right);
      };
    }

    @Override
    public Iterator<String> iterator() {
      return delegate.iterator();
    }

    @Override
    public int size() {
      return delegate.size();
    }

    @Override
    public boolean add(String value) {
      return delegate.add(value);
    }

    @Override
    public SortedSet<String> subSet(String fromElement, String toElement) {
      return delegate.subSet(fromElement, toElement);
    }

    @Override
    public SortedSet<String> headSet(String toElement) {
      return delegate.headSet(toElement);
    }

    @Override
    public SortedSet<String> tailSet(String fromElement) {
      return delegate.tailSet(fromElement);
    }

    @Override
    public String first() {
      return delegate.first();
    }

    @Override
    public String last() {
      return delegate.last();
    }
  }

  private static final class ThrowingComparatorSortedMap extends AbstractMap<String, Integer>
      implements SortedMap<String, Integer> {
    private final TreeMap<String, Integer> delegate = new TreeMap<>();

    @Override
    public Comparator<? super String> comparator() {
      return (left, right) -> {
        if ("boom".equals(right)) {
          throw new IllegalStateException("boom");
        }
        return left.compareTo(right);
      };
    }

    @Override
    public Integer put(String key, Integer value) {
      return delegate.put(key, value);
    }

    @Override
    public SortedMap<String, Integer> subMap(String fromKey, String toKey) {
      return delegate.subMap(fromKey, toKey);
    }

    @Override
    public SortedMap<String, Integer> headMap(String toKey) {
      return delegate.headMap(toKey);
    }

    @Override
    public SortedMap<String, Integer> tailMap(String fromKey) {
      return delegate.tailMap(fromKey);
    }

    @Override
    public String firstKey() {
      return delegate.firstKey();
    }

    @Override
    public String lastKey() {
      return delegate.lastKey();
    }

    @Override
    public java.util.Set<Entry<String, Integer>> entrySet() {
      return delegate.entrySet();
    }
  }

  private static class InheritedTreeSet extends TreeSet<String> {}

  private static final class OverrideAddTreeSet extends TreeSet<String> {
    @Override
    public boolean add(String value) {
      return super.add(value);
    }
  }

  private static final class OverrideAddAllTreeSet extends TreeSet<String> {
    @Override
    public boolean addAll(Collection<? extends String> values) {
      return super.addAll(values);
    }
  }

  private static class InheritedTreeMap extends TreeMap<String, String> {}

  private static final class OverridePutTreeMap extends TreeMap<String, String> {
    @Override
    public String put(String key, String value) {
      return super.put(key, value);
    }
  }

  private static final class OverridePutAllTreeMap extends TreeMap<String, String> {
    @Override
    public void putAll(Map<? extends String, ? extends String> map) {
      super.putAll(map);
    }
  }
}
