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

import java.io.Serializable;
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
import org.testng.Assert;
import org.testng.annotations.Test;

public class ContainerConstructorsTest {
  private static final class ReverseStringComparator implements Comparator<String>, Serializable {
    @Override
    public int compare(String left, String right) {
      return right.compareTo(left);
    }
  }

  public static class ComparatorOnlyTreeSet extends TreeSet<String> {
    private final Comparator<? super String> seenComparator;

    public ComparatorOnlyTreeSet(Comparator<? super String> comparator) {
      super(comparator);
      seenComparator = comparator;
    }
  }

  public static class SortedSetOnlyTreeSet extends TreeSet<String> {
    public SortedSetOnlyTreeSet(SortedSet<String> values) {
      super(values);
    }
  }

  public static class NoArgOnlyTreeSet extends TreeSet<String> {
    public NoArgOnlyTreeSet() {}
  }

  public static class CollectionOnlyConcurrentSkipListSet extends ConcurrentSkipListSet<String> {
    public CollectionOnlyConcurrentSkipListSet(Collection<String> values) {
      super(values);
    }
  }

  public static class UnsupportedTreeSet extends TreeSet<String> {
    public UnsupportedTreeSet(String ignored) {}
  }

  public static class ComparatorOnlyTreeMap extends TreeMap<String, Integer> {
    private final Comparator<? super String> seenComparator;

    public ComparatorOnlyTreeMap(Comparator<? super String> comparator) {
      super(comparator);
      seenComparator = comparator;
    }
  }

  public static class SortedMapOnlyTreeMap extends TreeMap<String, Integer> {
    public SortedMapOnlyTreeMap(SortedMap<String, Integer> values) {
      super(values);
    }
  }

  public static class NoArgOnlyTreeMap extends TreeMap<String, Integer> {
    public NoArgOnlyTreeMap() {}
  }

  public static class MapOnlyConcurrentSkipListMap extends ConcurrentSkipListMap<String, Integer> {
    public MapOnlyConcurrentSkipListMap(Map<String, Integer> values) {
      super(values);
    }
  }

  public static class UnsupportedTreeMap extends TreeMap<String, Integer> {
    public UnsupportedTreeMap(String ignored) {}
  }

  public static class CapacityComparatorPriorityQueue extends PriorityQueue<String> {
    private final int seenCapacity;
    private final Comparator<? super String> seenComparator;

    public CapacityComparatorPriorityQueue(
        int initialCapacity, Comparator<? super String> comparator) {
      super(initialCapacity, comparator);
      seenCapacity = initialCapacity;
      seenComparator = comparator;
    }
  }

  public static class ComparatorOnlyPriorityQueue extends PriorityQueue<String> {
    private final Comparator<? super String> seenComparator;

    public ComparatorOnlyPriorityQueue(Comparator<? super String> comparator) {
      super(1, comparator);
      seenComparator = comparator;
    }
  }

  public static class NoArgOnlyPriorityQueue extends PriorityQueue<String> {
    public NoArgOnlyPriorityQueue() {}
  }

  public static class CapacityOnlyPriorityQueue extends PriorityQueue<String> {
    private final int seenCapacity;

    public CapacityOnlyPriorityQueue(int initialCapacity) {
      super(initialCapacity);
      seenCapacity = initialCapacity;
    }
  }

  public static class PriorityQueueOnlyPriorityQueue extends PriorityQueue<String> {
    public PriorityQueueOnlyPriorityQueue(PriorityQueue<String> values) {
      super(values);
    }
  }

  public static class SortedSetOnlyPriorityQueue extends PriorityQueue<String> {
    public SortedSetOnlyPriorityQueue(SortedSet<String> values) {
      super(values);
    }
  }

  public static class CollectionOnlyPriorityQueue extends PriorityQueue<String> {
    public CollectionOnlyPriorityQueue(Collection<String> values) {
      super(values);
    }
  }

  public static class UnsupportedPriorityQueue extends PriorityQueue<String> {
    public UnsupportedPriorityQueue(String ignored) {}
  }

  @Test
  public void testRootTypeSelection() {
    Assert.assertSame(
        ContainerConstructors.getSortedSetRootType(ComparatorOnlyTreeSet.class), TreeSet.class);
    Assert.assertSame(
        ContainerConstructors.getSortedSetRootType(CollectionOnlyConcurrentSkipListSet.class),
        ConcurrentSkipListSet.class);
    Assert.assertSame(
        ContainerConstructors.getSortedMapRootType(ComparatorOnlyTreeMap.class), TreeMap.class);
    Assert.assertSame(
        ContainerConstructors.getSortedMapRootType(MapOnlyConcurrentSkipListMap.class),
        ConcurrentSkipListMap.class);
  }

  @Test
  public void testSortedSetFactoryModes() {
    Comparator<String> comparator = new ReverseStringComparator();
    ContainerConstructors.SortedSetFactory<ComparatorOnlyTreeSet> comparatorFactory =
        ContainerConstructors.sortedSetFactory(ComparatorOnlyTreeSet.class, TreeSet.class);
    Assert.assertFalse(comparatorFactory.needsStateTransfer(comparator));
    Assert.assertSame(comparatorFactory.getRootType(), TreeSet.class);
    ComparatorOnlyTreeSet directSet = comparatorFactory.newCollection(comparator);
    Assert.assertSame(directSet.seenComparator, comparator);
    Assert.assertSame(directSet.comparator(), comparator);

    ContainerConstructors.SortedSetFactory<SortedSetOnlyTreeSet> sortedSetFactory =
        ContainerConstructors.sortedSetFactory(SortedSetOnlyTreeSet.class, TreeSet.class);
    Assert.assertTrue(sortedSetFactory.needsStateTransfer(comparator));
    SortedSet<String> sortedSetRoot = sortedSetFactory.newRootCollection(comparator);
    Assert.assertSame(sortedSetRoot.getClass(), TreeSet.class);
    Assert.assertSame(sortedSetRoot.comparator(), comparator);

    ContainerConstructors.SortedSetFactory<NoArgOnlyTreeSet> noArgFactory =
        ContainerConstructors.sortedSetFactory(NoArgOnlyTreeSet.class, TreeSet.class);
    Assert.assertFalse(noArgFactory.needsStateTransfer(null));
    Assert.assertNull(noArgFactory.newCollection(null).comparator());

    ContainerConstructors.SortedSetFactory<CollectionOnlyConcurrentSkipListSet> collectionFactory =
        ContainerConstructors.sortedSetFactory(
            CollectionOnlyConcurrentSkipListSet.class, ConcurrentSkipListSet.class);
    Assert.assertTrue(collectionFactory.needsStateTransfer(null));
    SortedSet<String> concurrentRoot = collectionFactory.newRootCollection(null);
    Assert.assertSame(collectionFactory.getRootType(), ConcurrentSkipListSet.class);
    Assert.assertSame(concurrentRoot.getClass(), ConcurrentSkipListSet.class);
    Assert.assertNull(concurrentRoot.comparator());
  }

  @Test
  public void testSortedSetFactoryUnsupportedCases() {
    ContainerConstructors.SortedSetFactory<CollectionOnlyConcurrentSkipListSet> collectionFactory =
        ContainerConstructors.sortedSetFactory(
            CollectionOnlyConcurrentSkipListSet.class, ConcurrentSkipListSet.class);
    UnsupportedOperationException comparatorException =
        Assert.expectThrows(
            UnsupportedOperationException.class,
            () -> collectionFactory.needsStateTransfer(new ReverseStringComparator()));
    Assert.assertTrue(
        comparatorException.getMessage().contains("comparator-preserving constructor"));
    Assert.assertTrue(
        comparatorException
            .getMessage()
            .contains(CollectionOnlyConcurrentSkipListSet.class.getName()));

    UnsupportedOperationException unsupportedFactoryException =
        Assert.expectThrows(
            UnsupportedOperationException.class,
            () -> ContainerConstructors.sortedSetFactory(UnsupportedTreeSet.class, TreeSet.class));
    Assert.assertTrue(
        unsupportedFactoryException
            .getMessage()
            .contains(
                "a supported constructor among (), (Comparator), (Collection), or (SortedSet)"));
  }

  @Test
  public void testSortedMapFactoryModes() {
    Comparator<String> comparator = new ReverseStringComparator();
    ContainerConstructors.SortedMapFactory<ComparatorOnlyTreeMap> comparatorFactory =
        ContainerConstructors.sortedMapFactory(ComparatorOnlyTreeMap.class, TreeMap.class);
    Assert.assertFalse(comparatorFactory.needsStateTransfer(comparator));
    Assert.assertSame(comparatorFactory.getRootType(), TreeMap.class);
    ComparatorOnlyTreeMap directMap = comparatorFactory.newMap(comparator);
    Assert.assertSame(directMap.seenComparator, comparator);
    Assert.assertSame(directMap.comparator(), comparator);

    ContainerConstructors.SortedMapFactory<SortedMapOnlyTreeMap> sortedMapFactory =
        ContainerConstructors.sortedMapFactory(SortedMapOnlyTreeMap.class, TreeMap.class);
    Assert.assertTrue(sortedMapFactory.needsStateTransfer(comparator));
    SortedMap<String, Integer> sortedMapRoot = sortedMapFactory.newRootMap(comparator);
    Assert.assertSame(sortedMapRoot.getClass(), TreeMap.class);
    Assert.assertSame(sortedMapRoot.comparator(), comparator);

    ContainerConstructors.SortedMapFactory<NoArgOnlyTreeMap> noArgFactory =
        ContainerConstructors.sortedMapFactory(NoArgOnlyTreeMap.class, TreeMap.class);
    Assert.assertFalse(noArgFactory.needsStateTransfer(null));
    Assert.assertNull(noArgFactory.newMap(null).comparator());

    ContainerConstructors.SortedMapFactory<MapOnlyConcurrentSkipListMap> mapFactory =
        ContainerConstructors.sortedMapFactory(
            MapOnlyConcurrentSkipListMap.class, ConcurrentSkipListMap.class);
    Assert.assertTrue(mapFactory.needsStateTransfer(null));
    SortedMap<String, Integer> concurrentRoot = mapFactory.newRootMap(null);
    Assert.assertSame(mapFactory.getRootType(), ConcurrentSkipListMap.class);
    Assert.assertSame(concurrentRoot.getClass(), ConcurrentSkipListMap.class);
    Assert.assertNull(concurrentRoot.comparator());
  }

  @Test
  public void testSortedMapFactoryUnsupportedCases() {
    ContainerConstructors.SortedMapFactory<MapOnlyConcurrentSkipListMap> mapFactory =
        ContainerConstructors.sortedMapFactory(
            MapOnlyConcurrentSkipListMap.class, ConcurrentSkipListMap.class);
    UnsupportedOperationException comparatorException =
        Assert.expectThrows(
            UnsupportedOperationException.class,
            () -> mapFactory.needsStateTransfer(new ReverseStringComparator()));
    Assert.assertTrue(
        comparatorException.getMessage().contains("comparator-preserving constructor"));
    Assert.assertTrue(
        comparatorException.getMessage().contains(MapOnlyConcurrentSkipListMap.class.getName()));

    UnsupportedOperationException unsupportedFactoryException =
        Assert.expectThrows(
            UnsupportedOperationException.class,
            () -> ContainerConstructors.sortedMapFactory(UnsupportedTreeMap.class, TreeMap.class));
    Assert.assertTrue(
        unsupportedFactoryException
            .getMessage()
            .contains("a supported constructor among (), (Comparator), (Map), or (SortedMap)"));
  }

  @Test
  public void testPriorityQueueFactoryDirectModes() {
    Comparator<String> comparator = new ReverseStringComparator();
    ContainerConstructors.PriorityQueueFactory<CapacityComparatorPriorityQueue>
        capacityComparatorFactory =
            ContainerConstructors.priorityQueueFactory(CapacityComparatorPriorityQueue.class);
    Assert.assertFalse(capacityComparatorFactory.needsStateTransfer(comparator, 5));
    Assert.assertSame(capacityComparatorFactory.getRootType(), PriorityQueue.class);
    CapacityComparatorPriorityQueue capacityComparatorQueue =
        capacityComparatorFactory.newCollection(comparator, 5);
    Assert.assertEquals(capacityComparatorQueue.seenCapacity, 5);
    Assert.assertSame(capacityComparatorQueue.seenComparator, comparator);

    ContainerConstructors.PriorityQueueFactory<ComparatorOnlyPriorityQueue> comparatorFactory =
        ContainerConstructors.priorityQueueFactory(ComparatorOnlyPriorityQueue.class);
    Assert.assertFalse(comparatorFactory.needsStateTransfer(comparator, 0));
    ComparatorOnlyPriorityQueue comparatorQueue = comparatorFactory.newCollection(comparator, 0);
    Assert.assertSame(comparatorQueue.seenComparator, comparator);

    ContainerConstructors.PriorityQueueFactory<NoArgOnlyPriorityQueue> noArgFactory =
        ContainerConstructors.priorityQueueFactory(NoArgOnlyPriorityQueue.class);
    Assert.assertFalse(noArgFactory.needsStateTransfer(null, 0));
    Assert.assertNull(noArgFactory.newCollection(null, 0).comparator());

    ContainerConstructors.PriorityQueueFactory<CapacityOnlyPriorityQueue> capacityFactory =
        ContainerConstructors.priorityQueueFactory(CapacityOnlyPriorityQueue.class);
    Assert.assertFalse(capacityFactory.needsStateTransfer(null, 0));
    Assert.assertEquals(capacityFactory.newCollection(null, 0).seenCapacity, 1);
    Assert.assertEquals(capacityFactory.newCollection(null, 7).seenCapacity, 7);
  }

  @Test
  public void testPriorityQueueFactoryRootTransferModes() {
    Comparator<String> comparator = new ReverseStringComparator();
    ContainerConstructors.PriorityQueueFactory<PriorityQueueOnlyPriorityQueue>
        priorityQueueFactory =
            ContainerConstructors.priorityQueueFactory(PriorityQueueOnlyPriorityQueue.class);
    Assert.assertTrue(priorityQueueFactory.needsStateTransfer(comparator, 3));
    PriorityQueue<String> priorityQueueRoot = priorityQueueFactory.newRootCollection(comparator, 3);
    Assert.assertSame(priorityQueueRoot.getClass(), PriorityQueue.class);
    Assert.assertSame(priorityQueueRoot.comparator(), comparator);

    ContainerConstructors.PriorityQueueFactory<SortedSetOnlyPriorityQueue> sortedSetFactory =
        ContainerConstructors.priorityQueueFactory(SortedSetOnlyPriorityQueue.class);
    Assert.assertTrue(sortedSetFactory.needsStateTransfer(comparator, 3));
    PriorityQueue<String> sortedSetRoot = sortedSetFactory.newRootCollection(comparator, 3);
    Assert.assertSame(sortedSetRoot.getClass(), PriorityQueue.class);
    Assert.assertSame(sortedSetRoot.comparator(), comparator);

    ContainerConstructors.PriorityQueueFactory<CollectionOnlyPriorityQueue> collectionFactory =
        ContainerConstructors.priorityQueueFactory(CollectionOnlyPriorityQueue.class);
    Assert.assertTrue(collectionFactory.needsStateTransfer(null, 0));
    PriorityQueue<String> collectionRoot = collectionFactory.newRootCollection(null, 0);
    Assert.assertSame(collectionRoot.getClass(), PriorityQueue.class);
    Assert.assertNull(collectionRoot.comparator());
  }

  @Test
  public void testPriorityQueueFactoryUnsupportedCases() {
    ContainerConstructors.PriorityQueueFactory<CollectionOnlyPriorityQueue> collectionFactory =
        ContainerConstructors.priorityQueueFactory(CollectionOnlyPriorityQueue.class);
    UnsupportedOperationException comparatorException =
        Assert.expectThrows(
            UnsupportedOperationException.class,
            () -> collectionFactory.needsStateTransfer(new ReverseStringComparator(), 3));
    Assert.assertTrue(
        comparatorException.getMessage().contains("comparator-preserving constructor"));
    Assert.assertTrue(
        comparatorException.getMessage().contains(CollectionOnlyPriorityQueue.class.getName()));

    UnsupportedOperationException unsupportedFactoryException =
        Assert.expectThrows(
            UnsupportedOperationException.class,
            () -> ContainerConstructors.priorityQueueFactory(UnsupportedPriorityQueue.class));
    Assert.assertTrue(
        unsupportedFactoryException
            .getMessage()
            .contains(
                "a supported constructor among (), (int), (Comparator), (int, Comparator),"
                    + " (Collection), (PriorityQueue), or (SortedSet)"));
  }
}
