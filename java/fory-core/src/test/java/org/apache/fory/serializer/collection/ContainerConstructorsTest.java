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

  private static <T extends Collection>
      ContainerConstructors.DirectCollectionConstruction<T> assertDirectCollectionPlan(
          ContainerConstructors.CollectionConstruction<T> construction) {
    Assert.assertSame(construction.getKind(), ContainerConstructors.Kind.DIRECT);
    return (ContainerConstructors.DirectCollectionConstruction<T>) construction;
  }

  private static <T extends Collection>
      ContainerConstructors.RootTransferCollectionConstruction<T> assertRootTransferCollectionPlan(
          ContainerConstructors.CollectionConstruction<T> construction) {
    Assert.assertSame(construction.getKind(), ContainerConstructors.Kind.ROOT_TRANSFER);
    return (ContainerConstructors.RootTransferCollectionConstruction<T>) construction;
  }

  private static <T extends Map> ContainerConstructors.DirectMapConstruction<T> assertDirectMapPlan(
      ContainerConstructors.MapConstruction<T> construction) {
    Assert.assertSame(construction.getKind(), ContainerConstructors.Kind.DIRECT);
    return (ContainerConstructors.DirectMapConstruction<T>) construction;
  }

  private static <T extends Map>
      ContainerConstructors.RootTransferMapConstruction<T> assertRootTransferMapPlan(
          ContainerConstructors.MapConstruction<T> construction) {
    Assert.assertSame(construction.getKind(), ContainerConstructors.Kind.ROOT_TRANSFER);
    return (ContainerConstructors.RootTransferMapConstruction<T>) construction;
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
    Assert.assertTrue(comparatorFactory.isSupported());
    ContainerConstructors.DirectCollectionConstruction<ComparatorOnlyTreeSet>
        directComparatorConstruction =
            assertDirectCollectionPlan(comparatorFactory.newConstruction(comparator));
    ComparatorOnlyTreeSet directSet = directComparatorConstruction.newCollection();
    Assert.assertSame(directSet.seenComparator, comparator);
    Assert.assertSame(directSet.comparator(), comparator);

    ContainerConstructors.SortedSetFactory<SortedSetOnlyTreeSet> sortedSetFactory =
        ContainerConstructors.sortedSetFactory(SortedSetOnlyTreeSet.class, TreeSet.class);
    Assert.assertTrue(sortedSetFactory.isSupported());
    ContainerConstructors.RootTransferCollectionConstruction<SortedSetOnlyTreeSet>
        rootTransferSortedSetConstruction =
            assertRootTransferCollectionPlan(sortedSetFactory.newConstruction(comparator));
    SortedSet<String> sortedSetRoot =
        (SortedSet<String>) rootTransferSortedSetConstruction.newRootCollection();
    Assert.assertSame(rootTransferSortedSetConstruction.getRootType(), TreeSet.class);
    Assert.assertSame(sortedSetRoot.getClass(), TreeSet.class);
    Assert.assertSame(sortedSetRoot.comparator(), comparator);

    ContainerConstructors.SortedSetFactory<NoArgOnlyTreeSet> noArgFactory =
        ContainerConstructors.sortedSetFactory(NoArgOnlyTreeSet.class, TreeSet.class);
    Assert.assertTrue(noArgFactory.isSupported());
    ContainerConstructors.DirectCollectionConstruction<NoArgOnlyTreeSet> directNoArgConstruction =
        assertDirectCollectionPlan(noArgFactory.newConstruction(null));
    Assert.assertNull(directNoArgConstruction.newCollection().comparator());

    ContainerConstructors.SortedSetFactory<CollectionOnlyConcurrentSkipListSet> collectionFactory =
        ContainerConstructors.sortedSetFactory(
            CollectionOnlyConcurrentSkipListSet.class, ConcurrentSkipListSet.class);
    Assert.assertTrue(collectionFactory.isSupported());
    ContainerConstructors.RootTransferCollectionConstruction<CollectionOnlyConcurrentSkipListSet>
        rootTransferCollectionConstruction =
            assertRootTransferCollectionPlan(collectionFactory.newConstruction(null));
    SortedSet<String> concurrentRoot =
        (SortedSet<String>) rootTransferCollectionConstruction.newRootCollection();
    Assert.assertSame(
        rootTransferCollectionConstruction.getRootType(), ConcurrentSkipListSet.class);
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
            () -> collectionFactory.newConstruction(new ReverseStringComparator()));
    Assert.assertTrue(
        comparatorException.getMessage().contains("comparator-preserving constructor"));
    Assert.assertTrue(
        comparatorException
            .getMessage()
            .contains(CollectionOnlyConcurrentSkipListSet.class.getName()));

    ContainerConstructors.SortedSetFactory<UnsupportedTreeSet> unsupportedFactory =
        ContainerConstructors.sortedSetFactory(UnsupportedTreeSet.class, TreeSet.class);
    Assert.assertFalse(unsupportedFactory.isSupported());
    UnsupportedOperationException unsupportedFactoryException =
        Assert.expectThrows(
            UnsupportedOperationException.class, unsupportedFactory::checkSupported);
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
    Assert.assertTrue(comparatorFactory.isSupported());
    ContainerConstructors.DirectMapConstruction<ComparatorOnlyTreeMap>
        directComparatorConstruction =
            assertDirectMapPlan(comparatorFactory.newConstruction(comparator));
    ComparatorOnlyTreeMap directMap = directComparatorConstruction.newMap();
    Assert.assertSame(directMap.seenComparator, comparator);
    Assert.assertSame(directMap.comparator(), comparator);

    ContainerConstructors.SortedMapFactory<SortedMapOnlyTreeMap> sortedMapFactory =
        ContainerConstructors.sortedMapFactory(SortedMapOnlyTreeMap.class, TreeMap.class);
    Assert.assertTrue(sortedMapFactory.isSupported());
    ContainerConstructors.RootTransferMapConstruction<SortedMapOnlyTreeMap>
        rootTransferSortedMapConstruction =
            assertRootTransferMapPlan(sortedMapFactory.newConstruction(comparator));
    SortedMap<String, Integer> sortedMapRoot =
        (SortedMap<String, Integer>) rootTransferSortedMapConstruction.newRootMap();
    Assert.assertSame(rootTransferSortedMapConstruction.getRootType(), TreeMap.class);
    Assert.assertSame(sortedMapRoot.getClass(), TreeMap.class);
    Assert.assertSame(sortedMapRoot.comparator(), comparator);

    ContainerConstructors.SortedMapFactory<NoArgOnlyTreeMap> noArgFactory =
        ContainerConstructors.sortedMapFactory(NoArgOnlyTreeMap.class, TreeMap.class);
    Assert.assertTrue(noArgFactory.isSupported());
    ContainerConstructors.DirectMapConstruction<NoArgOnlyTreeMap> directNoArgConstruction =
        assertDirectMapPlan(noArgFactory.newConstruction(null));
    Assert.assertNull(directNoArgConstruction.newMap().comparator());

    ContainerConstructors.SortedMapFactory<MapOnlyConcurrentSkipListMap> mapFactory =
        ContainerConstructors.sortedMapFactory(
            MapOnlyConcurrentSkipListMap.class, ConcurrentSkipListMap.class);
    Assert.assertTrue(mapFactory.isSupported());
    ContainerConstructors.RootTransferMapConstruction<MapOnlyConcurrentSkipListMap>
        rootTransferMapConstruction = assertRootTransferMapPlan(mapFactory.newConstruction(null));
    SortedMap<String, Integer> concurrentRoot =
        (SortedMap<String, Integer>) rootTransferMapConstruction.newRootMap();
    Assert.assertSame(rootTransferMapConstruction.getRootType(), ConcurrentSkipListMap.class);
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
            () -> mapFactory.newConstruction(new ReverseStringComparator()));
    Assert.assertTrue(
        comparatorException.getMessage().contains("comparator-preserving constructor"));
    Assert.assertTrue(
        comparatorException.getMessage().contains(MapOnlyConcurrentSkipListMap.class.getName()));

    ContainerConstructors.SortedMapFactory<UnsupportedTreeMap> unsupportedFactory =
        ContainerConstructors.sortedMapFactory(UnsupportedTreeMap.class, TreeMap.class);
    Assert.assertFalse(unsupportedFactory.isSupported());
    UnsupportedOperationException unsupportedFactoryException =
        Assert.expectThrows(
            UnsupportedOperationException.class, unsupportedFactory::checkSupported);
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
    Assert.assertTrue(capacityComparatorFactory.isSupported());
    ContainerConstructors.DirectCollectionConstruction<CapacityComparatorPriorityQueue>
        directCapacityComparatorConstruction =
            assertDirectCollectionPlan(capacityComparatorFactory.newConstruction(comparator, 5));
    CapacityComparatorPriorityQueue capacityComparatorQueue =
        directCapacityComparatorConstruction.newCollection();
    Assert.assertEquals(capacityComparatorQueue.seenCapacity, 5);
    Assert.assertSame(capacityComparatorQueue.seenComparator, comparator);

    ContainerConstructors.PriorityQueueFactory<ComparatorOnlyPriorityQueue> comparatorFactory =
        ContainerConstructors.priorityQueueFactory(ComparatorOnlyPriorityQueue.class);
    Assert.assertTrue(comparatorFactory.isSupported());
    ContainerConstructors.DirectCollectionConstruction<ComparatorOnlyPriorityQueue>
        directComparatorConstruction =
            assertDirectCollectionPlan(comparatorFactory.newConstruction(comparator, 0));
    ComparatorOnlyPriorityQueue comparatorQueue = directComparatorConstruction.newCollection();
    Assert.assertSame(comparatorQueue.seenComparator, comparator);

    ContainerConstructors.PriorityQueueFactory<NoArgOnlyPriorityQueue> noArgFactory =
        ContainerConstructors.priorityQueueFactory(NoArgOnlyPriorityQueue.class);
    Assert.assertTrue(noArgFactory.isSupported());
    ContainerConstructors.DirectCollectionConstruction<NoArgOnlyPriorityQueue>
        directNoArgConstruction = assertDirectCollectionPlan(noArgFactory.newConstruction(null, 0));
    Assert.assertNull(directNoArgConstruction.newCollection().comparator());

    ContainerConstructors.PriorityQueueFactory<CapacityOnlyPriorityQueue> capacityFactory =
        ContainerConstructors.priorityQueueFactory(CapacityOnlyPriorityQueue.class);
    Assert.assertTrue(capacityFactory.isSupported());
    ContainerConstructors.DirectCollectionConstruction<CapacityOnlyPriorityQueue>
        directMinCapacityConstruction =
            assertDirectCollectionPlan(capacityFactory.newConstruction(null, 0));
    Assert.assertEquals(directMinCapacityConstruction.newCollection().seenCapacity, 1);
    ContainerConstructors.DirectCollectionConstruction<CapacityOnlyPriorityQueue>
        directSizedConstruction =
            assertDirectCollectionPlan(capacityFactory.newConstruction(null, 7));
    Assert.assertEquals(directSizedConstruction.newCollection().seenCapacity, 7);
  }

  @Test
  public void testPriorityQueueFactoryRootTransferModes() {
    Comparator<String> comparator = new ReverseStringComparator();
    ContainerConstructors.PriorityQueueFactory<PriorityQueueOnlyPriorityQueue>
        priorityQueueFactory =
            ContainerConstructors.priorityQueueFactory(PriorityQueueOnlyPriorityQueue.class);
    Assert.assertTrue(priorityQueueFactory.isSupported());
    ContainerConstructors.RootTransferCollectionConstruction<PriorityQueueOnlyPriorityQueue>
        rootTransferPriorityQueueConstruction =
            assertRootTransferCollectionPlan(priorityQueueFactory.newConstruction(comparator, 3));
    PriorityQueue<String> priorityQueueRoot =
        (PriorityQueue<String>) rootTransferPriorityQueueConstruction.newRootCollection();
    Assert.assertSame(rootTransferPriorityQueueConstruction.getRootType(), PriorityQueue.class);
    Assert.assertSame(priorityQueueRoot.getClass(), PriorityQueue.class);
    Assert.assertSame(priorityQueueRoot.comparator(), comparator);

    ContainerConstructors.PriorityQueueFactory<SortedSetOnlyPriorityQueue> sortedSetFactory =
        ContainerConstructors.priorityQueueFactory(SortedSetOnlyPriorityQueue.class);
    Assert.assertTrue(sortedSetFactory.isSupported());
    ContainerConstructors.RootTransferCollectionConstruction<SortedSetOnlyPriorityQueue>
        rootTransferSortedSetConstruction =
            assertRootTransferCollectionPlan(sortedSetFactory.newConstruction(comparator, 3));
    PriorityQueue<String> sortedSetRoot =
        (PriorityQueue<String>) rootTransferSortedSetConstruction.newRootCollection();
    Assert.assertSame(sortedSetRoot.getClass(), PriorityQueue.class);
    Assert.assertSame(sortedSetRoot.comparator(), comparator);

    ContainerConstructors.PriorityQueueFactory<CollectionOnlyPriorityQueue> collectionFactory =
        ContainerConstructors.priorityQueueFactory(CollectionOnlyPriorityQueue.class);
    Assert.assertTrue(collectionFactory.isSupported());
    ContainerConstructors.RootTransferCollectionConstruction<CollectionOnlyPriorityQueue>
        rootTransferCollectionConstruction =
            assertRootTransferCollectionPlan(collectionFactory.newConstruction(null, 0));
    PriorityQueue<String> collectionRoot =
        (PriorityQueue<String>) rootTransferCollectionConstruction.newRootCollection();
    Assert.assertSame(collectionRoot.getClass(), PriorityQueue.class);
    Assert.assertNull(collectionRoot.comparator());

    ContainerConstructors.RootTransferCollectionConstruction<CollectionOnlyPriorityQueue>
        rootTransferComparatorCollectionConstruction =
            assertRootTransferCollectionPlan(collectionFactory.newConstruction(comparator, 3));
    PriorityQueue<String> comparatorCollectionRoot =
        (PriorityQueue<String>) rootTransferComparatorCollectionConstruction.newRootCollection();
    Assert.assertSame(comparatorCollectionRoot.getClass(), PriorityQueue.class);
    Assert.assertSame(comparatorCollectionRoot.comparator(), comparator);
  }

  @Test
  public void testPriorityQueueFactoryUnsupportedCases() {
    ContainerConstructors.PriorityQueueFactory<UnsupportedPriorityQueue> unsupportedFactory =
        ContainerConstructors.priorityQueueFactory(UnsupportedPriorityQueue.class);
    Assert.assertFalse(unsupportedFactory.isSupported());
    UnsupportedOperationException unsupportedFactoryException =
        Assert.expectThrows(
            UnsupportedOperationException.class, unsupportedFactory::checkSupported);
    Assert.assertTrue(
        unsupportedFactoryException
            .getMessage()
            .contains(
                "a supported constructor among (), (int), (Comparator), (int, Comparator),"
                    + " (Collection), (PriorityQueue), or (SortedSet)"));
  }
}
