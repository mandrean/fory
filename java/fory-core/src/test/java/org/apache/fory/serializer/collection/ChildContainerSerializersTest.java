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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.fory.Fory;
import org.apache.fory.ForyTestBase;
import org.apache.fory.config.CompatibleMode;
import org.apache.fory.config.Language;
import org.apache.fory.serializer.Serializer;
import org.apache.fory.test.bean.Cyclic;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ChildContainerSerializersTest extends ForyTestBase {
  public static class ChildArrayList<E> extends ArrayList<E> {
    private int state;

    @Override
    public String toString() {
      return "ChildArrayList{" + "state=" + state + ",data=" + super.toString() + '}';
    }
  }

  public static class ChildLinkedList<E> extends LinkedList<E> {}

  public static class ChildArrayDeque<E> extends ArrayDeque<E> {}

  public static class ChildVector<E> extends Vector<E> {}

  public static class ChildHashSet<E> extends HashSet<E> {}

  @DataProvider(name = "foryConfig")
  public static Object[][] foryConfig() {
    return new Object[][] {
      {
        builder()
            .withRefTracking(false)
            .withScopedMetaShare(false)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .build()
      },
      {
        builder()
            .withRefTracking(false)
            .withScopedMetaShare(true)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .build()
      },
      {
        builder()
            .withRefTracking(false)
            .withCompatibleMode(CompatibleMode.SCHEMA_CONSISTENT)
            .build()
      },
    };
  }

  @Test(dataProvider = "foryConfig")
  public void testChildCollection(Fory fory) {
    List<Integer> data = ImmutableList.of(1, 2);
    {
      ChildArrayList<Integer> list = new ChildArrayList<>();
      list.addAll(data);
      list.state = 3;
      ChildArrayList<Integer> newList = serDe(fory, list);
      Assert.assertEquals(newList, list);
      Assert.assertEquals(newList.state, 3);
      Assert.assertEquals(
          fory.getTypeResolver().getSerializer(newList.getClass()).getClass(),
          ChildContainerSerializers.ChildArrayListSerializer.class);
      ArrayList<Integer> innerList =
          new ArrayList<Integer>() {
            {
              add(1);
            }
          };
      // innerList captures outer this.
      serDeCheck(fory, innerList);
      Assert.assertEquals(
          fory.getTypeResolver().getSerializer(innerList.getClass()).getClass(),
          CollectionSerializers.JDKCompatibleCollectionSerializer.class);
    }
    {
      ChildLinkedList<Integer> list = new ChildLinkedList<>();
      list.addAll(data);
      serDeCheck(fory, list);
    }
    {
      ChildArrayDeque<Integer> list = new ChildArrayDeque<>();
      list.addAll(data);
      Assert.assertEquals(ImmutableList.copyOf((ArrayDeque) (serDe(fory, list))), data);
    }
    {
      ChildVector<Integer> list = new ChildVector<>();
      list.addAll(data);
      serDeCheck(fory, list);
    }
    {
      ChildHashSet<Integer> list = new ChildHashSet<>();
      list.addAll(data);
      serDeCheck(fory, list);
    }
  }

  @Test(dataProvider = "foryCopyConfig")
  public void testChildCollectionCopy(Fory fory) {
    List<Object> data = ImmutableList.of(1, true, "test", Cyclic.create(true));
    {
      ChildArrayList<Object> list = new ChildArrayList<>();
      list.addAll(data);
      list.state = 3;
      ChildArrayList<Object> newList = fory.copy(list);
      Assert.assertEquals(newList, list);
      Assert.assertEquals(newList.state, 3);
      Assert.assertEquals(
          fory.getTypeResolver().getSerializer(newList.getClass()).getClass(),
          ChildContainerSerializers.ChildArrayListSerializer.class);
      ArrayList<Object> innerList =
          new ArrayList<Object>() {
            {
              add(Cyclic.create(true));
            }
          };
      copyCheck(fory, innerList);
      Assert.assertEquals(
          fory.getTypeResolver().getSerializer(innerList.getClass()).getClass(),
          CollectionSerializers.JDKCompatibleCollectionSerializer.class);
    }
    {
      ChildLinkedList<Object> list = new ChildLinkedList<>();
      list.addAll(data);
      copyCheck(fory, list);
    }
    {
      ChildArrayDeque<Object> list = new ChildArrayDeque<>();
      list.addAll(data);
      Assert.assertEquals(ImmutableList.copyOf(fory.copy(list)), data);
    }
    {
      ChildVector<Object> list = new ChildVector<>();
      list.addAll(data);
      copyCheck(fory, list);
    }
    {
      ChildHashSet<Object> list = new ChildHashSet<>();
      list.addAll(data);
      copyCheck(fory, list);
    }
  }

  public static class ChildHashMap<K, V> extends HashMap<K, V> {
    private int state;
  }

  public static class ChildLinkedHashMap<K, V> extends LinkedHashMap<K, V> {}

  public static class ChildConcurrentHashMap<K, V> extends ConcurrentHashMap<K, V> {}

  @Test(dataProvider = "foryConfig")
  public void testChildMap(Fory fory) {
    Map<String, Integer> data = ImmutableMap.of("k1", 1, "k2", 2);
    {
      ChildHashMap<String, Integer> map = new ChildHashMap<>();
      map.putAll(data);
      map.state = 3;
      ChildHashMap<String, Integer> newMap = (ChildHashMap<String, Integer>) serDe(fory, map);
      Assert.assertEquals(newMap, map);
      Assert.assertEquals(newMap.state, 3);
      Assert.assertEquals(
          fory.getTypeResolver().getSerializer(newMap.getClass()).getClass(),
          ChildContainerSerializers.ChildMapSerializer.class);
    }
    {
      ChildLinkedHashMap<String, Integer> map = new ChildLinkedHashMap<>();
      map.putAll(data);
      serDeCheck(fory, map);
    }
    {
      ChildConcurrentHashMap<String, Integer> map = new ChildConcurrentHashMap<>();
      map.putAll(data);
      serDeCheck(fory, map);
    }
  }

  @Test(dataProvider = "foryCopyConfig")
  public void testChildMapCopy(Fory fory) {
    Map<String, Object> data = ImmutableMap.of("k1", 1, "k2", 2, "k3", Cyclic.create(true));
    {
      ChildHashMap<String, Object> map = new ChildHashMap<>();
      map.putAll(data);
      map.state = 3;
      ChildHashMap<String, Object> copy = fory.copy(map);
      Assert.assertEquals(map, copy);
      Assert.assertEquals(map.state, copy.state);
      Assert.assertNotSame(map, copy);
    }
    {
      ChildLinkedHashMap<String, Object> map = new ChildLinkedHashMap<>();
      map.putAll(data);
      copyCheck(fory, map);
    }
    {
      ChildConcurrentHashMap<String, Object> map = new ChildConcurrentHashMap<>();
      map.putAll(data);
      copyCheck(fory, map);
    }
  }

  private static class CustomMap extends HashMap<String, String> {}

  @Data
  private static class UserDO {
    private CustomMap features;
  }

  @Test(dataProvider = "enableCodegen")
  public void testSerializeCustomPrivateMap(boolean enableCodegen) {
    CustomMap features = new CustomMap();
    features.put("a", "A");
    UserDO outerDO = new UserDO();
    outerDO.setFeatures(features);
    Fory fory =
        builder()
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withDeserializeUnknownClass(true)
            .withMetaShare(true)
            .withScopedMetaShare(false)
            .withCodegen(enableCodegen)
            .build();
    serDeMetaShared(fory, outerDO);
  }

  /**
   * Tests that meta context indices stay synchronized when layer class meta entries from
   * readAndSkipLayerClassMeta are interleaved with regular type info entries. Multiple instances of
   * the same nested HashMap subclass type force meta context reference lookups, which would fail if
   * readAndSkipLayerClassMeta did not add placeholders to readTypeInfos.
   */
  @Test(dataProvider = "enableCodegen")
  public void testMetaReadContextIndexSyncWithNestedChildMaps(boolean enableCodegen) {
    Fory fory =
        builder()
            .withCodegen(enableCodegen)
            .withAsyncCompilation(false)
            .withRefTracking(false)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .build();

    ChildHashMap1 map1a = new ChildHashMap1();
    map1a.put("k1", "v1");

    ChildHashMap1 map1b = new ChildHashMap1();
    map1b.put("k2", "v2");

    ChildHashMap2 map2a = new ChildHashMap2();
    map2a.put("a", map1a);

    ChildHashMap2 map2b = new ChildHashMap2();
    map2b.put("b", map1b);

    ChildHashMap3 map3a = new ChildHashMap3();
    map3a.put("x", map2a);

    ChildHashMap3 map3b = new ChildHashMap3();
    map3b.put("y", map2b);

    ChildHashMap4 map4 = new ChildHashMap4();
    map4.put("group1", map3a);
    map4.put("group2", map3b);

    ChildMapHolder holder = new ChildMapHolder("meta-sync-test", map4);
    ChildMapHolder deserialized = serDe(fory, holder);
    Assert.assertEquals(deserialized, holder);
  }

  /* Deeply nested HashMap subclass hierarchy for testing generic propagation */

  public static class ChildHashMap1 extends HashMap<String, String> {}

  public static class ChildHashMap2 extends HashMap<String, ChildHashMap1> {}

  public static class ChildHashMap3 extends HashMap<String, ChildHashMap2> {}

  public static class ChildHashMap4 extends HashMap<String, ChildHashMap3> {}

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ChildMapHolder {
    private String id;
    private ChildHashMap4 nestedMaps;
  }

  @Test(dataProvider = "enableCodegen")
  public void testNestedHashMapSubclassSerialization(boolean enableCodegen) {
    Fory fory =
        Fory.builder()
            .withCodegen(enableCodegen)
            .withRefTracking(false)
            .requireClassRegistration(false)
            .withLanguage(Language.JAVA)
            .build();

    ChildHashMap1 map1a = new ChildHashMap1();
    map1a.put("k1", "v1");
    map1a.put("k2", "v2");

    ChildHashMap1 map1b = new ChildHashMap1();
    map1b.put("k3", "v3");
    map1b.put("k4", "v4");

    ChildHashMap2 map2a = new ChildHashMap2();
    map2a.put("a", map1a);
    map2a.put("b", map1b);

    ChildHashMap2 map2b = new ChildHashMap2();
    map2b.put("c", map1b);

    ChildHashMap3 map3a = new ChildHashMap3();
    map3a.put("x", map2a);
    map3a.put("y", map2b);

    ChildHashMap3 map3b = new ChildHashMap3();
    map3b.put("z", map2a);

    ChildHashMap4 map4 = new ChildHashMap4();
    map4.put("group1", map3a);
    map4.put("group2", map3b);

    ChildMapHolder holder = new ChildMapHolder("doc-123", map4);
    ChildMapHolder deserialized = serDe(fory, holder);
    Assert.assertEquals(deserialized, holder);
  }

  @Test
  public void testNestedHashMapSubclassWithCompatibleMode() {
    Fory fory =
        Fory.builder()
            .withCodegen(false)
            .withAsyncCompilation(false)
            .withRefTracking(false)
            .requireClassRegistration(false)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withLanguage(Language.JAVA)
            .build();

    ChildHashMap1 map1a = new ChildHashMap1();
    map1a.put("k1", "v1");
    map1a.put("k2", "v2");

    ChildHashMap1 map1b = new ChildHashMap1();
    map1b.put("k3", "v3");
    map1b.put("k4", "v4");

    ChildHashMap2 map2a = new ChildHashMap2();
    map2a.put("a", map1a);
    map2a.put("b", map1b);

    ChildHashMap2 map2b = new ChildHashMap2();
    map2b.put("c", map1a);

    ChildHashMap3 map3a = new ChildHashMap3();
    map3a.put("x", map2a);
    map3a.put("y", map2b);

    ChildHashMap3 map3b = new ChildHashMap3();
    map3b.put("z", map2a);

    ChildHashMap4 map4 = new ChildHashMap4();
    map4.put("group1", map3a);
    map4.put("group2", map3b);

    ChildMapHolder holder = new ChildMapHolder("config-456", map4);
    ChildMapHolder deserialized = serDe(fory, holder);
    Assert.assertEquals(deserialized, holder);
  }

  /* Mixed collection subclass test (TreeSet + HashMap subclasses) */

  public static class ChildTreeSet extends TreeSet<ChildTreeSetEntry> {
    public ChildTreeSet() {
      super();
    }

    public static ChildTreeSet empty() {
      return new ChildTreeSet();
    }

    public static Collector<ChildTreeSetEntry, ?, ChildTreeSet> collector() {
      return Collectors.collectingAndThen(
          Collectors.toCollection(TreeSet::new),
          set -> {
            ChildTreeSet docs = new ChildTreeSet();
            docs.addAll(set);
            return docs;
          });
    }

    public static ChildTreeSet of(Collection<ChildTreeSetEntry> multiple) {
      return multiple.stream().collect(collector());
    }
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @EqualsAndHashCode
  public static class ChildTreeSetEntry implements Comparable<ChildTreeSetEntry> {
    private String id;
    private String name;

    @Override
    public int compareTo(ChildTreeSetEntry o) {
      return this.id.compareTo(o.id);
    }
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ChildMixedContainer {
    private String id;
    private ChildHashMap4 nestedMaps;
    private ChildTreeSet entries;
    private Map<String, ChildTreeSet> entriesByCategory;
  }

  @Test
  public void testMixedCollectionSubclassesWithCompatibleMode() {
    Fory fory =
        Fory.builder()
            .withCodegen(false)
            .withAsyncCompilation(false)
            .withRefTracking(false)
            .requireClassRegistration(false)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withLanguage(Language.JAVA)
            .build();

    ChildHashMap1 map1 = new ChildHashMap1();
    map1.put("k1", "v1");
    map1.put("k2", "v2");

    ChildHashMap2 map2 = new ChildHashMap2();
    map2.put("a", map1);

    ChildHashMap3 map3 = new ChildHashMap3();
    map3.put("x", map2);

    ChildHashMap4 map4 = new ChildHashMap4();
    map4.put("group1", map3);

    ChildTreeSet set1 = ChildTreeSet.empty();
    set1.add(new ChildTreeSetEntry("1", "entry1"));
    set1.add(new ChildTreeSetEntry("2", "entry2"));

    ChildTreeSet set2 = ChildTreeSet.empty();
    set2.add(new ChildTreeSetEntry("3", "entry3"));

    Map<String, ChildTreeSet> setsByKey = new HashMap<>();
    setsByKey.put("category1", set1);
    setsByKey.put("category2", set2);

    ChildMixedContainer container = new ChildMixedContainer("mixed-789", map4, set1, setsByKey);
    ChildMixedContainer deserialized = serDe(fory, container);
    Assert.assertEquals(deserialized, container);
  }

  private interface StateCarrier {
    String getState();

    void setState(String state);
  }

  private static final class ReverseStringComparator implements Comparator<String>, Serializable {
    private static final ReverseStringComparator INSTANCE = new ReverseStringComparator();

    @Override
    public int compare(String left, String right) {
      return right.compareTo(left);
    }
  }

  private abstract static class StatefulTreeSet extends TreeSet<String> implements StateCarrier {
    private String state;

    protected StatefulTreeSet() {}

    protected StatefulTreeSet(Comparator<? super String> comparator) {
      super(comparator);
    }

    protected StatefulTreeSet(Collection<? extends String> values) {
      super(values);
    }

    protected StatefulTreeSet(SortedSet<String> values) {
      super(values);
    }

    @Override
    public String getState() {
      return state;
    }

    @Override
    public void setState(String state) {
      this.state = state;
    }
  }

  public static class ChildTreeSetNoArg extends StatefulTreeSet {
    public ChildTreeSetNoArg() {}
  }

  public static class ChildTreeSetComparatorCtor extends StatefulTreeSet {
    public ChildTreeSetComparatorCtor(Comparator<? super String> comparator) {
      super(comparator);
    }
  }

  public static class ChildTreeSetCollectionCtor extends StatefulTreeSet {
    public ChildTreeSetCollectionCtor(Collection<? extends String> values) {
      super(values);
    }
  }

  public static class ChildTreeSetSortedSetCtor extends StatefulTreeSet {
    public ChildTreeSetSortedSetCtor(SortedSet<String> values) {
      super(values);
    }
  }

  private abstract static class StatefulConcurrentSkipListSet extends ConcurrentSkipListSet<String>
      implements StateCarrier {
    private String state;

    protected StatefulConcurrentSkipListSet() {}

    protected StatefulConcurrentSkipListSet(Comparator<? super String> comparator) {
      super(comparator);
    }

    protected StatefulConcurrentSkipListSet(Collection<? extends String> values) {
      super(values);
    }

    protected StatefulConcurrentSkipListSet(SortedSet<String> values) {
      super(values);
    }

    @Override
    public String getState() {
      return state;
    }

    @Override
    public void setState(String state) {
      this.state = state;
    }
  }

  public static class ChildConcurrentSkipListSetNoArg extends StatefulConcurrentSkipListSet {
    public ChildConcurrentSkipListSetNoArg() {}
  }

  public static class ChildConcurrentSkipListSetComparatorCtor
      extends StatefulConcurrentSkipListSet {
    public ChildConcurrentSkipListSetComparatorCtor(Comparator<? super String> comparator) {
      super(comparator);
    }
  }

  public static class ChildConcurrentSkipListSetCollectionCtor
      extends StatefulConcurrentSkipListSet {
    public ChildConcurrentSkipListSetCollectionCtor(Collection<? extends String> values) {
      super(values);
    }
  }

  public static class ChildConcurrentSkipListSetSortedSetCtor
      extends StatefulConcurrentSkipListSet {
    public ChildConcurrentSkipListSetSortedSetCtor(SortedSet<String> values) {
      super(values);
    }
  }

  private abstract static class StatefulTreeMap extends TreeMap<String, String>
      implements StateCarrier {
    private String state;

    protected StatefulTreeMap() {}

    protected StatefulTreeMap(Comparator<? super String> comparator) {
      super(comparator);
    }

    protected StatefulTreeMap(Map<? extends String, ? extends String> values) {
      super(values);
    }

    protected StatefulTreeMap(SortedMap<String, ? extends String> values) {
      super(values);
    }

    @Override
    public String getState() {
      return state;
    }

    @Override
    public void setState(String state) {
      this.state = state;
    }
  }

  public static class ChildTreeMapNoArg extends StatefulTreeMap {
    public ChildTreeMapNoArg() {}
  }

  public static class ChildTreeMapComparatorCtor extends StatefulTreeMap {
    public ChildTreeMapComparatorCtor(Comparator<? super String> comparator) {
      super(comparator);
    }
  }

  public static class ChildTreeMapMapCtor extends StatefulTreeMap {
    public ChildTreeMapMapCtor(Map<? extends String, ? extends String> values) {
      super(values);
    }
  }

  public static class ChildTreeMapSortedMapCtor extends StatefulTreeMap {
    public ChildTreeMapSortedMapCtor(SortedMap<String, ? extends String> values) {
      super(values);
    }
  }

  private abstract static class StatefulConcurrentSkipListMap
      extends ConcurrentSkipListMap<String, String> implements StateCarrier {
    private String state;

    protected StatefulConcurrentSkipListMap() {}

    protected StatefulConcurrentSkipListMap(Comparator<? super String> comparator) {
      super(comparator);
    }

    protected StatefulConcurrentSkipListMap(Map<? extends String, ? extends String> values) {
      super(values);
    }

    protected StatefulConcurrentSkipListMap(SortedMap<String, ? extends String> values) {
      super(values);
    }

    @Override
    public String getState() {
      return state;
    }

    @Override
    public void setState(String state) {
      this.state = state;
    }
  }

  public static class ChildConcurrentSkipListMapNoArg extends StatefulConcurrentSkipListMap {
    public ChildConcurrentSkipListMapNoArg() {}
  }

  public static class ChildConcurrentSkipListMapComparatorCtor
      extends StatefulConcurrentSkipListMap {
    public ChildConcurrentSkipListMapComparatorCtor(Comparator<? super String> comparator) {
      super(comparator);
    }
  }

  public static class ChildConcurrentSkipListMapMapCtor extends StatefulConcurrentSkipListMap {
    public ChildConcurrentSkipListMapMapCtor(Map<? extends String, ? extends String> values) {
      super(values);
    }
  }

  public static class ChildConcurrentSkipListMapSortedMapCtor
      extends StatefulConcurrentSkipListMap {
    public ChildConcurrentSkipListMapSortedMapCtor(SortedMap<String, ? extends String> values) {
      super(values);
    }
  }

  private abstract static class StatefulPriorityQueue extends PriorityQueue<String>
      implements StateCarrier {
    private String state;

    protected StatefulPriorityQueue() {}

    protected StatefulPriorityQueue(Comparator<? super String> comparator) {
      super(comparator);
    }

    protected StatefulPriorityQueue(Collection<? extends String> values) {
      super(values);
    }

    protected StatefulPriorityQueue(PriorityQueue<? extends String> values) {
      super(values);
    }

    protected StatefulPriorityQueue(SortedSet<? extends String> values) {
      super(values);
    }

    protected StatefulPriorityQueue(int initialCapacity) {
      super(initialCapacity);
    }

    protected StatefulPriorityQueue(int initialCapacity, Comparator<? super String> comparator) {
      super(initialCapacity, comparator);
    }

    @Override
    public String getState() {
      return state;
    }

    @Override
    public void setState(String state) {
      this.state = state;
    }
  }

  public static class ChildPriorityQueueNoArg extends StatefulPriorityQueue {
    public ChildPriorityQueueNoArg() {}
  }

  public static class ChildPriorityQueueComparatorCtor extends StatefulPriorityQueue {
    public ChildPriorityQueueComparatorCtor(Comparator<? super String> comparator) {
      super(comparator);
    }
  }

  public static class ChildPriorityQueueCollectionCtor extends StatefulPriorityQueue {
    public ChildPriorityQueueCollectionCtor(Collection<? extends String> values) {
      super(values);
    }
  }

  public static class ChildPriorityQueuePriorityQueueCtor extends StatefulPriorityQueue {
    public ChildPriorityQueuePriorityQueueCtor(PriorityQueue<? extends String> values) {
      super(values);
    }
  }

  public static class ChildPriorityQueueSortedSetCtor extends StatefulPriorityQueue {
    public ChildPriorityQueueSortedSetCtor(SortedSet<? extends String> values) {
      super(values);
    }
  }

  public static class ChildPriorityQueueCapacityCtor extends StatefulPriorityQueue {
    public ChildPriorityQueueCapacityCtor(int initialCapacity) {
      super(initialCapacity);
    }
  }

  public static class ChildPriorityQueueCapacityComparatorCtor extends StatefulPriorityQueue {
    public ChildPriorityQueueCapacityComparatorCtor(
        int initialCapacity, Comparator<? super String> comparator) {
      super(initialCapacity, comparator);
    }
  }

  public static class UnsupportedChildTreeSet extends TreeSet<String> {
    public UnsupportedChildTreeSet(int ignored) {}
  }

  public static class CustomSerializedChildTreeSet extends TreeSet<String> {
    private void writeObject(ObjectOutputStream stream) throws IOException {
      stream.defaultWriteObject();
    }

    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
      stream.defaultReadObject();
    }
  }

  public static class UnsupportedChildTreeMap extends TreeMap<String, String> {
    public UnsupportedChildTreeMap(int ignored) {}
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class AsyncSortedHolder {
    private ChildTreeSetNoArg documents;
    private ChildTreeMapMapCtor attributes;
  }

  @Test
  public void testAutoSortedSetSubclassConstructors() {
    Fory fory =
        builder()
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withCodegen(false)
            .withAsyncCompilation(false)
            .build();

    ChildTreeSetNoArg treeSetNoArg = new ChildTreeSetNoArg();
    treeSetNoArg.addAll(ImmutableList.of("b", "a", "c"));
    treeSetNoArg.setState("tree-no-arg");
    assertSortedSetAutoPath(
        fory, treeSetNoArg, ChildContainerSerializers.ChildSortedSetSerializer.class, false);

    ChildTreeSetComparatorCtor treeSetComparator =
        new ChildTreeSetComparatorCtor(ReverseStringComparator.INSTANCE);
    treeSetComparator.addAll(ImmutableList.of("b", "a", "c"));
    treeSetComparator.setState("tree-comparator");
    assertSortedSetAutoPath(
        fory, treeSetComparator, ChildContainerSerializers.ChildSortedSetSerializer.class, true);

    ChildTreeSetCollectionCtor treeSetCollection =
        new ChildTreeSetCollectionCtor(ImmutableList.of("b", "a", "c"));
    treeSetCollection.setState("tree-collection");
    assertSortedSetAutoPath(
        fory, treeSetCollection, ChildContainerSerializers.ChildSortedSetSerializer.class, false);

    TreeSet<String> reverseTreeSource = new TreeSet<>(ReverseStringComparator.INSTANCE);
    reverseTreeSource.addAll(ImmutableList.of("b", "a", "c"));
    ChildTreeSetSortedSetCtor treeSetSortedSet = new ChildTreeSetSortedSetCtor(reverseTreeSource);
    treeSetSortedSet.setState("tree-sorted-set");
    assertSortedSetAutoPath(
        fory, treeSetSortedSet, ChildContainerSerializers.ChildSortedSetSerializer.class, true);

    ChildConcurrentSkipListSetNoArg skipListNoArg = new ChildConcurrentSkipListSetNoArg();
    skipListNoArg.addAll(ImmutableList.of("b", "a", "c"));
    skipListNoArg.setState("skiplist-no-arg");
    assertSortedSetAutoPath(
        fory,
        skipListNoArg,
        ChildContainerSerializers.ChildConcurrentSkipListSetSerializer.class,
        false);

    ChildConcurrentSkipListSetComparatorCtor skipListComparator =
        new ChildConcurrentSkipListSetComparatorCtor(ReverseStringComparator.INSTANCE);
    skipListComparator.addAll(ImmutableList.of("b", "a", "c"));
    skipListComparator.setState("skiplist-comparator");
    assertSortedSetAutoPath(
        fory,
        skipListComparator,
        ChildContainerSerializers.ChildConcurrentSkipListSetSerializer.class,
        true);

    ChildConcurrentSkipListSetCollectionCtor skipListCollection =
        new ChildConcurrentSkipListSetCollectionCtor(ImmutableList.of("b", "a", "c"));
    skipListCollection.setState("skiplist-collection");
    assertSortedSetAutoPath(
        fory,
        skipListCollection,
        ChildContainerSerializers.ChildConcurrentSkipListSetSerializer.class,
        false);

    TreeSet<String> reverseSkipListSource = new TreeSet<>(ReverseStringComparator.INSTANCE);
    reverseSkipListSource.addAll(ImmutableList.of("b", "a", "c"));
    ChildConcurrentSkipListSetSortedSetCtor skipListSortedSet =
        new ChildConcurrentSkipListSetSortedSetCtor(reverseSkipListSource);
    skipListSortedSet.setState("skiplist-sorted-set");
    assertSortedSetAutoPath(
        fory,
        skipListSortedSet,
        ChildContainerSerializers.ChildConcurrentSkipListSetSerializer.class,
        true);
  }

  @Test
  public void testAutoSortedMapSubclassConstructors() {
    Fory fory =
        builder()
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withCodegen(false)
            .withAsyncCompilation(false)
            .build();

    ChildTreeMapNoArg treeMapNoArg = new ChildTreeMapNoArg();
    treeMapNoArg.put("b", "B");
    treeMapNoArg.put("a", "A");
    treeMapNoArg.setState("tree-map-no-arg");
    assertSortedMapAutoPath(
        fory, treeMapNoArg, ChildContainerSerializers.ChildSortedMapSerializer.class, false);

    ChildTreeMapComparatorCtor treeMapComparator =
        new ChildTreeMapComparatorCtor(ReverseStringComparator.INSTANCE);
    treeMapComparator.put("b", "B");
    treeMapComparator.put("a", "A");
    treeMapComparator.setState("tree-map-comparator");
    assertSortedMapAutoPath(
        fory, treeMapComparator, ChildContainerSerializers.ChildSortedMapSerializer.class, true);

    ChildTreeMapMapCtor treeMapMap = new ChildTreeMapMapCtor(ImmutableMap.of("b", "B", "a", "A"));
    treeMapMap.setState("tree-map-map");
    assertSortedMapAutoPath(
        fory, treeMapMap, ChildContainerSerializers.ChildSortedMapSerializer.class, false);

    TreeMap<String, String> reverseTreeMapSource = new TreeMap<>(ReverseStringComparator.INSTANCE);
    reverseTreeMapSource.put("b", "B");
    reverseTreeMapSource.put("a", "A");
    ChildTreeMapSortedMapCtor treeMapSortedMap =
        new ChildTreeMapSortedMapCtor(reverseTreeMapSource);
    treeMapSortedMap.setState("tree-map-sorted-map");
    assertSortedMapAutoPath(
        fory, treeMapSortedMap, ChildContainerSerializers.ChildSortedMapSerializer.class, true);

    ChildConcurrentSkipListMapNoArg skipListMapNoArg = new ChildConcurrentSkipListMapNoArg();
    skipListMapNoArg.put("b", "B");
    skipListMapNoArg.put("a", "A");
    skipListMapNoArg.setState("skip-list-map-no-arg");
    assertSortedMapAutoPath(
        fory,
        skipListMapNoArg,
        ChildContainerSerializers.ChildConcurrentSkipListMapSerializer.class,
        false);

    ChildConcurrentSkipListMapComparatorCtor skipListMapComparator =
        new ChildConcurrentSkipListMapComparatorCtor(ReverseStringComparator.INSTANCE);
    skipListMapComparator.put("b", "B");
    skipListMapComparator.put("a", "A");
    skipListMapComparator.setState("skip-list-map-comparator");
    assertSortedMapAutoPath(
        fory,
        skipListMapComparator,
        ChildContainerSerializers.ChildConcurrentSkipListMapSerializer.class,
        true);

    ChildConcurrentSkipListMapMapCtor skipListMapMap =
        new ChildConcurrentSkipListMapMapCtor(ImmutableMap.of("b", "B", "a", "A"));
    skipListMapMap.setState("skip-list-map-map");
    assertSortedMapAutoPath(
        fory,
        skipListMapMap,
        ChildContainerSerializers.ChildConcurrentSkipListMapSerializer.class,
        false);

    TreeMap<String, String> reverseSkipListMapSource =
        new TreeMap<>(ReverseStringComparator.INSTANCE);
    reverseSkipListMapSource.put("b", "B");
    reverseSkipListMapSource.put("a", "A");
    ChildConcurrentSkipListMapSortedMapCtor skipListMapSortedMap =
        new ChildConcurrentSkipListMapSortedMapCtor(reverseSkipListMapSource);
    skipListMapSortedMap.setState("skip-list-map-sorted-map");
    assertSortedMapAutoPath(
        fory,
        skipListMapSortedMap,
        ChildContainerSerializers.ChildConcurrentSkipListMapSerializer.class,
        true);
  }

  @Test
  public void testAutoPriorityQueueSubclassConstructors() {
    Fory fory =
        builder()
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withCodegen(false)
            .withAsyncCompilation(false)
            .build();

    ChildPriorityQueueNoArg queueNoArg = new ChildPriorityQueueNoArg();
    queueNoArg.addAll(ImmutableList.of("b", "a", "c"));
    queueNoArg.setState("queue-no-arg");
    assertPriorityQueueAutoPath(fory, queueNoArg, false);

    ChildPriorityQueueComparatorCtor queueComparator =
        new ChildPriorityQueueComparatorCtor(ReverseStringComparator.INSTANCE);
    queueComparator.addAll(ImmutableList.of("b", "a", "c"));
    queueComparator.setState("queue-comparator");
    assertPriorityQueueAutoPath(fory, queueComparator, true);

    ChildPriorityQueueCollectionCtor queueCollection =
        new ChildPriorityQueueCollectionCtor(ImmutableList.of("b", "a", "c"));
    queueCollection.setState("queue-collection");
    assertPriorityQueueAutoPath(fory, queueCollection, false);

    PriorityQueue<String> reverseQueueSource =
        new PriorityQueue<>(3, ReverseStringComparator.INSTANCE);
    reverseQueueSource.addAll(ImmutableList.of("b", "a", "c"));
    ChildPriorityQueuePriorityQueueCtor queuePriorityQueue =
        new ChildPriorityQueuePriorityQueueCtor(reverseQueueSource);
    queuePriorityQueue.setState("queue-priority-queue");
    assertPriorityQueueAutoPath(fory, queuePriorityQueue, true);

    TreeSet<String> reverseSortedQueueSource = new TreeSet<>(ReverseStringComparator.INSTANCE);
    reverseSortedQueueSource.addAll(ImmutableList.of("b", "a", "c"));
    ChildPriorityQueueSortedSetCtor queueSortedSet =
        new ChildPriorityQueueSortedSetCtor(reverseSortedQueueSource);
    queueSortedSet.setState("queue-sorted-set");
    assertPriorityQueueAutoPath(fory, queueSortedSet, true);

    ChildPriorityQueueCapacityCtor queueCapacity = new ChildPriorityQueueCapacityCtor(32);
    queueCapacity.addAll(ImmutableList.of("b", "a", "c"));
    queueCapacity.setState("queue-capacity");
    assertPriorityQueueAutoPath(fory, queueCapacity, false);

    ChildPriorityQueueCapacityComparatorCtor queueCapacityComparator =
        new ChildPriorityQueueCapacityComparatorCtor(32, ReverseStringComparator.INSTANCE);
    queueCapacityComparator.addAll(ImmutableList.of("b", "a", "c"));
    queueCapacityComparator.setState("queue-capacity-comparator");
    assertPriorityQueueAutoPath(fory, queueCapacityComparator, true);
  }

  @Test
  public void testUnsupportedSortedContainerSubclassesFallbackToJdkCompatibility() {
    Fory fory =
        builder()
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withCodegen(false)
            .withAsyncCompilation(false)
            .build();

    Assert.assertEquals(
        fory.getTypeResolver().getSerializer(UnsupportedChildTreeSet.class).getClass(),
        CollectionSerializers.JDKCompatibleCollectionSerializer.class);
    Assert.assertEquals(
        fory.getTypeResolver().getSerializer(CustomSerializedChildTreeSet.class).getClass(),
        CollectionSerializers.JDKCompatibleCollectionSerializer.class);
    Assert.assertEquals(
        fory.getTypeResolver().getSerializer(UnsupportedChildTreeMap.class).getClass(),
        MapSerializers.JDKCompatibleMapSerializer.class);
  }

  @Test
  public void testAsyncChildSortedContainersAvoidJdkCompatibleFallback() {
    Fory fory =
        builder()
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withCodegen(true)
            .withAsyncCompilation(true)
            .build();

    ChildTreeSetNoArg documents = new ChildTreeSetNoArg();
    documents.addAll(ImmutableList.of("b", "a", "c"));
    documents.setState("async-docs");

    ChildTreeMapMapCtor attributes = new ChildTreeMapMapCtor(ImmutableMap.of("b", "B", "a", "A"));
    attributes.setState("async-attributes");

    AsyncSortedHolder holder = new AsyncSortedHolder(documents, attributes);
    AsyncSortedHolder deserialized = serDe(fory, holder);
    Assert.assertEquals(deserialized, holder);
    Assert.assertEquals(
        fory.getTypeResolver().getSerializer(ChildTreeSetNoArg.class).getClass(),
        ChildContainerSerializers.ChildSortedSetSerializer.class);
    Assert.assertEquals(
        fory.getTypeResolver().getSerializer(ChildTreeMapMapCtor.class).getClass(),
        ChildContainerSerializers.ChildSortedMapSerializer.class);
  }

  private static <T extends SortedSet<String> & StateCarrier> void assertSortedSetAutoPath(
      Fory fory, T value, Class<? extends Serializer> serializerClass, boolean expectComparator) {
    Assert.assertEquals(
        fory.getTypeResolver().getSerializer(value.getClass()).getClass(), serializerClass);
    T deserialized = serDe(fory, value);
    Assert.assertEquals(deserialized, value);
    Assert.assertEquals(deserialized.getClass(), value.getClass());
    Assert.assertEquals(deserialized.getState(), value.getState());
    assertComparator(deserialized.comparator(), expectComparator);

    T copy = (T) fory.copy(value);
    Assert.assertEquals(copy, value);
    Assert.assertEquals(copy.getClass(), value.getClass());
    Assert.assertEquals(copy.getState(), value.getState());
    Assert.assertNotSame(copy, value);
    assertComparator(copy.comparator(), expectComparator);
  }

  private static <T extends SortedMap<String, String> & StateCarrier> void assertSortedMapAutoPath(
      Fory fory, T value, Class<? extends Serializer> serializerClass, boolean expectComparator) {
    Assert.assertEquals(
        fory.getTypeResolver().getSerializer(value.getClass()).getClass(), serializerClass);
    T deserialized = serDe(fory, value);
    Assert.assertEquals(deserialized, value);
    Assert.assertEquals(deserialized.getClass(), value.getClass());
    Assert.assertEquals(deserialized.getState(), value.getState());
    assertComparator(deserialized.comparator(), expectComparator);

    T copy = (T) fory.copy(value);
    Assert.assertEquals(copy, value);
    Assert.assertEquals(copy.getClass(), value.getClass());
    Assert.assertEquals(copy.getState(), value.getState());
    Assert.assertNotSame(copy, value);
    assertComparator(copy.comparator(), expectComparator);
  }

  private static <T extends PriorityQueue<String> & StateCarrier> void assertPriorityQueueAutoPath(
      Fory fory, T value, boolean expectComparator) {
    Assert.assertEquals(
        fory.getTypeResolver().getSerializer(value.getClass()).getClass(),
        ChildContainerSerializers.ChildPriorityQueueSerializer.class);
    T deserialized = serDe(fory, value);
    Assert.assertEquals(drainQueue(deserialized), drainQueue(value));
    Assert.assertEquals(deserialized.getClass(), value.getClass());
    Assert.assertEquals(deserialized.getState(), value.getState());
    assertComparator(deserialized.comparator(), expectComparator);

    T copy = (T) fory.copy(value);
    Assert.assertEquals(drainQueue(copy), drainQueue(value));
    Assert.assertEquals(copy.getClass(), value.getClass());
    Assert.assertEquals(copy.getState(), value.getState());
    Assert.assertNotSame(copy, value);
    assertComparator(copy.comparator(), expectComparator);
  }

  private static List<String> drainQueue(PriorityQueue<String> queue) {
    PriorityQueue<String> copy = new PriorityQueue<>(queue);
    List<String> values = new ArrayList<>();
    while (!copy.isEmpty()) {
      values.add(copy.poll());
    }
    return values;
  }

  private static void assertComparator(Comparator<?> comparator, boolean expectComparator) {
    if (expectComparator) {
      Assert.assertNotNull(comparator);
      Assert.assertEquals(comparator.getClass(), ReverseStringComparator.class);
      Assert.assertTrue(((Comparator<String>) comparator).compare("a", "b") > 0);
    } else {
      Assert.assertNull(comparator);
    }
  }

  public static class ChildLinkedListElemList extends LinkedList<ChildLinkedListElemList> {}

  public static class ChildLinkedListElemListStruct {
    public ChildLinkedListElemList list;
  }

  @Test
  public void testElemTypeSameWithCollection() {
    Fory fory = builder().withRefTracking(true).build();
    ChildLinkedListElemList list = new ChildLinkedListElemList();
    list.add(list);
    ChildLinkedListElemList list1 = serDe(fory, list);
    Assert.assertSame(list1.get(0), list1);

    ChildLinkedListElemListStruct struct = new ChildLinkedListElemListStruct();
    struct.list = list;
    ChildLinkedListElemListStruct struct1 = serDe(fory, struct);
    Assert.assertSame(struct1.list.get(0), struct1.list);
  }
}
