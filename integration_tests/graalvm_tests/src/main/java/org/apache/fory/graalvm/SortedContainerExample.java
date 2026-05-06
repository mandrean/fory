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

package org.apache.fory.graalvm;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.fory.Fory;
import org.apache.fory.config.Config;
import org.apache.fory.context.ReadContext;
import org.apache.fory.context.WriteContext;
import org.apache.fory.serializer.ImmutableSerializer;
import org.apache.fory.serializer.Serializer;
import org.apache.fory.serializer.collection.ChildContainerSerializers;
import org.apache.fory.util.Preconditions;

public class SortedContainerExample {
  private static final Comparator<String> NATURAL_ORDER = Comparator.naturalOrder();
  private static final Comparator<String> REVERSE_ORDER = Collections.reverseOrder();
  private static final Fory FORY;

  static {
    FORY = createFory();
    FORY.ensureSerializersCompiled();
  }

  public static void main(String[] args) throws Exception {
    roundTripContainers(FORY);
    System.out.println("SortedContainerExample succeed");
  }

  private static Fory createFory() {
    Fory fory =
        Fory.builder()
            .withName(SortedContainerExample.class.getName())
            .requireClassRegistration(true)
            .build();
    registerChildContainer(fory, ChildTreeSet.class);
    registerChildContainer(fory, ChildTreeMap.class);
    registerChildContainer(fory, CollectionCtorChildTreeSet.class);
    registerChildContainer(fory, SortedSetCtorChildTreeSet.class);
    registerChildContainer(fory, MapCtorChildTreeMap.class);
    registerChildContainer(fory, SortedMapCtorChildTreeMap.class);
    registerChildContainer(fory, DescendingIterationChildTreeSet.class);
    registerChildContainer(fory, DescendingEntrySetChildTreeMap.class);
    registerNaturalOrderComparator(fory);
    registerReverseOrderComparator(fory);
    return fory;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void registerChildContainer(Fory fory, Class<?> cls) {
    Class<? extends Serializer> serializerClass =
        Collection.class.isAssignableFrom(cls)
            ? ChildContainerSerializers.getCollectionSerializerClass(cls)
            : ChildContainerSerializers.getMapSerializerClass(cls);
    Preconditions.checkArgument(serializerClass != null, "No child serializer for %s", cls);
    fory.registerSerializerAndType((Class) cls, serializerClass);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void registerNaturalOrderComparator(Fory fory) {
    fory.registerSerializerAndType(
        (Class) NATURAL_ORDER.getClass(), NaturalOrderComparatorSerializer.class);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void registerReverseOrderComparator(Fory fory) {
    fory.registerSerializerAndType(
        (Class) REVERSE_ORDER.getClass(), ReverseOrderComparatorSerializer.class);
  }

  private static void roundTripContainers(Fory fory) {
    Comparator<String> reverseComparator = REVERSE_ORDER;
    TreeSet<String> baseSet = new TreeSet<>(reverseComparator);
    baseSet.add("bbb");
    baseSet.add("a");
    baseSet.add("cc");
    TreeSet<String> baseSetResult = (TreeSet<String>) fory.deserialize(fory.serialize(baseSet));
    Preconditions.checkArgument(baseSetResult.equals(baseSet));
    Preconditions.checkArgument(
        baseSetResult.comparator().getClass().equals(baseSet.comparator().getClass()));

    TreeMap<String, String> baseMap = new TreeMap<>(reverseComparator);
    baseMap.put("bbb", "3");
    baseMap.put("a", "1");
    baseMap.put("cc", "2");
    TreeMap<String, String> baseMapResult =
        (TreeMap<String, String>) fory.deserialize(fory.serialize(baseMap));
    Preconditions.checkArgument(baseMapResult.equals(baseMap));
    Preconditions.checkArgument(
        baseMapResult.comparator().getClass().equals(baseMap.comparator().getClass()));

    ChildTreeSet childSet = new ChildTreeSet(reverseComparator);
    childSet.add("bbb");
    childSet.add("a");
    childSet.add("cc");
    childSet.state = 7;
    ChildTreeSet childSetResult = (ChildTreeSet) fory.deserialize(fory.serialize(childSet));
    Preconditions.checkArgument(childSetResult.equals(childSet));
    Preconditions.checkArgument(childSetResult.state == childSet.state);

    ChildTreeMap childMap = new ChildTreeMap(reverseComparator);
    childMap.put("bbb", "3");
    childMap.put("a", "1");
    childMap.put("cc", "2");
    childMap.state = 9;
    ChildTreeMap childMapResult = (ChildTreeMap) fory.deserialize(fory.serialize(childMap));
    Preconditions.checkArgument(childMapResult.equals(childMap));
    Preconditions.checkArgument(childMapResult.state == childMap.state);

    CollectionCtorChildTreeSet collectionCtorSet =
        new CollectionCtorChildTreeSet(Arrays.asList("bbb", "a", "cc"));
    collectionCtorSet.state = 15;
    CollectionCtorChildTreeSet collectionCtorSetResult =
        (CollectionCtorChildTreeSet) fory.deserialize(fory.serialize(collectionCtorSet));
    assertRoundTripSet(collectionCtorSetResult, collectionCtorSet, false);

    TreeSet<String> reverseSetSource = new TreeSet<>(reverseComparator);
    reverseSetSource.add("bbb");
    reverseSetSource.add("a");
    reverseSetSource.add("cc");
    SortedSetCtorChildTreeSet sortedSetCtorSet = new SortedSetCtorChildTreeSet(reverseSetSource);
    sortedSetCtorSet.state = 17;
    SortedSetCtorChildTreeSet sortedSetCtorSetResult =
        (SortedSetCtorChildTreeSet) fory.deserialize(fory.serialize(sortedSetCtorSet));
    assertRoundTripSet(sortedSetCtorSetResult, sortedSetCtorSet, true);

    LinkedHashMap<String, String> mapSource = new LinkedHashMap<>();
    mapSource.put("bbb", "3");
    mapSource.put("a", "1");
    mapSource.put("cc", "2");
    MapCtorChildTreeMap mapCtorChildMap = new MapCtorChildTreeMap(mapSource);
    mapCtorChildMap.state = 19;
    MapCtorChildTreeMap mapCtorChildMapResult =
        (MapCtorChildTreeMap) fory.deserialize(fory.serialize(mapCtorChildMap));
    assertRoundTripMap(mapCtorChildMapResult, mapCtorChildMap, false);

    TreeMap<String, String> reverseMapSource = new TreeMap<>(reverseComparator);
    reverseMapSource.put("bbb", "3");
    reverseMapSource.put("a", "1");
    reverseMapSource.put("cc", "2");
    SortedMapCtorChildTreeMap sortedMapCtorChildMap =
        new SortedMapCtorChildTreeMap(reverseMapSource);
    sortedMapCtorChildMap.state = 21;
    SortedMapCtorChildTreeMap sortedMapCtorChildMapResult =
        (SortedMapCtorChildTreeMap) fory.deserialize(fory.serialize(sortedMapCtorChildMap));
    assertRoundTripMap(sortedMapCtorChildMapResult, sortedMapCtorChildMap, true);

    DescendingIterationChildTreeSet descendingSet = new DescendingIterationChildTreeSet();
    descendingSet.add("a");
    descendingSet.add("b");
    descendingSet.add("c");
    descendingSet.state = 11;
    DescendingIterationChildTreeSet descendingSetResult =
        (DescendingIterationChildTreeSet) fory.deserialize(fory.serialize(descendingSet));
    Preconditions.checkArgument(descendingSetResult.equals(descendingSet));
    Preconditions.checkArgument(descendingSetResult.state == descendingSet.state);

    DescendingEntrySetChildTreeMap descendingMap = new DescendingEntrySetChildTreeMap();
    descendingMap.put("a", "1");
    descendingMap.put("b", "2");
    descendingMap.put("c", "3");
    descendingMap.state = 13;
    DescendingEntrySetChildTreeMap descendingMapResult =
        (DescendingEntrySetChildTreeMap) fory.deserialize(fory.serialize(descendingMap));
    Preconditions.checkArgument(descendingMapResult.equals(descendingMap));
    Preconditions.checkArgument(descendingMapResult.state == descendingMap.state);
  }

  private static void assertRoundTripSet(
      StatefulTreeSet actual, StatefulTreeSet expected, boolean expectComparator) {
    Preconditions.checkArgument(actual.equals(expected));
    Preconditions.checkArgument(actual.getClass().equals(expected.getClass()));
    Preconditions.checkArgument(actual.state == expected.state);
    assertComparator(actual.comparator(), expectComparator);
  }

  private static void assertRoundTripMap(
      StatefulTreeMap actual, StatefulTreeMap expected, boolean expectComparator) {
    Preconditions.checkArgument(actual.equals(expected));
    Preconditions.checkArgument(actual.getClass().equals(expected.getClass()));
    Preconditions.checkArgument(actual.state == expected.state);
    assertComparator(actual.comparator(), expectComparator);
  }

  private static void assertComparator(Comparator<?> comparator, boolean expectReverseComparator) {
    if (expectReverseComparator) {
      Preconditions.checkArgument(comparator != null);
      Preconditions.checkArgument(((Comparator<String>) comparator).compare("a", "b") > 0);
    } else {
      Preconditions.checkArgument(comparator == null);
    }
  }

  public abstract static class StatefulTreeSet extends TreeSet<String> {
    int state;

    public StatefulTreeSet() {}

    public StatefulTreeSet(Comparator<? super String> comparator) {
      super(comparator);
    }

    public StatefulTreeSet(Collection<? extends String> values) {
      super(values);
    }

    public StatefulTreeSet(SortedSet<String> values) {
      super(values);
    }
  }

  public static class ChildTreeSet extends StatefulTreeSet {
    public ChildTreeSet() {}

    public ChildTreeSet(Comparator<? super String> comparator) {
      super(comparator);
    }
  }

  public static class CollectionCtorChildTreeSet extends StatefulTreeSet {
    public CollectionCtorChildTreeSet(Collection<? extends String> values) {
      super(values);
    }
  }

  public static class SortedSetCtorChildTreeSet extends StatefulTreeSet {
    public SortedSetCtorChildTreeSet(SortedSet<String> values) {
      super(values);
    }
  }

  public abstract static class StatefulTreeMap extends TreeMap<String, String> {
    int state;

    public StatefulTreeMap() {}

    public StatefulTreeMap(Comparator<? super String> comparator) {
      super(comparator);
    }

    public StatefulTreeMap(Map<? extends String, ? extends String> values) {
      super(values);
    }

    public StatefulTreeMap(SortedMap<String, ? extends String> values) {
      super(values);
    }
  }

  public static class ChildTreeMap extends StatefulTreeMap {
    public ChildTreeMap() {}

    public ChildTreeMap(Comparator<? super String> comparator) {
      super(comparator);
    }
  }

  public static class MapCtorChildTreeMap extends StatefulTreeMap {
    public MapCtorChildTreeMap(Map<? extends String, ? extends String> values) {
      super(values);
    }
  }

  public static class SortedMapCtorChildTreeMap extends StatefulTreeMap {
    public SortedMapCtorChildTreeMap(SortedMap<String, ? extends String> values) {
      super(values);
    }
  }

  public abstract static class SingletonComparatorSerializer
      extends ImmutableSerializer<Comparator<String>> {
    @SuppressWarnings("rawtypes")
    protected SingletonComparatorSerializer(Config config, Class type) {
      super(config, type);
    }

    @Override
    public void write(WriteContext writeContext, Comparator<String> value) {}
  }

  public static final class NaturalOrderComparatorSerializer extends SingletonComparatorSerializer {
    @SuppressWarnings("rawtypes")
    public NaturalOrderComparatorSerializer(Config config, Class type) {
      super(config, type);
    }

    @Override
    public Comparator<String> read(ReadContext readContext) {
      return NATURAL_ORDER;
    }
  }

  public static final class ReverseOrderComparatorSerializer extends SingletonComparatorSerializer {
    @SuppressWarnings("rawtypes")
    public ReverseOrderComparatorSerializer(Config config, Class type) {
      super(config, type);
    }

    @Override
    public Comparator<String> read(ReadContext readContext) {
      return REVERSE_ORDER;
    }
  }

  public static final class DescendingIterationChildTreeSet extends ChildTreeSet {
    @Override
    public Iterator<String> iterator() {
      return descendingIterator();
    }
  }

  public static final class DescendingEntrySetChildTreeMap extends ChildTreeMap {
    @Override
    public java.util.Set<Map.Entry<String, String>> entrySet() {
      return descendingMap().entrySet();
    }
  }
}
