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

package org.apache.fory.benchmark;

import java.io.Serializable;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.fory.Fory;
import org.apache.fory.config.CompatibleMode;
import org.apache.fory.logging.Logger;
import org.apache.fory.logging.LoggerFactory;
import org.openjdk.jmh.Main;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * Benchmark suite for sorted container serialization. Base-type and child-type states are isolated
 * so their serializer dispatch remains monomorphic. A mixed TreeSet state is retained as a
 * secondary signal for the PR #1 bimorphic-call-site artifact.
 */
@BenchmarkMode(Mode.Throughput)
public class SortedContainerSuite {
  private static final Logger LOG = LoggerFactory.getLogger(SortedContainerSuite.class);
  private static final int SIZE = 256;

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      String commandLine =
          "org.apache.fory.*SortedContainerSuite.* -f 2 -wi 3 -i 5 -t 1 -w 3s -r 3s -rf csv";
      System.out.println(commandLine);
      args = commandLine.split(" ");
    }
    Main.main(args);
  }

  public static class ReverseComparator implements Comparator<String>, Serializable {
    public static final ReverseComparator INSTANCE = new ReverseComparator();

    @Override
    public int compare(String a, String b) {
      return b.compareTo(a);
    }
  }

  public static class ChildTreeSet extends TreeSet<String> {
    private String label;

    public ChildTreeSet() {}

    public ChildTreeSet(Comparator<? super String> comparator) {
      super(comparator);
    }

    public String getLabel() {
      return label;
    }

    public void setLabel(String label) {
      this.label = label;
    }
  }

  public static class ChildTreeMap extends TreeMap<String, String> {
    private String label;

    public ChildTreeMap() {}

    public ChildTreeMap(Comparator<? super String> comparator) {
      super(comparator);
    }

    public String getLabel() {
      return label;
    }

    public void setLabel(String label) {
      this.label = label;
    }
  }

  public static class ChildPriorityQueue extends PriorityQueue<String> {
    private String label;

    public ChildPriorityQueue() {}

    public ChildPriorityQueue(Comparator<? super String> comparator) {
      super(comparator);
    }

    public String getLabel() {
      return label;
    }

    public void setLabel(String label) {
      this.label = label;
    }
  }

  private abstract static class SortedContainerStateSupport {
    protected Fory newFory() {
      return Fory.builder()
          .withCompatibleMode(CompatibleMode.COMPATIBLE)
          .withRefTracking(false)
          .requireClassRegistration(false)
          .build();
    }

    protected void populate(
        TreeSet<String> treeSet,
        TreeMap<String, String> treeMap,
        PriorityQueue<String> priorityQueue) {
      for (int i = 0; i < SIZE; i++) {
        String key = String.format("key-%04d", i);
        String val = String.format("val-%04d", i);
        treeSet.add(key);
        treeMap.put(key, val);
        priorityQueue.add(key);
      }
    }

    protected void logSizes(
        String label, byte[] treeSetBytes, byte[] treeMapBytes, byte[] priorityQueueBytes) {
      LOG.info("{} TreeSet bytes: {}", label, treeSetBytes.length);
      LOG.info("{} TreeMap bytes: {}", label, treeMapBytes.length);
      LOG.info("{} PriorityQueue bytes: {}", label, priorityQueueBytes.length);
    }
  }

  @State(Scope.Thread)
  public static class BaseTypesState extends SortedContainerStateSupport {
    private Fory fory;
    private TreeSet<String> treeSet;
    private byte[] treeSetBytes;
    private TreeMap<String, String> treeMap;
    private byte[] treeMapBytes;
    private PriorityQueue<String> priorityQueue;
    private byte[] priorityQueueBytes;

    @Setup
    public void setup() {
      fory = newFory();
      treeSet = new TreeSet<>(ReverseComparator.INSTANCE);
      treeMap = new TreeMap<>(ReverseComparator.INSTANCE);
      priorityQueue = new PriorityQueue<>(SIZE, ReverseComparator.INSTANCE);
      populate(treeSet, treeMap, priorityQueue);
      treeSetBytes = fory.serialize(treeSet);
      treeMapBytes = fory.serialize(treeMap);
      priorityQueueBytes = fory.serialize(priorityQueue);
      logSizes("Base", treeSetBytes, treeMapBytes, priorityQueueBytes);
    }
  }

  @State(Scope.Thread)
  public static class ChildTypesState extends SortedContainerStateSupport {
    private Fory fory;
    private ChildTreeSet childTreeSet;
    private byte[] childTreeSetBytes;
    private ChildTreeMap childTreeMap;
    private byte[] childTreeMapBytes;
    private ChildPriorityQueue childPriorityQueue;
    private byte[] childPriorityQueueBytes;

    @Setup
    public void setup() {
      fory = newFory();
      childTreeSet = new ChildTreeSet(ReverseComparator.INSTANCE);
      childTreeSet.setLabel("bench");
      childTreeMap = new ChildTreeMap(ReverseComparator.INSTANCE);
      childTreeMap.setLabel("bench");
      childPriorityQueue = new ChildPriorityQueue(ReverseComparator.INSTANCE);
      childPriorityQueue.setLabel("bench");
      populate(childTreeSet, childTreeMap, childPriorityQueue);
      childTreeSetBytes = fory.serialize(childTreeSet);
      childTreeMapBytes = fory.serialize(childTreeMap);
      childPriorityQueueBytes = fory.serialize(childPriorityQueue);
      logSizes("Child", childTreeSetBytes, childTreeMapBytes, childPriorityQueueBytes);
    }
  }

  @State(Scope.Thread)
  public static class MixedTreeSetState extends SortedContainerStateSupport {
    private Fory fory;
    private TreeSet<String> treeSet;
    private byte[] treeSetBytes;
    private ChildTreeSet childTreeSet;
    private byte[] childTreeSetBytes;

    @Setup
    public void setup() {
      fory = newFory();
      treeSet = new TreeSet<>(ReverseComparator.INSTANCE);
      childTreeSet = new ChildTreeSet(ReverseComparator.INSTANCE);
      childTreeSet.setLabel("bench");
      for (int i = 0; i < SIZE; i++) {
        String key = String.format("key-%04d", i);
        treeSet.add(key);
        childTreeSet.add(key);
      }
      treeSetBytes = fory.serialize(treeSet);
      childTreeSetBytes = fory.serialize(childTreeSet);
      LOG.info("Mixed TreeSet bytes: {}", treeSetBytes.length);
      LOG.info("Mixed ChildTreeSet bytes: {}", childTreeSetBytes.length);
    }
  }

  @Benchmark
  public Object serializeBaseTreeSet(BaseTypesState state) {
    return state.fory.serialize(state.treeSet);
  }

  @Benchmark
  public Object deserializeBaseTreeSet(BaseTypesState state) {
    return state.fory.deserialize(state.treeSetBytes);
  }

  @Benchmark
  public Object serializeBaseTreeMap(BaseTypesState state) {
    return state.fory.serialize(state.treeMap);
  }

  @Benchmark
  public Object deserializeBaseTreeMap(BaseTypesState state) {
    return state.fory.deserialize(state.treeMapBytes);
  }

  @Benchmark
  public Object serializeBasePriorityQueue(BaseTypesState state) {
    return state.fory.serialize(state.priorityQueue);
  }

  @Benchmark
  public Object deserializeBasePriorityQueue(BaseTypesState state) {
    return state.fory.deserialize(state.priorityQueueBytes);
  }

  @Benchmark
  public Object serializeChildTreeSet(ChildTypesState state) {
    return state.fory.serialize(state.childTreeSet);
  }

  @Benchmark
  public Object deserializeChildTreeSet(ChildTypesState state) {
    return state.fory.deserialize(state.childTreeSetBytes);
  }

  @Benchmark
  public Object serializeChildTreeMap(ChildTypesState state) {
    return state.fory.serialize(state.childTreeMap);
  }

  @Benchmark
  public Object deserializeChildTreeMap(ChildTypesState state) {
    return state.fory.deserialize(state.childTreeMapBytes);
  }

  @Benchmark
  public Object serializeChildPriorityQueue(ChildTypesState state) {
    return state.fory.serialize(state.childPriorityQueue);
  }

  @Benchmark
  public Object deserializeChildPriorityQueue(ChildTypesState state) {
    return state.fory.deserialize(state.childPriorityQueueBytes);
  }

  @Benchmark
  public Object serializeMixedBaseTreeSet(MixedTreeSetState state) {
    return state.fory.serialize(state.treeSet);
  }

  @Benchmark
  public Object deserializeMixedBaseTreeSet(MixedTreeSetState state) {
    return state.fory.deserialize(state.treeSetBytes);
  }

  @Benchmark
  public Object serializeMixedChildTreeSet(MixedTreeSetState state) {
    return state.fory.serialize(state.childTreeSet);
  }

  @Benchmark
  public Object deserializeMixedChildTreeSet(MixedTreeSetState state) {
    return state.fory.deserialize(state.childTreeSetBytes);
  }
}
