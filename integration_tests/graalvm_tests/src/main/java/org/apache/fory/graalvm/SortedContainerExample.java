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

import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.fory.Fory;
import org.apache.fory.ThreadLocalFory;
import org.apache.fory.ThreadSafeFory;
import org.apache.fory.util.Preconditions;

public class SortedContainerExample {
  private static final ThreadSafeFory THREAD_SAFE_FORY =
      new ThreadLocalFory(
          classLoader -> {
            Fory fory = createFory();
            fory.ensureSerializersCompiled();
            return fory;
          });

  public static void main(String[] args) throws Exception {
    Fory fory = createFory();
    fory.ensureSerializersCompiled();
    roundTripContainers(fory);
    roundTripContainersThreadSafe(THREAD_SAFE_FORY);
    System.out.println("SortedContainerExample succeed");
  }

  private static Fory createFory() {
    Fory fory =
        Fory.builder()
            .withName(SortedContainerExample.class.getName())
            .requireClassRegistration(true)
            .build();
    fory.register(ReverseComparator.class);
    fory.register(ChildTreeSet.class);
    fory.register(ChildTreeMap.class);
    fory.register(DescendingIterationChildTreeSet.class);
    fory.register(DescendingEntrySetChildTreeMap.class);
    return fory;
  }

  private static void roundTripContainers(Fory fory) {
    TreeSet<String> baseSet = new TreeSet<>(new ReverseComparator());
    baseSet.add("bbb");
    baseSet.add("a");
    baseSet.add("cc");
    TreeSet<String> baseSetResult = (TreeSet<String>) fory.deserialize(fory.serialize(baseSet));
    Preconditions.checkArgument(baseSetResult.equals(baseSet));
    Preconditions.checkArgument(
        baseSetResult.comparator().getClass().equals(baseSet.comparator().getClass()));

    TreeMap<String, String> baseMap = new TreeMap<>(new ReverseComparator());
    baseMap.put("bbb", "3");
    baseMap.put("a", "1");
    baseMap.put("cc", "2");
    TreeMap<String, String> baseMapResult =
        (TreeMap<String, String>) fory.deserialize(fory.serialize(baseMap));
    Preconditions.checkArgument(baseMapResult.equals(baseMap));
    Preconditions.checkArgument(
        baseMapResult.comparator().getClass().equals(baseMap.comparator().getClass()));

    ChildTreeSet childSet = new ChildTreeSet(new ReverseComparator());
    childSet.add("bbb");
    childSet.add("a");
    childSet.add("cc");
    childSet.state = 7;
    ChildTreeSet childSetResult = (ChildTreeSet) fory.deserialize(fory.serialize(childSet));
    Preconditions.checkArgument(childSetResult.equals(childSet));
    Preconditions.checkArgument(childSetResult.state == childSet.state);

    ChildTreeMap childMap = new ChildTreeMap(new ReverseComparator());
    childMap.put("bbb", "3");
    childMap.put("a", "1");
    childMap.put("cc", "2");
    childMap.state = 9;
    ChildTreeMap childMapResult = (ChildTreeMap) fory.deserialize(fory.serialize(childMap));
    Preconditions.checkArgument(childMapResult.equals(childMap));
    Preconditions.checkArgument(childMapResult.state == childMap.state);

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

  private static void roundTripContainersThreadSafe(ThreadSafeFory fory) throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(8);
    try {
      Future<?>[] futures = new Future<?>[128];
      for (int i = 0; i < futures.length; i++) {
        futures[i] =
            executor.submit(
                () -> {
                  fory.execute(
                      f -> {
                        roundTripContainers(f);
                        return null;
                      });
                });
      }
      for (Future<?> future : futures) {
        future.get();
      }
    } finally {
      executor.shutdownNow();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  public static final class ReverseComparator implements Comparator<String>, Serializable {
    @Override
    public int compare(String left, String right) {
      return right.compareTo(left);
    }
  }

  public static class ChildTreeSet extends TreeSet<String> {
    int state;

    public ChildTreeSet() {}

    public ChildTreeSet(Comparator<? super String> comparator) {
      super(comparator);
    }
  }

  public static class ChildTreeMap extends TreeMap<String, String> {
    int state;

    public ChildTreeMap() {}

    public ChildTreeMap(Comparator<? super String> comparator) {
      super(comparator);
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
