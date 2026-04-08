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

import static org.apache.fory.collection.Collections.ofHashSet;

import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Consumer;
import org.apache.fory.builder.LayerMarkerClassGenerator;
import org.apache.fory.collection.CollectionSnapshot;
import org.apache.fory.collection.MapSnapshot;
import org.apache.fory.context.CopyContext;
import org.apache.fory.context.MetaReadContext;
import org.apache.fory.context.ReadContext;
import org.apache.fory.context.WriteContext;
import org.apache.fory.memory.MemoryBuffer;
import org.apache.fory.meta.TypeDef;
import org.apache.fory.reflect.ReflectionUtils;
import org.apache.fory.resolver.ClassResolver;
import org.apache.fory.resolver.TypeResolver;
import org.apache.fory.serializer.AbstractObjectSerializer;
import org.apache.fory.serializer.FieldGroups;
import org.apache.fory.serializer.FieldGroups.SerializationFieldInfo;
import org.apache.fory.serializer.JavaSerializer;
import org.apache.fory.serializer.MetaSharedLayerSerializer;
import org.apache.fory.serializer.ObjectSerializer;
import org.apache.fory.serializer.Serializer;
import org.apache.fory.util.Preconditions;

/**
 * Serializers for subclasses of common JDK container types. Subclasses of {@link ArrayList}/{@link
 * HashMap}/{@link LinkedHashMap}/{@link java.util.TreeMap}/etc have `writeObject`/`readObject`
 * defined, which will use JDK compatible serializers, thus inefficient. Serializers will optimize
 * the serialization for those cases by serializing super classes part separately using existing
 * JIT/interpreter-mode serializers.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class ChildContainerSerializers {

  public static Class<? extends Serializer> getCollectionSerializerClass(Class<?> cls) {
    if (ChildCollectionSerializer.superClasses.contains(cls)
        || cls == TreeSet.class
        || cls == ConcurrentSkipListSet.class
        || cls == PriorityQueue.class) {
      return null;
    }
    if (ClassResolver.useReplaceResolveSerializer(cls)) {
      return null;
    }
    Class<?> type = cls;
    while (type != Object.class) {
      if (ChildCollectionSerializer.superClasses.contains(type)) {
        // Collection/Map must have default constructor to be invoked by fory, otherwise created
        // object can't be used to adding elements.
        // For example: `new ArrayList<Integer> { add(1);}`, without default constructor, created
        // list will have elementData as null, adding elements will raise NPE.
        if (!ReflectionUtils.hasNoArgConstructor(cls)) {
          return null;
        }
        if (type == ArrayList.class) {
          return ChildArrayListSerializer.class;
        }
        return ChildCollectionSerializer.class;
      }
      if (type == TreeSet.class) {
        return ContainerConstructors.sortedSetFactory((Class) cls, TreeSet.class).isSupported()
            ? ChildSortedSetSerializer.class
            : null;
      }
      if (type == ConcurrentSkipListSet.class) {
        return ContainerConstructors.sortedSetFactory((Class) cls, ConcurrentSkipListSet.class)
                .isSupported()
            ? ChildConcurrentSkipListSetSerializer.class
            : null;
      }
      if (type == PriorityQueue.class) {
        return ContainerConstructors.priorityQueueFactory((Class) cls).isSupported()
            ? ChildPriorityQueueSerializer.class
            : null;
      }
      if (JavaSerializer.getReadRefMethod(type, false) != null
          || JavaSerializer.getWriteObjectMethod(type, false) != null) {
        return null;
      }
      type = type.getSuperclass();
    }
    return null;
  }

  public static Class<? extends Serializer> getMapSerializerClass(Class<?> cls) {
    if (ChildMapSerializer.superClasses.contains(cls)
        || cls == TreeMap.class
        || cls == ConcurrentSkipListMap.class) {
      return null;
    }
    if (ClassResolver.useReplaceResolveSerializer(cls)) {
      return null;
    }
    Class<?> type = cls;
    while (type != Object.class) {
      if (ChildMapSerializer.superClasses.contains(type)) {
        // Collection/Map must have default constructor to be invoked by fory, otherwise created
        // object can't be used to adding elements.
        // For example: `new ArrayList<Integer> { add(1);}`, without default constructor, created
        // list will have elementData as null, adding elements will raise NPE.
        if (!ReflectionUtils.hasNoArgConstructor(cls)) {
          return null;
        }
        return ChildMapSerializer.class;
      }
      if (type == TreeMap.class) {
        return ContainerConstructors.sortedMapFactory((Class) cls, TreeMap.class).isSupported()
            ? ChildSortedMapSerializer.class
            : null;
      }
      if (type == ConcurrentSkipListMap.class) {
        return ContainerConstructors.sortedMapFactory((Class) cls, ConcurrentSkipListMap.class)
                .isSupported()
            ? ChildConcurrentSkipListMapSerializer.class
            : null;
      }
      if (JavaSerializer.getReadRefMethod(type, false) != null
          || JavaSerializer.getWriteObjectMethod(type, false) != null) {
        return null;
      }
      type = type.getSuperclass();
    }
    return null;
  }

  /**
   * Serializer for subclasses of {@link ChildCollectionSerializer#superClasses} if no jdk custom
   * serialization in those classes.
   */
  public static class ChildCollectionSerializer<T extends Collection>
      extends CollectionSerializer<T> {
    public static Set<Class<?>> superClasses =
        ofHashSet(
            ArrayList.class, LinkedList.class, ArrayDeque.class, Vector.class, HashSet.class
            // PriorityQueue/TreeSet/ConcurrentSkipListSet need comparator as constructor argument
            );
    protected final ChildCollectionSlots<T> childSlots;

    public ChildCollectionSerializer(TypeResolver typeResolver, Class<T> cls) {
      super(typeResolver, cls);
      childSlots = new ChildCollectionSlots<>(typeResolver, superClasses, cls);
    }

    @Override
    public Collection onCollectionWrite(WriteContext writeContext, T value) {
      MemoryBuffer buffer = writeContext.getBuffer();
      buffer.writeVarUint32Small7(value.size());
      childSlots.writeSlots(writeContext, value);
      return value;
    }

    public Collection newCollection(ReadContext readContext) {
      Collection collection = super.newCollection(readContext);
      childSlots.readInto(readContext, (T) collection);
      return collection;
    }

    @Override
    public Collection newCollection(CopyContext copyContext, Collection originCollection) {
      Collection newCollection = super.newCollection(copyContext, originCollection);
      childSlots.copyInto(copyContext, (T) originCollection, (T) newCollection);
      return newCollection;
    }
  }

  public static final class ChildArrayListSerializer<T extends ArrayList>
      extends ChildCollectionSerializer<T> {
    public ChildArrayListSerializer(TypeResolver typeResolver, Class<T> cls) {
      super(typeResolver, cls);
    }

    @Override
    public T newCollection(ReadContext readContext) {
      T collection = (T) super.newCollection(readContext);
      int numElements = getAndClearNumElements();
      setNumElements(numElements);
      collection.ensureCapacity(numElements);
      return collection;
    }
  }

  public static class ChildSortedSetSerializer<T extends SortedSet>
      extends CollectionSerializer<T> {
    public static Set<Class<?>> superClasses = ofHashSet(TreeSet.class);
    private final ContainerConstructors.SortedSetFactory<T> constructorFactory;
    private final ChildCollectionSlots<T> childSlots;
    private final boolean bulkReadEnabled;
    private final int bulkReadBufferLimitBytes;

    public ChildSortedSetSerializer(TypeResolver typeResolver, Class<T> cls) {
      super(typeResolver, cls, true);
      constructorFactory = ContainerConstructors.sortedSetFactory(cls, TreeSet.class);
      constructorFactory.checkSupported();
      childSlots = new ChildCollectionSlots<>(typeResolver, superClasses, cls);
      bulkReadEnabled = SortedContainerBulkAccess.canBulkReadSortedSet(cls);
      bulkReadBufferLimitBytes = config.sortedContainerBulkReadBufferLimitBytes();
    }

    @Override
    public Collection onCollectionWrite(WriteContext writeContext, T value) {
      MemoryBuffer buffer = writeContext.getBuffer();
      buffer.writeVarUint32Small7(value.size());
      if (!config.isXlang()) {
        writeContext.writeRef(value.comparator());
      }
      childSlots.writeSlots(writeContext, value);
      return value;
    }

    @Override
    public Collection newCollection(ReadContext readContext) {
      assert !config.isXlang();
      MemoryBuffer buffer = readContext.getBuffer();
      int numElements = buffer.readVarUint32Small7();
      setNumElements(numElements);
      Comparator comparator = (Comparator) readContext.readRef();
      return childSlots.readCollection(
          readContext,
          constructorFactory.newConstruction(comparator),
          readContext::reference,
          numElements,
          bulkReadEnabled,
          bulkReadBufferLimitBytes);
    }

    @Override
    public T onCollectionRead(Collection collection) {
      return ContainerTransfer.<T>finishCollection(collection);
    }

    @Override
    public T copy(CopyContext copyContext, T originCollection) {
      Comparator comparator = copyContext.copyObject(((SortedSet) originCollection).comparator());
      return childSlots.copyCollection(
          copyContext,
          originCollection,
          constructorFactory.newConstruction(comparator),
          targetCollection -> copyElements(copyContext, originCollection, targetCollection));
    }
  }

  public static class ChildConcurrentSkipListSetSerializer<T extends ConcurrentSkipListSet>
      extends ConcurrentCollectionSerializer<T> {
    public static Set<Class<?>> superClasses = ofHashSet(ConcurrentSkipListSet.class);
    private final ContainerConstructors.SortedSetFactory<T> constructorFactory;
    private final ChildCollectionSlots<T> childSlots;

    public ChildConcurrentSkipListSetSerializer(TypeResolver typeResolver, Class<T> cls) {
      super(typeResolver, cls, true);
      constructorFactory = ContainerConstructors.sortedSetFactory(cls, ConcurrentSkipListSet.class);
      constructorFactory.checkSupported();
      childSlots = new ChildCollectionSlots<>(typeResolver, superClasses, cls);
    }

    @Override
    public CollectionSnapshot onCollectionWrite(WriteContext writeContext, T value) {
      CollectionSnapshot snapshot = super.onCollectionWrite(writeContext, value);
      if (!config.isXlang()) {
        writeContext.writeRef(value.comparator());
      }
      childSlots.writeSlots(writeContext, value);
      return snapshot;
    }

    @Override
    public Collection newCollection(ReadContext readContext) {
      assert !config.isXlang();
      MemoryBuffer buffer = readContext.getBuffer();
      int numElements = buffer.readVarUint32Small7();
      setNumElements(numElements);
      int refId = readContext.lastPreservedRefId();
      Comparator comparator = (Comparator) readContext.readRef();
      return childSlots.readCollection(
          readContext,
          constructorFactory.newConstruction(comparator),
          target -> readContext.setReadRef(refId, target));
    }

    @Override
    public T onCollectionRead(Collection collection) {
      return ContainerTransfer.<T>finishCollection(collection);
    }

    @Override
    public T copy(CopyContext copyContext, T originCollection) {
      Comparator comparator = copyContext.copyObject(((SortedSet) originCollection).comparator());
      return childSlots.copyCollection(
          copyContext,
          originCollection,
          constructorFactory.newConstruction(comparator),
          targetCollection -> copyElements(copyContext, originCollection, targetCollection));
    }
  }

  public static class ChildPriorityQueueSerializer<T extends PriorityQueue>
      extends CollectionSerializer<T> {
    public static Set<Class<?>> superClasses = ofHashSet(PriorityQueue.class);
    private final ContainerConstructors.PriorityQueueFactory<T> constructorFactory;
    private final ChildCollectionSlots<T> childSlots;

    public ChildPriorityQueueSerializer(TypeResolver typeResolver, Class<T> cls) {
      super(typeResolver, cls, true);
      constructorFactory = ContainerConstructors.priorityQueueFactory(cls);
      constructorFactory.checkSupported();
      childSlots = new ChildCollectionSlots<>(typeResolver, superClasses, cls);
    }

    @Override
    public Collection onCollectionWrite(WriteContext writeContext, T value) {
      MemoryBuffer buffer = writeContext.getBuffer();
      buffer.writeVarUint32Small7(value.size());
      if (!config.isXlang()) {
        writeContext.writeRef(value.comparator());
      }
      childSlots.writeSlots(writeContext, value);
      return value;
    }

    @Override
    public Collection newCollection(ReadContext readContext) {
      assert !config.isXlang();
      MemoryBuffer buffer = readContext.getBuffer();
      int numElements = buffer.readVarUint32Small7();
      setNumElements(numElements);
      Comparator comparator = (Comparator) readContext.readRef();
      return childSlots.readCollection(
          readContext,
          constructorFactory.newConstruction(comparator, numElements),
          readContext::reference);
    }

    @Override
    public T onCollectionRead(Collection collection) {
      return ContainerTransfer.<T>finishCollection(collection);
    }

    @Override
    public T copy(CopyContext copyContext, T originCollection) {
      Comparator comparator =
          copyContext.copyObject(((PriorityQueue) originCollection).comparator());
      return childSlots.copyCollection(
          copyContext,
          originCollection,
          constructorFactory.newConstruction(comparator, originCollection.size()),
          targetCollection -> copyElements(copyContext, originCollection, targetCollection));
    }
  }

  /**
   * Serializer for subclasses of {@link ChildMapSerializer#superClasses} if no jdk custom
   * serialization in those classes.
   */
  public static class ChildMapSerializer<T extends Map> extends MapSerializer<T> {
    public static Set<Class<?>> superClasses =
        ofHashSet(
            HashMap.class, LinkedHashMap.class, ConcurrentHashMap.class
            // TreeMap/ConcurrentSkipListMap need comparator as constructor argument
            );
    private final ChildMapSlots<T> childSlots;

    public ChildMapSerializer(TypeResolver typeResolver, Class<T> cls) {
      super(typeResolver, cls);
      childSlots = new ChildMapSlots<>(typeResolver, superClasses, cls);
    }

    @Override
    public Map onMapWrite(WriteContext writeContext, T value) {
      MemoryBuffer buffer = writeContext.getBuffer();
      buffer.writeVarUint32Small7(value.size());
      childSlots.writeSlots(writeContext, value);
      return value;
    }

    @Override
    public Map newMap(ReadContext readContext) {
      Map map = super.newMap(readContext);
      childSlots.readInto(readContext, (T) map);
      return map;
    }

    @Override
    public Map newMap(CopyContext copyContext, Map originMap) {
      Map newMap = super.newMap(copyContext, originMap);
      childSlots.copyInto(copyContext, (T) originMap, (T) newMap);
      return newMap;
    }
  }

  public static class ChildSortedMapSerializer<T extends SortedMap> extends MapSerializer<T> {
    public static Set<Class<?>> superClasses = ofHashSet(TreeMap.class);
    private final ContainerConstructors.SortedMapFactory<T> constructorFactory;
    private final ChildMapSlots<T> childSlots;
    private final boolean bulkReadEnabled;
    private final int bulkReadBufferLimitBytes;

    public ChildSortedMapSerializer(TypeResolver typeResolver, Class<T> cls) {
      super(typeResolver, cls, true);
      constructorFactory = ContainerConstructors.sortedMapFactory(cls, TreeMap.class);
      constructorFactory.checkSupported();
      childSlots = new ChildMapSlots<>(typeResolver, superClasses, cls);
      bulkReadEnabled = SortedContainerBulkAccess.canBulkReadSortedMap(cls);
      bulkReadBufferLimitBytes = config.sortedContainerBulkReadBufferLimitBytes();
    }

    @Override
    public Map onMapWrite(WriteContext writeContext, T value) {
      MemoryBuffer buffer = writeContext.getBuffer();
      buffer.writeVarUint32Small7(value.size());
      if (!config.isXlang()) {
        writeContext.writeRef(value.comparator());
      }
      childSlots.writeSlots(writeContext, value);
      return value;
    }

    @Override
    public Map newMap(ReadContext readContext) {
      assert !config.isXlang();
      MemoryBuffer buffer = readContext.getBuffer();
      int numElements = buffer.readVarUint32Small7();
      setNumElements(numElements);
      Comparator comparator = (Comparator) readContext.readRef();
      return childSlots.readMap(
          readContext,
          constructorFactory.newConstruction(comparator),
          readContext::reference,
          numElements,
          bulkReadEnabled,
          bulkReadBufferLimitBytes);
    }

    @Override
    public T onMapRead(Map map) {
      return ContainerTransfer.<T>finishMap(map);
    }

    @Override
    public T copy(CopyContext copyContext, T originMap) {
      Comparator comparator = copyContext.copyObject(((SortedMap) originMap).comparator());
      return (T)
          childSlots.copyMap(
              copyContext,
              originMap,
              constructorFactory.newConstruction(comparator),
              targetMap -> copyEntry(copyContext, originMap, targetMap));
    }
  }

  public static class ChildConcurrentSkipListMapSerializer<T extends ConcurrentSkipListMap>
      extends ConcurrentMapSerializer<T> {
    public static Set<Class<?>> superClasses = ofHashSet(ConcurrentSkipListMap.class);
    private final ContainerConstructors.SortedMapFactory<T> constructorFactory;
    private final ChildMapSlots<T> childSlots;

    public ChildConcurrentSkipListMapSerializer(TypeResolver typeResolver, Class<T> cls) {
      super(typeResolver, cls, true);
      constructorFactory = ContainerConstructors.sortedMapFactory(cls, ConcurrentSkipListMap.class);
      constructorFactory.checkSupported();
      childSlots = new ChildMapSlots<>(typeResolver, superClasses, cls);
    }

    @Override
    public MapSnapshot onMapWrite(WriteContext writeContext, T value) {
      MapSnapshot snapshot = super.onMapWrite(writeContext, value);
      if (!config.isXlang()) {
        writeContext.writeRef(value.comparator());
      }
      childSlots.writeSlots(writeContext, value);
      return snapshot;
    }

    @Override
    public Map newMap(ReadContext readContext) {
      assert !config.isXlang();
      MemoryBuffer buffer = readContext.getBuffer();
      setNumElements(buffer.readVarUint32Small7());
      int refId = readContext.lastPreservedRefId();
      Comparator comparator = (Comparator) readContext.readRef();
      return childSlots.readMap(
          readContext,
          constructorFactory.newConstruction(comparator),
          target -> readContext.setReadRef(refId, target));
    }

    @Override
    public T onMapRead(Map map) {
      return ContainerTransfer.<T>finishMap(map);
    }

    @Override
    public T copy(CopyContext copyContext, T originMap) {
      Comparator comparator = copyContext.copyObject(((SortedMap) originMap).comparator());
      return (T)
          childSlots.copyMap(
              copyContext,
              originMap,
              constructorFactory.newConstruction(comparator),
              targetMap -> copyEntry(copyContext, originMap, targetMap));
    }
  }

  private static final class ChildCollectionSlots<T extends Collection> {
    private final TypeResolver typeResolver;
    private final Set<Class<?>> superClasses;
    private final Class<T> type;
    private final Serializer[] slotsSerializers;
    private SerializationFieldInfo[] fieldInfos;

    private ChildCollectionSlots(
        TypeResolver typeResolver, Set<Class<?>> superClasses, Class<T> type) {
      this.typeResolver = typeResolver;
      this.superClasses = superClasses;
      this.type = type;
      slotsSerializers = buildSlotsSerializers(typeResolver, superClasses, type);
    }

    private void writeSlots(WriteContext writeContext, T value) {
      ChildContainerSerializers.writeSlots(writeContext, value, slotsSerializers);
    }

    private void readInto(ReadContext readContext, T target) {
      // Child-only slot fields belong to the final leaf instance before any root-state transfer
      // runs, otherwise the inherited root copy would have nowhere safe to land.
      readAndSetFields(readContext, typeResolver, target, slotsSerializers);
    }

    private void copyInto(CopyContext copyContext, T origin, T target) {
      fieldInfos =
          copyFields(copyContext, typeResolver, type, superClasses, fieldInfos, origin, target);
    }

    private Collection readCollection(
        ReadContext readContext,
        ContainerConstructors.CollectionConstruction<T> construction,
        Consumer<T> registrar) {
      return readCollection(readContext, construction, registrar, 0, false, 0);
    }

    private Collection readCollection(
        ReadContext readContext,
        ContainerConstructors.CollectionConstruction<T> construction,
        Consumer<T> registrar,
        int numElements,
        boolean bulkReadEnabled,
        int bulkReadBufferLimitBytes) {
      return ContainerTransfer.readCollection(
          type,
          construction,
          registrar,
          target -> readInto(readContext, target),
          numElements,
          bulkReadEnabled,
          bulkReadBufferLimitBytes);
    }

    private T copyCollection(
        CopyContext copyContext,
        T originCollection,
        ContainerConstructors.CollectionConstruction<T> construction,
        Consumer<Collection> contentCopier) {
      return ContainerTransfer.<T>copyCollection(
          type,
          originCollection,
          copyContext,
          construction,
          target -> copyInto(copyContext, originCollection, target),
          contentCopier);
    }
  }

  private static final class ChildMapSlots<T extends Map> {
    private final TypeResolver typeResolver;
    private final Set<Class<?>> superClasses;
    private final Class<T> type;
    private final Serializer[] slotsSerializers;
    private SerializationFieldInfo[] fieldInfos;

    private ChildMapSlots(TypeResolver typeResolver, Set<Class<?>> superClasses, Class<T> type) {
      this.typeResolver = typeResolver;
      this.superClasses = superClasses;
      this.type = type;
      slotsSerializers = buildSlotsSerializers(typeResolver, superClasses, type);
    }

    private void writeSlots(WriteContext writeContext, T value) {
      ChildContainerSerializers.writeSlots(writeContext, value, slotsSerializers);
    }

    private void readInto(ReadContext readContext, T target) {
      // Child-only slot fields belong to the final leaf instance before any root-state transfer
      // runs, otherwise the inherited root copy would have nowhere safe to land.
      readAndSetFields(readContext, typeResolver, target, slotsSerializers);
    }

    private void copyInto(CopyContext copyContext, T origin, T target) {
      fieldInfos =
          copyFields(copyContext, typeResolver, type, superClasses, fieldInfos, origin, target);
    }

    private Map readMap(
        ReadContext readContext,
        ContainerConstructors.MapConstruction<T> construction,
        Consumer<T> registrar) {
      return readMap(readContext, construction, registrar, 0, false, 0);
    }

    private Map readMap(
        ReadContext readContext,
        ContainerConstructors.MapConstruction<T> construction,
        Consumer<T> registrar,
        int numElements,
        boolean bulkReadEnabled,
        int bulkReadBufferLimitBytes) {
      return ContainerTransfer.readMap(
          type,
          construction,
          registrar,
          target -> readInto(readContext, target),
          numElements,
          bulkReadEnabled,
          bulkReadBufferLimitBytes);
    }

    private T copyMap(
        CopyContext copyContext,
        T originMap,
        ContainerConstructors.MapConstruction<T> construction,
        Consumer<Map> entryCopier) {
      return ContainerTransfer.<T>copyMap(
          type,
          originMap,
          copyContext,
          construction,
          target -> copyInto(copyContext, originMap, target),
          entryCopier);
    }
  }

  private static void writeSlots(
      WriteContext writeContext, Object value, Serializer[] slotsSerializers) {
    for (Serializer slotsSerializer : slotsSerializers) {
      slotsSerializer.write(writeContext, value);
    }
  }

  private static SerializationFieldInfo[] copyFields(
      CopyContext copyContext,
      TypeResolver typeResolver,
      Class<?> type,
      Set<Class<?>> superClasses,
      SerializationFieldInfo[] fieldInfos,
      Object origin,
      Object target) {
    if (fieldInfos == null) {
      List<Field> fields = ReflectionUtils.getFieldsWithoutSuperClasses(type, superClasses);
      fieldInfos = FieldGroups.buildFieldsInfo(typeResolver, fields).allFields;
    }
    AbstractObjectSerializer.copyFields(copyContext, fieldInfos, origin, target);
    return fieldInfos;
  }

  private static <T> Serializer[] buildSlotsSerializers(
      TypeResolver typeResolver, Set<Class<?>> superClasses, Class<T> cls) {
    Preconditions.checkArgument(!superClasses.contains(cls));
    List<Serializer> serializers = new ArrayList<>();
    int layerIndex = 0;
    while (!superClasses.contains(cls)) {
      Serializer slotsSerializer;
      if (typeResolver.getConfig().isCompatible()) {
        TypeDef layerTypeDef = typeResolver.getTypeDef(cls, false);
        // Use layer index within class hierarchy (not global counter)
        // This ensures unique marker classes for each layer
        Class<?> layerMarkerClass = LayerMarkerClassGenerator.getOrCreate(cls, layerIndex);
        slotsSerializer =
            new MetaSharedLayerSerializer(typeResolver, cls, layerTypeDef, layerMarkerClass);
      } else {
        slotsSerializer = new ObjectSerializer<>(typeResolver, cls, false);
      }
      serializers.add(slotsSerializer);
      cls = (Class<T>) cls.getSuperclass();
      layerIndex++;
    }
    Collections.reverse(serializers);
    return serializers.toArray(new Serializer[0]);
  }

  private static void readAndSetFields(
      ReadContext readContext,
      TypeResolver typeResolver,
      Object collection,
      Serializer[] slotsSerializers) {
    for (Serializer slotsSerializer : slotsSerializers) {
      if (slotsSerializer instanceof MetaSharedLayerSerializer) {
        MetaSharedLayerSerializer metaSerializer = (MetaSharedLayerSerializer) slotsSerializer;
        if (typeResolver.getConfig().isMetaShareEnabled()) {
          // Read layer class meta first if meta share is enabled
          // This corresponds to writeLayerClassMeta() in MetaSharedLayerSerializer.write()
          readAndSkipLayerClassMeta(readContext);
        }
        metaSerializer.readAndSetFields(readContext, collection);
      } else {
        ((ObjectSerializer) slotsSerializer).readAndSetFields(readContext, collection);
      }
    }
  }

  /**
   * Read and skip the layer class meta from buffer. This is used to skip over the class definition
   * that was written by MetaSharedLayerSerializer.writeLayerClassMeta(). For
   * ChildContainerSerializers, we use the same serializer on both write and read sides, so we just
   * need to skip the meta without actually parsing it.
   */
  private static void readAndSkipLayerClassMeta(ReadContext readContext) {
    MemoryBuffer buffer = readContext.getBuffer();
    MetaReadContext metaReadContext = readContext.getMetaReadContext();
    if (metaReadContext == null) {
      return;
    }
    int indexMarker = buffer.readVarUint32Small14();
    boolean isRef = (indexMarker & 1) == 1;
    int index = indexMarker >>> 1;
    if (isRef) {
      // Reference to previously read type - nothing more to read
      return;
    }
    // New type - need to read and skip the TypeDef bytes
    long id = buffer.readInt64();
    TypeDef.skipTypeDef(buffer, id);
    // Add a placeholder to keep readTypeInfos indices in sync with the write side's classMap.
    // The write side (writeLayerClassMeta) adds layer marker classes to classMap which shares
    // the same index space as writeSharedClassMeta. Without this placeholder, subsequent
    // readSharedClassMeta reference lookups would use wrong indices.
    metaReadContext.readTypeInfos.add(null);
  }
}
