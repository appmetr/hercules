package com.appmetr.hercules.manager;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.HerculesMonitoringGroup;
import com.appmetr.hercules.annotations.TopKey;
import com.appmetr.hercules.driver.DataDriver;
import com.appmetr.hercules.driver.HerculesQueryResult;
import com.appmetr.hercules.driver.serializer.RowSerializer;
import com.appmetr.hercules.driver.serializer.UniversalRowSerializer;
import com.appmetr.hercules.metadata.WideEntityMetadata;
import com.appmetr.hercules.operations.ExecutableOperation;
import com.appmetr.hercules.operations.OperationsCollector;
import com.appmetr.hercules.operations.OperationsResult;
import com.appmetr.hercules.operations.SaveExecutableOperation;
import com.appmetr.hercules.partition.NoPartitionProvider;
import com.appmetr.hercules.partition.PartitionProvider;
import com.appmetr.hercules.profile.DataOperationsProfile;
import com.appmetr.hercules.serializers.SerializerProvider;
import com.appmetr.hercules.wide.SliceDataSpecificator;
import com.appmetr.hercules.wide.SliceDataSpecificatorByCF;
import com.appmetr.monblank.Monitoring;
import com.appmetr.monblank.StopWatch;
import com.datastax.driver.core.TypeCodec;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;

public class WideEntityManager {

    private static Logger logger = LoggerFactory.getLogger(WideEntityManager.class);

    @Inject private Injector injector;
    @Inject private Hercules hercules;
    @Inject private DataDriver dataDriver;
    @Inject private Monitoring monitoring;
    @Inject private SerializerProvider serializerProvider;
    @Inject private EntityListenerInvocationHelper listenerInvocationHelper;

    public <E, R, T> E get(Class<E> clazz, R rowKey, T topKey, DataOperationsProfile dataOperationsProfile) {
        if (topKey == null) return null;

        List<E> entities = get(clazz, rowKey, new SliceDataSpecificator<>(topKey), dataOperationsProfile);
        return entities.size() > 0 ? entities.get(0) : null;
    }

    public <E, R, T> List<E> get(Class<E> clazz, R rowKey, DataOperationsProfile dataOperationsProfile) {
        return get(clazz, rowKey, new SliceDataSpecificator<T>(null, null, false, null), dataOperationsProfile);
    }

    public <E, R, T> List<E> get(Class<E> clazz, R rowKey, T[] columns, DataOperationsProfile dataOperationsProfile) {
        return get(clazz, rowKey, new SliceDataSpecificator<>(columns), dataOperationsProfile);
    }

    public <E, R, T> List<E> get(Class<E> clazz, R rowKey, Collection<T> columns, DataOperationsProfile dataOperationsProfile) {
        return get(clazz, rowKey, new SliceDataSpecificator<>(columns), dataOperationsProfile);
    }

    public <E, R, T> List<E> get(Class<E> clazz, R rowKey, T lowEnd, T highEnd, DataOperationsProfile dataOperationsProfile) {
        return get(clazz, rowKey, new SliceDataSpecificator<>(lowEnd, highEnd, false, null), dataOperationsProfile);
    }

    public <E, R, T> List<E> get(Class<E> clazz, R rowKey, T lowEnd, T highEnd, boolean reverse, Integer count, DataOperationsProfile dataOperationsProfile) {
        return get(clazz, rowKey, new SliceDataSpecificator<>(lowEnd, highEnd, reverse, count), dataOperationsProfile);
    }

    public <E, R, T> List<E> get(Class<E> clazz, R rowKey, SliceDataSpecificator<T> sliceDataSpecificator, DataOperationsProfile dataOperationsProfile) {
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_WM, "Get " + clazz.getSimpleName());

        try {
            WideEntityMetadata metadata = getMetadata(clazz);

            Map<T, Object> totalResults = new LinkedHashMap<>();
            List<SliceDataSpecificatorByCF<T>> partQueries = this.<R, T>getPartitionProvider(metadata).getPartitionedQueries(rowKey, sliceDataSpecificator);

            RowSerializer<R, T> rowSerializer = this.getRowSerializerForEntity(metadata);

            if (sliceDataSpecificator.getType() == SliceDataSpecificator.SliceDataSpecificatorType.RANGE) {

                for (SliceDataSpecificatorByCF<T> kv : partQueries) {

                    int partLimit = sliceDataSpecificator.getLimit() - totalResults.size();

                    if (partLimit <= 0) {
                        break;
                    }

                    HerculesQueryResult<T> result = dataDriver.getSlice(
                            hercules.getKeyspace(), metadata.getColumnFamily() + kv.getPartitionName(), dataOperationsProfile, rowSerializer, rowKey,
                            new SliceDataSpecificator<>(
                                    kv.getSliceDataSpecificator().getLowEnd(),
                                    kv.getSliceDataSpecificator().getHighEnd(),
                                    kv.getSliceDataSpecificator().isOrderDesc(),
                                    partLimit)
                    );

                    if (result.hasResult()) {
                        totalResults.putAll(result.getEntries());
                    }
                }

            } else if (sliceDataSpecificator.getType() == SliceDataSpecificator.SliceDataSpecificatorType.COLUMNS) {
                for (SliceDataSpecificatorByCF<T> kv : partQueries) {
                    HerculesQueryResult<T> result = dataDriver.getSlice(
                            hercules.getKeyspace(), metadata.getColumnFamily() + kv.getPartitionName(), dataOperationsProfile, rowSerializer, rowKey,
                            kv.getSliceDataSpecificator());

                    if (result.hasResult()) {
                        totalResults.putAll(result.getEntries());
                    }
                }
            } else {
                throw new IllegalStateException("Invalid type: " + sliceDataSpecificator.getType());
            }

            Field rowKeyField = metadata.getRowKeyMetadata().getField();
            Field topKeyField = metadata.getTopKeyMetadata().getField();

            List<E> entities = new ArrayList<>();
            for (Map.Entry<T, Object> entry : totalResults.entrySet()) {
                E entity = (E) entry.getValue();

                if (rowKeyField != null) {
                    rowKeyField.set(entity, rowKey);
                }
                if (topKeyField != null) {
                    topKeyField.set(entity, entry.getKey());
                }

                E postLoadResult = listenerInvocationHelper.invokePostLoadListener(metadata.getListenerMetadata(), entity);
                if (postLoadResult != null) {
                    entity = postLoadResult;
                }

                entities.add(entity);
            }

            countEntities(dataOperationsProfile, entities);

            return entities;
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } finally {
            monitor.stop();
        }
    }

    public <E, T> T getTopKey(Class clazz, E entity) {
        return getTopKey(entity, getMetadata(clazz));
    }

    public <E, R> void save(R rowKey, E entity, DataOperationsProfile dataOperationsProfile) {
        save(rowKey, getTopKey(entity, getMetadata(entity.getClass())), entity, dataOperationsProfile);
    }

    public <E, R> void save(R rowKey, E entity, int ttl, DataOperationsProfile dataOperationsProfile) {
        save(rowKey, getTopKey(entity, getMetadata(entity.getClass())), entity, ttl, dataOperationsProfile);
    }

    public <E, R, T> void save(Class<E> clazz, R rowKey, Iterable<E> entities, DataOperationsProfile dataOperationsProfile) {
        WideEntityMetadata metadata = getMetadata(clazz);
        save(clazz, rowKey, entities, metadata.getEntityTTL(), dataOperationsProfile);
    }

    public <E, R, T> void save(Class<E> clazz, R rowKey, Iterable<E> entities, int ttl, DataOperationsProfile dataOperationsProfile) {
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_WM, "Save " + clazz.getSimpleName());

        try {
            WideEntityMetadata metadata = getMetadata(clazz);

            Map<String, Map<T, Object>> partitionedValues = new HashMap<>();
            //Regroup entities by partitions
            for (E entity : entities) {
                E prePersistResult = listenerInvocationHelper.invokePrePersistListener(metadata.getListenerMetadata(), entity);
                if (prePersistResult != null) {
                    entity = prePersistResult;
                }

                T topKey = this.getTopKey(entity, metadata);
                String cfName = getCFName(metadata, rowKey, topKey);

                if (!partitionedValues.containsKey(cfName)) {
                    partitionedValues.put(cfName, new HashMap<>());
                }

                partitionedValues.get(cfName).put(topKey, entity);
            }

            //Save regrouped entities
            for (String cfName : partitionedValues.keySet()) {
                dataDriver.insert(hercules.getKeyspace(), cfName, dataOperationsProfile, this.getRowSerializerForEntity(metadata), rowKey, partitionedValues.get(cfName), ttl);

                for (Map.Entry<T, Object> entry : partitionedValues.get(cfName).entrySet()) {
                    listenerInvocationHelper.invokePostPersistListener(metadata.getListenerMetadata(), entry.getValue());
                }
            }
        } finally {
            monitor.stop();
        }
    }

    public <E, R, T> void save(R rowKey, T topKey, E value, DataOperationsProfile dataOperationsProfile) {
        WideEntityMetadata metadata = getMetadata(value.getClass());
        save(rowKey, topKey, value, metadata.getEntityTTL(), dataOperationsProfile);
    }

    public <E, R, T> void save(R rowKey, T topKey, E value, int ttl, DataOperationsProfile dataOperationsProfile) {
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_WM, "Save " + value.getClass().getSimpleName());

        try {
            WideEntityMetadata metadata = getMetadata(value.getClass());

            E prePersistResult = listenerInvocationHelper.invokePrePersistListener(metadata.getListenerMetadata(), value);
            if (prePersistResult != null) {
                value = prePersistResult;
            }

            dataDriver.insert(hercules.getKeyspace(), getCFName(metadata, rowKey, topKey), dataOperationsProfile, getRowSerializerForEntity(metadata),
                    rowKey, topKey, value, ttl);

            listenerInvocationHelper.invokePostPersistListener(metadata.getListenerMetadata(), value);
        } finally {
            monitor.stop();
        }
    }

    public <R> void delete(Class<?> clazz, R rowKey, DataOperationsProfile dataOperationsProfile) {
        WideEntityMetadata metadata = getMetadata(clazz);

        Set<String> columnFamilies = hercules.getColumnFamilies();
        for (String cfName : getCFForPartitionCreation(clazz)) {
            String cfFullName = metadata.getColumnFamily() + cfName;

            if (columnFamilies.contains(cfFullName)) {
                if (metadata.getListenerMetadata().getPreDeleteMethod() != null ||
                        metadata.getListenerMetadata().getPostDeleteMethod() != null) {
                    logger.warn("Pre/Post delete listener doesn't invoke for row delete");
                }

                dataDriver.delete(hercules.getKeyspace(), cfFullName, dataOperationsProfile, getRowSerializerForEntity(metadata), rowKey);
            }
        }
    }

    public <E, R> void delete(Class<?> clazz, R rowKey, E entity, DataOperationsProfile dataOperationsProfile) {
        deleteByKeys(clazz, rowKey, Arrays.asList(getTopKey(entity, getMetadata(entity.getClass()))), dataOperationsProfile);
    }

    public <E, R, T> void delete(Class<?> clazz, R rowKey, Iterable<E> entities, DataOperationsProfile dataOperationsProfile) {
        List<T> topKeys = new ArrayList<>();

        for (E entity : entities) {
            topKeys.add(this.getTopKey(entity, getMetadata(entity.getClass())));
        }

        deleteByKeys(clazz, rowKey, topKeys, dataOperationsProfile);
    }

    public <R, T> void deleteByKey(Class<?> clazz, R rowKey, T topKey, DataOperationsProfile dataOperationsProfile) {
        deleteByKeys(clazz, rowKey, Collections.singletonList(topKey), dataOperationsProfile);
    }

    public <R, T> void deleteByKeys(Class<?> clazz, R rowKey, Iterable<T> topKeys, DataOperationsProfile dataOperationsProfile) {
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_WM, "Delete by keys " + clazz.getSimpleName());

        try {
            WideEntityMetadata metadata = getMetadata(clazz);

            Map<String, List<T>> partitionedTopKeys = new HashMap<>();
            //Regroup entities by partitions
            for (T topKey : topKeys) {
                String cfName = getCFName(metadata, rowKey, topKey);

                if (!partitionedTopKeys.containsKey(cfName)) {
                    partitionedTopKeys.put(cfName, new ArrayList<>());
                }

                partitionedTopKeys.get(cfName).add(topKey);
            }

            for (String cfName : partitionedTopKeys.keySet()) {
                dataDriver.delete(hercules.getKeyspace(), cfName, dataOperationsProfile, this.getRowSerializerForEntity(metadata), rowKey, partitionedTopKeys.get(cfName));
            }

        } finally {
            monitor.stop();
        }
    }

    public <E, R, T> List<R> getKeyRange(Class<E> clazz, R from, R to, int batchSize, DataOperationsProfile dataOperationsProfile) {
        WideEntityMetadata metadata = getMetadata(clazz);

        if (!metadata.getPartitionProviderClass().equals(NoPartitionProvider.class)) {
            throw new RuntimeException("Get key range not supported for partitioned wide service");
        }

        RowSerializer<R, T> rowSerializer = getRowSerializerForEntity(metadata);
        return dataDriver.getKeyRange(hercules.getKeyspace(), metadata.getColumnFamily(), dataOperationsProfile, rowSerializer, from, to, batchSize);
    }

    public <E, R, T> List<R> getAllRowKeys(Class<E> clazz, DataOperationsProfile dataOperationsProfile) {
        WideEntityMetadata metadata = getMetadata(clazz);
        List<String> partitions = getPartitionProvider(metadata).getPartitionsForCreation();

        RowSerializer<R, T> rowSerializer = getRowSerializerForEntity(metadata);

        List<R> rowKeys = new ArrayList<>();
        for (String partition : partitions) {
            rowKeys.addAll(dataDriver.getKeyRange(hercules.getKeyspace(), metadata.getColumnFamily() + partition, dataOperationsProfile, rowSerializer, null, null, null));
        }

        return rowKeys;
    }

    public List<String> getCFForPartitionCreation(Class<?> clazz) {
        WideEntityMetadata metadata = getMetadata(clazz);

        return getPartitionProvider(metadata).getPartitionsForCreation();
    }

    public <O extends ExecutableOperation> OperationsResult executeOperations(OperationsCollector<O> collector, DataOperationsProfile dataOperationsProfile) {
        try {
            OperationsCollector.Type operationType = null;

            Object rowKey = null;
            TypeCodec rowSerializer = null;

            TypeCodec topSerializer = null;

            Map<Class, WideEntityMetadata> metadataMap = new HashMap<>();
            Map<Object, Class> topKeysToClassMap = new HashMap<>();
            Map<String, Map<Object, Object>> data = new HashMap<>();
            Map<String, Map<Object, Integer>> ttls = new HashMap<>();
            Map<Object, TypeCodec> serializers = new HashMap<>();

            TypeCodec bytesArraySerializer = TypeCodec.blob();

            Boolean rowDelete = false;
            WideEntityMetadata rowDeleteMetadata = null;

            //Prepare data for execution
            for (O operation : collector.getOperations()) {
                WideEntityMetadata metadata = getMetadata(operation.getClazz());
                if (!metadataMap.containsKey(operation.getClazz())) {
                    metadataMap.put(operation.getClazz(), metadata);
                }

                if (operationType == null) {
                    operationType = collector.getOperationType(operation);
                }

                int operationTtl = DataDriver.EMPTY_TTL;
                if (operationType.equals(OperationsCollector.Type.SAVE)) {
                    SaveExecutableOperation saveExecutableOperation = (SaveExecutableOperation) operation;
                    Integer ttl = saveExecutableOperation.getTTL();
                    operationTtl = ttl == null ? metadata.getEntityTTL() : ttl;
                }

                if (!operationType.equals(collector.getOperationType(operation))) {
                    throw new RuntimeException("Operations in collector should be the same type");
                }

                if (rowKey == null) {
                    rowKey = operation.getRowKey();
                }

                if (!rowKey.equals(operation.getRowKey())) {
                    throw new RuntimeException("Operations should be execute on same rowKey");
                }

                RowSerializer entitySerializer = getRowSerializerForEntity(metadata);

                if (rowSerializer == null) {
                    rowSerializer = entitySerializer.getRowKeySerializer();
                }

                if (topSerializer == null) {
                    topSerializer = entitySerializer.getTopKeySerializer();
                } else if (!topSerializer.getClass().equals(entitySerializer.getTopKeySerializer().getClass())) {
                    throw new RuntimeException("Operations in collector should use same top key serializer");
                }

                //Top key might be null, cause row serializer for wide entity is universal
                TypeCodec valueSerializer = entitySerializer.getValueSerializer(null);

                if (operation.getTopKeys() != null) {
                    for (Object topKey : operation.getTopKeys()) {

                        String operationCFName = getCFName(metadata, operation.getRowKey(), topKey);
                        if (!data.containsKey(operationCFName)) {
                            data.put(operationCFName, new LinkedHashMap<>());
                        }

                        data.get(operationCFName).put(topKey, null);
                        serializers.put(topKey, valueSerializer);
                        topKeysToClassMap.put(topKey, operation.getClazz());
                    }
                } else if (operation.getEntities() != null) {
                    for (Object entity : operation.getEntities()) {
                        Object topKey = getTopKey(entity, metadata);

                        String operationCFName = getCFName(metadata, operation.getRowKey(), topKey);
                        if (!data.containsKey(operationCFName)) {
                            data.put(operationCFName, new LinkedHashMap<>());
                        }

                        data.get(operationCFName).put(topKey, bytesArraySerializer.deserialize(valueSerializer.serialize(entity, null), null));

                        if (!ttls.containsKey(operationCFName)) {
                            ttls.put(operationCFName, new LinkedHashMap<>());
                        }
                        ttls.get(operationCFName).put(topKey, operationTtl);

                        serializers.put(topKey, valueSerializer);
                        topKeysToClassMap.put(topKey, operation.getClazz());
                    }
                } else if (operationType.equals(OperationsCollector.Type.DELETE)) {
                    rowDelete = true;
                    rowDeleteMetadata = metadata;
                    break;
                } else {
                    throw new RuntimeException("Trying execute non delete operation on whole row key");
                }
            }

            //Execute operations and build result
            OperationsResult result = new OperationsResult();
            UniversalRowSerializer<Object, Object> operationRowSerializer = new UniversalRowSerializer(rowSerializer, topSerializer, bytesArraySerializer);

            if (operationType != null) {
                switch (operationType) {
                    case DELETE:
                        if (rowDelete) {
                            for (String cfName : (List<String>) rowDeleteMetadata.getPartitionProvider().getPartitionsForCreation()) {
                                dataDriver.delete(hercules.getKeyspace(), rowDeleteMetadata.getColumnFamily() + cfName, dataOperationsProfile, operationRowSerializer, rowKey);
                            }
                        } else {
                            for (String cfName : data.keySet()) {
                                dataDriver.delete(hercules.getKeyspace(), cfName, dataOperationsProfile, operationRowSerializer, rowKey, data.get(cfName).keySet());
                            }
                        }

                        break;
                    case SAVE:
                        for (String cfName : data.keySet()) {
                            dataDriver.insert(hercules.getKeyspace(), cfName, dataOperationsProfile, operationRowSerializer, rowKey, data.get(cfName), ttls.get(cfName));
                        }

                        break;
                    case GET:
                        List resultObjects = new ArrayList<>();

                        for (String cfName : data.keySet()) {
                            HerculesQueryResult queryResult = dataDriver.getSlice(hercules.getKeyspace(), cfName, dataOperationsProfile,
                                    operationRowSerializer, rowKey, new SliceDataSpecificator<>(data.get(cfName).keySet()));

                            for (Object topKey : queryResult.getEntries().keySet()) {
                                Object entity = serializers.get(topKey).deserialize(
                                        bytesArraySerializer.serialize(queryResult.getEntries().get(topKey), null),
                                        null);

                                Field rowKeyField = metadataMap.get(topKeysToClassMap.get(topKey)).getRowKeyMetadata().getField();
                                Field topKeyField = metadataMap.get(topKeysToClassMap.get(topKey)).getTopKeyMetadata().getField();

                                if (rowKeyField != null) {
                                    rowKeyField.set(entity, rowKey);
                                }
                                if (topKeyField != null) {
                                    topKeyField.set(entity, topKey);
                                }

                                resultObjects.add(entity);
                            }
                        }

                        countEntities(dataOperationsProfile, resultObjects);
                        result = new OperationsResult(resultObjects);
                        break;
                    default:
                        throw new RuntimeException("Unknown operation type");
                }
            }

            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected <E, T> T getTopKey(E entity, WideEntityMetadata metadata) {
        final Field topKeyField = metadata.getTopKeyMetadata().getField();

        if (topKeyField == null) {
            throw new RuntimeException(TopKey.class.getSimpleName() + " field isn't declared for entity " + entity.getClass() + " but it needs in the operation");
        }

        try {
            return (T) topKeyField.get(entity);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    WideEntityMetadata getMetadata(Class<?> entityClass) {
        return hercules.getWideMetadata(entityClass);
    }

    private <R, T> UniversalRowSerializer<R, T> getRowSerializerForEntity(WideEntityMetadata metadata) {
        TypeCodec rowKeySerializer = metadata.getRowKeyMetadata().getSerializer() == null ?
                serializerProvider.getSerializer(metadata.getRowKeyMetadata().getKeyClass()) :
                serializerProvider.getSerializer(metadata.getRowKeyMetadata().getSerializer(), metadata.getRowKeyMetadata().getKeyClass());

        TypeCodec topKeySerializer = metadata.getTopKeyMetadata().getSerializer() == null ?
                serializerProvider.getSerializer(metadata.getTopKeyMetadata().getKeyClass()) :
                serializerProvider.getSerializer(metadata.getTopKeyMetadata().getSerializer(), metadata.getTopKeyMetadata().getKeyClass());

        TypeCodec universalSerializer = serializerProvider.getSerializer(metadata.getEntitySerializer(), metadata.getEntityClass());


        return new UniversalRowSerializer<R, T>(rowKeySerializer, topKeySerializer, universalSerializer);
    }

    private <R, T> PartitionProvider<R, T> getPartitionProvider(WideEntityMetadata metadata) {
        if (metadata.getPartitionProvider() == null) {
            metadata.setPartitionProvider(injector.getInstance(metadata.getPartitionProviderClass()));
        }

        return metadata.getPartitionProvider();
    }

    private <R, T> String getCFName(WideEntityMetadata metadata, R rowKey, T topKey) {
        return metadata.getColumnFamily() + getPartitionProvider(metadata).getPartition(rowKey, topKey);
    }

    private void countEntities(DataOperationsProfile dataOperationsProfile, Collection entries) {
        if (dataOperationsProfile != null) {
            dataOperationsProfile.count += entries.size();
        }
    }
}
