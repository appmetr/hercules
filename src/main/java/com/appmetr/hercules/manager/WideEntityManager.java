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
import com.appmetr.hercules.partition.NoPartitionProvider;
import com.appmetr.hercules.partition.PartitionProvider;
import com.appmetr.hercules.profile.DataOperationsProfile;
import com.appmetr.hercules.serializers.InformerSerializer;
import com.appmetr.hercules.serializers.SerializerProvider;
import com.appmetr.hercules.wide.SliceDataSpecificator;
import com.appmetr.hercules.wide.SliceDataSpecificatorByCF;
import com.appmetr.monblank.Monitoring;
import com.appmetr.monblank.StopWatch;
import com.google.inject.Inject;
import com.google.inject.Injector;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.hector.api.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
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

        List<E> entities = get(clazz, rowKey, new SliceDataSpecificator<T>(topKey), dataOperationsProfile);
        return entities.size() > 0 ? entities.get(0) : null;
    }

    public <E, R, T> List<E> get(Class<E> clazz, R rowKey, DataOperationsProfile dataOperationsProfile) {
        return get(clazz, rowKey, new SliceDataSpecificator<T>(null, null, false, null), dataOperationsProfile);
    }

    public <E, R, T> List<E> get(Class<E> clazz, R rowKey, T[] columns, DataOperationsProfile dataOperationsProfile) {
        return get(clazz, rowKey, new SliceDataSpecificator<T>(columns), dataOperationsProfile);
    }

    public <E, R, T> List<E> get(Class<E> clazz, R rowKey, Collection<T> columns, DataOperationsProfile dataOperationsProfile) {
        return get(clazz, rowKey, new SliceDataSpecificator<T>(columns), dataOperationsProfile);
    }

    public <E, R, T> List<E> get(Class<E> clazz, R rowKey, T start, T end, DataOperationsProfile dataOperationsProfile) {
        return get(clazz, rowKey, new SliceDataSpecificator<T>(start, end, false, null), dataOperationsProfile);
    }

    public <E, R, T> List<E> get(Class<E> clazz, R rowKey, T start, T end, boolean reverse, Integer count, DataOperationsProfile dataOperationsProfile) {
        return get(clazz, rowKey, new SliceDataSpecificator<T>(start, end, reverse, count), dataOperationsProfile);
    }

    public <E, R, T> List<E> get(Class<E> clazz, R rowKey, SliceDataSpecificator<T> sliceDataSpecificator, DataOperationsProfile dataOperationsProfile) {
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_WM, "Get " + clazz.getSimpleName());

        try {
            WideEntityMetadata metadata = getMetadata(clazz);

            Map<T, Object> totalResults = new LinkedHashMap<T, Object>();
            List<SliceDataSpecificatorByCF<T>> partQueries = this.<R, T>getPartitionProvider(metadata).getPartitionedQueries(rowKey, sliceDataSpecificator);

            RowSerializer<R, T> rowSerializer = this.getRowSerializerForEntity(metadata, dataOperationsProfile);

            if (sliceDataSpecificator.getType() == SliceDataSpecificator.SliceDataSpecificatorType.RANGE) {

                for (SliceDataSpecificatorByCF<T> kv : partQueries) {

                    int partLimit = sliceDataSpecificator.getLimit() - totalResults.size();

                    if (partLimit <= 0) {
                        break;
                    }

                    HerculesQueryResult<T> result = dataDriver.getSlice(
                            hercules.getKeyspace(), metadata.getColumnFamily() + kv.getPartitionName(), dataOperationsProfile, rowSerializer, rowKey,
                            new SliceDataSpecificator<T>(
                                    kv.getSliceDataSpecificator().getStart(),
                                    kv.getSliceDataSpecificator().getEnd(),
                                    kv.getSliceDataSpecificator().isOrderDesc(),
                                    partLimit));

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

            List<E> entities = new ArrayList<E>();
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

    public <E, R, T> void save(Class<E> clazz, R rowKey, Iterable<E> entities, DataOperationsProfile dataOperationsProfile) {
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_WM, "Save " + clazz.getSimpleName());

        try {
            WideEntityMetadata metadata = getMetadata(clazz);

            Map<String, Map<T, Object>> partitionedValues = new HashMap<String, Map<T, Object>>();
            //Regroup entities by partitions
            for (E entity : entities) {
                E prePersistResult = listenerInvocationHelper.invokePrePersistListener(metadata.getListenerMetadata(), entity);
                if (prePersistResult != null) {
                    entity = prePersistResult;
                }

                T topKey = this.getTopKey(entity, metadata);
                String cfName = getCFName(metadata, rowKey, topKey);

                if (!partitionedValues.containsKey(cfName)) {
                    partitionedValues.put(cfName, new HashMap<T, Object>());
                }

                partitionedValues.get(cfName).put(topKey, entity);
            }

            //Save regrouped entities
            for (String cfName : partitionedValues.keySet()) {
                dataDriver.insert(hercules.getKeyspace(), cfName, dataOperationsProfile, this.<R, T>getRowSerializerForEntity(metadata, dataOperationsProfile), rowKey, partitionedValues.get(cfName));

                for (Map.Entry<T, Object> entry : partitionedValues.get(cfName).entrySet()) {
                    listenerInvocationHelper.invokePostPersistListener(metadata.getListenerMetadata(), entry.getValue());
                }
            }
        } finally {
            monitor.stop();
        }
    }

    public <E, R, T> void save(R rowKey, T topKey, E value, DataOperationsProfile dataOperationsProfile) {
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_WM, "Save " + value.getClass().getSimpleName());

        try {
            WideEntityMetadata metadata = getMetadata(value.getClass());

            E prePersistResult = listenerInvocationHelper.invokePrePersistListener(metadata.getListenerMetadata(), value);
            if (prePersistResult != null) {
                value = prePersistResult;
            }

            dataDriver.insert(hercules.getKeyspace(), getCFName(metadata, rowKey, topKey), dataOperationsProfile, getRowSerializerForEntity(metadata, dataOperationsProfile),
                    rowKey, topKey, value);

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

                dataDriver.delete(hercules.getKeyspace(), cfFullName, dataOperationsProfile, getRowSerializerForEntity(metadata, dataOperationsProfile), rowKey);
            }
        }
    }

    public <E, R> void delete(Class<?> clazz, R rowKey, E entity, DataOperationsProfile dataOperationsProfile) {
        deleteByKeys(clazz, rowKey, Arrays.asList(getTopKey(entity, getMetadata(entity.getClass()))), dataOperationsProfile);
    }

    public <E, R, T> void delete(Class<?> clazz, R rowKey, Iterable<E> entities, DataOperationsProfile dataOperationsProfile) {
        List<T> topKeys = new ArrayList<T>();

        for (E entity : entities) {
            topKeys.add(this.<E, T>getTopKey(entity, getMetadata(entity.getClass())));
        }

        deleteByKeys(clazz, rowKey, topKeys, dataOperationsProfile);
    }

    public <R, T> void deleteByKey(Class<?> clazz, R rowKey, T topKey, DataOperationsProfile dataOperationsProfile) {
        deleteByKeys(clazz, rowKey, Arrays.asList(topKey), dataOperationsProfile);
    }

    public <R, T> void deleteByKeys(Class<?> clazz, R rowKey, Iterable<T> topKeys, DataOperationsProfile dataOperationsProfile) {
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_WM, "Delete by keys " + clazz.getSimpleName());

        try {
            WideEntityMetadata metadata = getMetadata(clazz);

            Map<String, List<T>> partitionedTopKeys = new HashMap<String, List<T>>();
            //Regroup entities by partitions
            for (T topKey : topKeys) {
                String cfName = getCFName(metadata, rowKey, topKey);

                if (!partitionedTopKeys.containsKey(cfName)) {
                    partitionedTopKeys.put(cfName, new ArrayList<T>());
                }

                partitionedTopKeys.get(cfName).add(topKey);
            }

            for (String cfName : partitionedTopKeys.keySet()) {
                dataDriver.delete(hercules.getKeyspace(), cfName, dataOperationsProfile, this.<R, T>getRowSerializerForEntity(metadata, dataOperationsProfile), rowKey, partitionedTopKeys.get(cfName));
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

        RowSerializer<R, T> rowSerializer = getRowSerializerForEntity(metadata, dataOperationsProfile);
        return dataDriver.getKeyRange(hercules.getKeyspace(), metadata.getColumnFamily(), dataOperationsProfile, rowSerializer, from, to, batchSize);
    }

    public <E, R, T> List<R> getAllRowKeys(Class<E> clazz, DataOperationsProfile dataOperationsProfile) {
        WideEntityMetadata metadata = getMetadata(clazz);
        List<String> partitions = getPartitionProvider(metadata).getPartitionsForCreation();

        RowSerializer<R, T> rowSerializer = getRowSerializerForEntity(metadata, dataOperationsProfile);

        List<R> rowKeys = new ArrayList<R>();
        for (String partition : partitions) {
            rowKeys.addAll(dataDriver.<R, T>getKeyRange(hercules.getKeyspace(), metadata.getColumnFamily() + partition, dataOperationsProfile, rowSerializer, null, null, null));
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
            Serializer rowSerializer = null;

            Serializer topSerializer = null;

            Map<Class, WideEntityMetadata> metadataMap = new HashMap<Class, WideEntityMetadata>();
            Map<Object, Class> topKeysToClassMap = new HashMap<Object, Class>();
            Map<String, Map<Object, Object>> data = new HashMap<String, Map<Object, Object>>();
            Map<Object, Serializer> serializers = new HashMap<Object, Serializer>();

            BytesArraySerializer bytesArraySerializer = BytesArraySerializer.get();

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

                if (!operationType.equals(collector.getOperationType(operation))) {
                    throw new RuntimeException("Operations in collector should be the same type");
                }

                if (rowKey == null) {
                    rowKey = operation.getRowKey();
                }

                if (!rowKey.equals(operation.getRowKey())) {
                    throw new RuntimeException("Operations should be execute on same rowKey");
                }

                RowSerializer entitySerializer = getRowSerializerForEntity(metadata, dataOperationsProfile);

                if (rowSerializer == null) {
                    rowSerializer = entitySerializer.getRowKeySerializer();
                }

                if (topSerializer == null) {
                    topSerializer = entitySerializer.getTopKeySerializer();
                } else if (!topSerializer.getClass().equals(entitySerializer.getTopKeySerializer().getClass())) {
                    throw new RuntimeException("Operations in collector should use same top key serializer");
                }

                //Top key might be null, cause row serializer for wide entity is universal
                Serializer valueSerializer = entitySerializer.getValueSerializer(null);

                if (operation.getTopKeys() != null) {
                    for (Object topKey : operation.getTopKeys()) {

                        String operationCFName = getCFName(metadata, operation.getRowKey(), topKey);
                        if (!data.containsKey(operationCFName)) {
                            data.put(operationCFName, new LinkedHashMap<Object, Object>());
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
                            data.put(operationCFName, new LinkedHashMap<Object, Object>());
                        }

                        data.get(operationCFName).put(topKey, bytesArraySerializer.fromByteBuffer(valueSerializer.toByteBuffer(entity)));
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
                            dataDriver.insert(hercules.getKeyspace(), cfName, dataOperationsProfile, operationRowSerializer, rowKey, data.get(cfName));
                            ;
                        }

                        break;
                    case GET:
                        List resultObjects = new ArrayList<Object>();

                        for (String cfName : data.keySet()) {
                            HerculesQueryResult queryResult = dataDriver.getSlice(hercules.getKeyspace(), cfName, dataOperationsProfile,
                                    operationRowSerializer, rowKey, new SliceDataSpecificator<Object>(data.get(cfName).keySet()));

                            for (Object topKey : queryResult.getEntries().keySet()) {
                                Object entity = serializers.get(topKey).fromByteBuffer(
                                        bytesArraySerializer.toByteBuffer((byte[]) queryResult.getEntries().get(topKey)));

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

    private <R, T> UniversalRowSerializer<R, T> getRowSerializerForEntity(WideEntityMetadata metadata, DataOperationsProfile dataOperationsProfile) {
        Serializer rowKeySerializer = metadata.getRowKeyMetadata().getSerializer() == null ?
                dataDriver.getSerializerForClass(metadata.getRowKeyMetadata().getKeyClass()) :
                serializerProvider.getSerializer(metadata.getRowKeyMetadata().getSerializer(), metadata.getRowKeyMetadata().getKeyClass());

        Serializer topKeySerializer = metadata.getTopKeyMetadata().getSerializer() == null ?
                dataDriver.getSerializerForClass(metadata.getTopKeyMetadata().getKeyClass()) :
                serializerProvider.getSerializer(metadata.getTopKeyMetadata().getSerializer(), metadata.getTopKeyMetadata().getKeyClass());

        Serializer universalSerializer = serializerProvider.getSerializer(metadata.getEntitySerializer(), metadata.getEntityClass());


        return new UniversalRowSerializer<R, T>(
                new InformerSerializer(rowKeySerializer, dataOperationsProfile),
                new InformerSerializer(topKeySerializer, dataOperationsProfile),
                new InformerSerializer(universalSerializer, dataOperationsProfile));
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
}
