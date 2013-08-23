package com.appmetr.hercules.manager;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.HerculesMonitoringGroup;
import com.appmetr.hercules.FieldFilter;
import com.appmetr.hercules.annotations.Id;
import com.appmetr.hercules.driver.DataDriver;
import com.appmetr.hercules.driver.HerculesMultiQueryResult;
import com.appmetr.hercules.driver.HerculesQueryResult;
import com.appmetr.hercules.driver.serializer.ByteArrayRowSerializer;
import com.appmetr.hercules.driver.serializer.ColumnRowSerializer;
import com.appmetr.hercules.driver.serializer.RowSerializer;
import com.appmetr.hercules.keys.ForeignKey;
import com.appmetr.hercules.metadata.EntityMetadata;
import com.appmetr.hercules.metadata.ForeignKeyMetadata;
import com.appmetr.hercules.serializers.AbstractHerculesSerializer;
import com.appmetr.hercules.serializers.SerializerProvider;
import com.appmetr.hercules.utils.Tuple2;
import com.appmetr.hercules.wide.SliceDataSpecificator;
import com.appmetr.monblank.Monitoring;
import com.appmetr.monblank.StopWatch;
import com.google.inject.Inject;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.text.MessageFormat;
import java.util.*;

public class EntityManager {
    private Logger logger = LoggerFactory.getLogger(EntityManager.class);

    public static final String PRIMARY_KEY_CF_NAME = "PrimaryKeyIndices";
    public static final String SERIALIZED_ENTITY_TOP_KEY = "entityData";
    public static final String METADATA_COLUMN_NAME = "*";

    @Inject private Hercules hercules;
    @Inject private DataDriver dataDriver;
    @Inject private IndexManager indexManager;
    @Inject private SerializerProvider serializerProvider;
    @Inject private Monitoring monitoring;
    @Inject private EntityListenerInvocationHelper listenerInvocationHelper;

    //public level
    public <E, K> K getPK(E entity) {
        return (K) getPrimaryKey(entity, getMetadata(entity.getClass()));
    }

    public <E, K> Serializer<K> getPKSerializer(Class<E> clazz) {
        return getPrimaryKeySerializer(getMetadata(clazz));
    }

    public <E, K> E get(Class<E> clazz, K primaryKey) {
        if (primaryKey == null) return null;

        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_EM, "Get by PK " + clazz.getSimpleName());

        try {
            EntityMetadata metadata = getMetadata(clazz);

            HerculesQueryResult<String> queryResult = dataDriver.getRow(hercules.getKeyspace(), metadata.getColumnFamily(), getRowSerializerForEntity(metadata), primaryKey);

            if (queryResult.hasResult()) {
                return convertToEntity(clazz, primaryKey, queryResult.getEntries());
            } else {
                return null;
            }
        } catch (RuntimeException e) {
            monitoring.inc(HerculesMonitoringGroup.HERCULES_EM, "Error: getting entity");
            logger.error("Entity get by row key exception", e);
            throw e;
        } finally {
            monitor.stop();
        }
    }

    public <E, K> List<E> get(Class<E> clazz, Iterable<K> primaryKeys) {
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_EM, "Get list by PK " + clazz.getSimpleName());

        try {
            EntityMetadata metadata = getMetadata(clazz);

            HerculesMultiQueryResult<K, String> queryResult = dataDriver.getRows(hercules.getKeyspace(), metadata.getColumnFamily(),
                    this.<K>getRowSerializerForEntity(metadata), primaryKeys);

            if (queryResult.hasResult()) {
                return convertToEntityList(clazz, queryResult.getEntries());
            } else {
                return new ArrayList<E>();
            }
        } catch (RuntimeException e) {
            monitoring.inc(HerculesMonitoringGroup.HERCULES_EM, "Error: getting entities list");
            logger.error("Entity get by row keys exception", e);
            throw e;
        } finally {
            monitor.stop();
        }
    }

    public <E, K> List<K> getKeyRange(Class<E> clazz, K from, K to, Integer count) {
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_EM, "Get key range " + clazz.getSimpleName());

        try {
            EntityMetadata metadata = getMetadata(clazz);

            return dataDriver.getKeyRange(hercules.getKeyspace(), metadata.getColumnFamily(), this.<K>getRowSerializerForEntity(metadata), from, to, count);
        } catch (RuntimeException e) {
            monitoring.inc(HerculesMonitoringGroup.HERCULES_EM, "Error: getting entities key range");
            logger.error("Get key range exception", e);
            throw e;
        } finally {
            monitor.stop();
        }
    }

    public <E, K> Tuple2<List<E>, K> getRange(Class<E> clazz, K from, K to, Integer count) {
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_EM, "Get range " + clazz.getSimpleName());

        try {
            EntityMetadata metadata = getMetadata(clazz);

            HerculesMultiQueryResult<K, String> queryResult = dataDriver.getRangeSlice(hercules.getKeyspace(), metadata.getColumnFamily(),
                    this.<K>getRowSerializerForEntity(metadata), from, to, count, new SliceDataSpecificator<String>(null, null, false, null));

            if (queryResult.hasResult()) {
                return new Tuple2<List<E>, K>(convertToEntityList(clazz, queryResult.getEntries()), queryResult.getLastKey());
            } else {
                return new Tuple2<List<E>, K>(new ArrayList<E>(), queryResult.getLastKey());
            }
        } catch (RuntimeException e) {
            monitoring.inc(HerculesMonitoringGroup.HERCULES_EM, "Error: getting entities range");
            logger.error("Get entity range exception", e);
            throw e;
        } finally {
            monitor.stop();
        }
    }

    public <E> List<E> getAll(Class<E> clazz) {
        EntityMetadata metadata = getMetadata(clazz);

        List<E> entities;
        if (metadata.isCreatePrimaryKeyIndex()) {
            entities = getAllByIndex(clazz);
        } else {
            entities = getAllPlain(clazz);
        }

        return entities;
    }

    public <E> List<E> getByFK(Class<E> clazz, ForeignKey foreignKey) {
        return getByFK(clazz, foreignKey, null);
    }

    public <E> E getSingleByFK(Class<E> clazz, ForeignKey foreignKey) {
        List<E> entites = getByFK(clazz, foreignKey, 1);

        return entites.size() > 0 ? entites.get(0) : null;
    }

    public <E> int getCountByFK(Class<E> clazz, ForeignKey foreignKey) {
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_EM, "Get count by FK " + clazz.getSimpleName());

        try {
            EntityMetadata metadata = getMetadata(clazz);
            ForeignKeyMetadata foreignKeyMetadata = metadata.getIndexMetadata(foreignKey.getClass());

            return dataDriver.getTopCount(hercules.getKeyspace(), foreignKeyMetadata.getColumnFamily(),
                    new ByteArrayRowSerializer<Object, Object>(
                            getForeignKeySerializer(foreignKeyMetadata),
                            getPrimaryKeySerializer(metadata)),
                    foreignKey, null, null, null);
        } catch (RuntimeException e) {
            monitoring.inc(HerculesMonitoringGroup.HERCULES_EM, "Error: getting count by FK");
            logger.error("Get count by FK exception", e);
            throw e;
        } finally {
            monitor.stop();
        }
    }

    public <E> void save(E entity, FieldFilter fieldFilter) {
        EntityMetadata metadata = getMetadata(entity.getClass());
        Object primaryKey = getPrimaryKey(entity, metadata);

        save(primaryKey, entity, fieldFilter);
    }

    public <E, K> void save(K primaryKey, E entity, FieldFilter fieldFilter) {
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_EM, "Save " + entity.getClass().getSimpleName());

        try {
            EntityMetadata metadata = getMetadata(entity.getClass());
            E prePersistResult = listenerInvocationHelper.invokePrePersistListener(metadata.getListenerMetadata(), entity);
            if (prePersistResult != null) {
                entity = prePersistResult;
            }

            if (primaryKey == null && metadata.isPrimaryKeyGenerated()) {
                try {
                    Field primaryKeyField = metadata.getPrimaryKeyMetadata().getField();
                    UUID uid = UUID.randomUUID();

                    if (metadata.getPrimaryKeyMetadata().getKeyClass().isAssignableFrom(UUID.class)) {
                        primaryKey = (K) uid;
                    } else if (metadata.getPrimaryKeyMetadata().getKeyClass().isAssignableFrom(String.class)) {
                        primaryKey = (K) uid.toString();
                    } else {
                        String primaryKeyFieldClassName = primaryKeyField == null ? null : primaryKeyField.getClass().getName();
                        throw new RuntimeException(MessageFormat.format("Currently we support only java.util.UUID and String id generation, but entity of type {0} have id with class {1}", entity.getClass().getName(), primaryKeyFieldClassName));
                    }

                    if (primaryKeyField != null) {
                        metadata.getPrimaryKeyMetadata().getField().set(entity, primaryKey);
                    }
                } catch (IllegalAccessException ex) {
                    String descr = "Unable to set generated id for entity: " + entity.getClass();
                    monitoring.inc(HerculesMonitoringGroup.HERCULES_EM, "Error: " + descr);
                    throw new RuntimeException(descr, ex);
                }
            }

            E oldEntity = null;
            if (metadata.getIndexes().size() > 0) {
                HerculesQueryResult<String> queryResult = dataDriver.getRow(hercules.getKeyspace(), metadata.getColumnFamily(), getRowSerializerForEntity(metadata), primaryKey);

                if (queryResult.hasResult()) {
                    oldEntity = (E) this.<E, K>convertToEntity(metadata.getEntityClass(), primaryKey, queryResult.getEntries());
                }
            }

            saveEntity(primaryKey, entity, fieldFilter);
            indexManager.updateIndexOnSave(entity, oldEntity);

            listenerInvocationHelper.invokePostPersistListener(metadata.getListenerMetadata(), entity);
        } catch (RuntimeException e) {
            monitoring.inc(HerculesMonitoringGroup.HERCULES_EM, "Error: saving entity");
            logger.error("Entity save exception" + (entity != null ? ". Entity class:" + entity.getClass().getSimpleName() : ""), e);
            throw e;
        } finally {
            monitor.stop();
        }
    }

    public <E> void delete(E entity) {
        if (entity == null) return;

        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_EM, "Delete " + entity.getClass().getSimpleName());

        try {
            EntityMetadata metadata = getMetadata(entity.getClass());
            Object primaryKey = getPrimaryKey(entity, metadata);

            delete(primaryKey, getMetadata(metadata.getEntityClass()));

            indexManager.updateIndexOnDelete(entity);
        } catch (RuntimeException e) {
            monitoring.inc(HerculesMonitoringGroup.HERCULES_EM, "Error: deleting exception");
            logger.error("Entity delete exception", e);
            throw e;
        } finally {
            monitor.stop();
        }
    }

    public <K> void delete(Class clazz, K primaryKey) {
        //Can't delete entity by PK directly, cause we need to delete indexes
        delete(get(clazz, primaryKey));
    }

    //package level
    EntityMetadata getMetadata(Class entityClass) {
        return hercules.getMetadata(entityClass);
    }

    <E, T> T getPrimaryKey(E entity, EntityMetadata metadata) {
        final Field primaryKeyField = metadata.getPrimaryKeyMetadata().getField();

        if (primaryKeyField == null) {
            throw new RuntimeException(Id.class.getSimpleName() + " field isn't declared for entity " + entity.getClass() + " in range query");
        }

        try {
            return (T) primaryKeyField.get(entity);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    <K> Serializer<K> getPrimaryKeySerializer(EntityMetadata metadata) {
        Class keyClass = metadata.getPrimaryKeyMetadata().getKeyClass();
        Class keySerializer = metadata.getPrimaryKeyMetadata().getSerializer();

        return keySerializer == null ?
                dataDriver.<K>getSerializerForClass(keyClass) :
                serializerProvider.<K>getSerializer(keySerializer, keyClass);
    }

    <K> Serializer<K> getForeignKeySerializer(ForeignKeyMetadata metadata) {
        return serializerProvider.getSerializer(metadata.getSerializer(), metadata.getKeyClass());
    }

    <E, F> F getForeignKeyFromEntity(E entity, EntityMetadata metadata, Class<? extends ForeignKey> foreignKeyClass) {
        ForeignKeyMetadata keyMetadata = metadata.getIndexMetadata(foreignKeyClass);

        try {
            F foreignKey = (F) foreignKeyClass.newInstance();

            boolean notNullValueExist = false;
            for (Field keyField : keyMetadata.getKeyFields()) {
                try {
                    //try to find field in entity
                    Field entityField = metadata.getEntityClass().getDeclaredField(keyField.getName());
                    entityField.setAccessible(true);

                    Object fieldValue = entityField.get(entity);
                    keyField.set(foreignKey, fieldValue);

                    if (fieldValue != null) {
                        notNullValueExist = true;
                    }
                } catch (NoSuchFieldException noField) {
                    //try to find field in entity key
                    Field primaryKeyField = metadata.getPrimaryKeyMetadata().getKeyClass().getDeclaredField(keyField.getName());
                    primaryKeyField.setAccessible(true);

                    Object fieldValue = primaryKeyField.get(metadata.getPrimaryKeyMetadata().getField().get(entity));
                    keyField.set(foreignKey, fieldValue);

                    if (fieldValue != null) {
                        notNullValueExist = true;
                    }
                }
            }

            return notNullValueExist ? foreignKey : null;
        } catch (NoSuchFieldException e) {
            throw new RuntimeException("Failed to serialize foreign key of class " + foreignKeyClass.getName() + " and entity class " + entity.getClass().getName(), e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to serialize foreign key of class " + foreignKeyClass.getName() + " and entity class " + entity.getClass().getName(), e);
        } catch (InstantiationException e) {
            throw new RuntimeException("Failed to serialize foreign key of class " + foreignKeyClass.getName() + " and entity class " + entity.getClass().getName(), e);
        }

    }

    //private level
    private <E, K> List<E> getAllPlain(Class<E> clazz) {
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_EM, "Get all plain " + clazz.getSimpleName());

        try {
            EntityMetadata metadata = getMetadata(clazz);

            HerculesMultiQueryResult<K, String> result = dataDriver.getAllRows(hercules.getKeyspace(), metadata.getColumnFamily(), this.<K>getRowSerializerForEntity(metadata));

            List<E> entities = new ArrayList<E>();
            if (result.hasResult()) {
                for (K primaryKey : result.getEntries().keySet()) {
                    entities.add(convertToEntity(clazz, primaryKey, result.getEntries().get(primaryKey)));
                }
            }

            return entities;
        } catch (RuntimeException e) {
            monitoring.inc(HerculesMonitoringGroup.HERCULES_EM, "Error: get all plain exception");
            logger.error("Entity get all plain exception", e);
            throw e;
        } finally {
            monitor.stop();
        }
    }

    private <E, K> List<E> getAllByIndex(Class<E> clazz) {
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_EM, "Get all by index " + clazz.getSimpleName());

        try {
            EntityMetadata metadata = getMetadata(clazz);

            HerculesQueryResult<K> queryResult = dataDriver.getRow(
                    hercules.getKeyspace(),
                    PRIMARY_KEY_CF_NAME,
                    new ByteArrayRowSerializer<String, K>(StringSerializer.get(), this.<K>getPrimaryKeySerializer(metadata)),
                    metadata.getColumnFamily());

            Set<K> indexes = queryResult.getEntries().keySet();

            List<E> entities = new ArrayList<E>();
            if (indexes.size() > 0) {
                HerculesMultiQueryResult<K, String> entitiesQueryResult = dataDriver.getRows(hercules.getKeyspace(), metadata.getColumnFamily(),
                        this.<K>getRowSerializerForEntity(metadata), indexes);

                if (queryResult.hasResult()) {
                    for (Map.Entry<K, LinkedHashMap<String, Object>> entry : entitiesQueryResult.getEntries().entrySet()) {
                        entities.add(convertToEntity(clazz, entry.getKey(), entry.getValue()));
                    }
                }
            }

            return entities;
        } catch (RuntimeException e) {
            monitoring.inc(HerculesMonitoringGroup.HERCULES_EM, "Error: get all by index exception");
            logger.error("Entity get all by index exception", e);
            throw e;
        } finally {
            monitor.stop();
        }
    }

    private <E, K> List<E> getByFK(Class<E> clazz, ForeignKey foreignKey, Integer count) {
        StopWatch monitor = monitoring.start(HerculesMonitoringGroup.HERCULES_EM, "Get list by FK " + clazz.getSimpleName());

        try {
            EntityMetadata metadata = getMetadata(clazz);
            ForeignKeyMetadata foreignKeyMetadata = metadata.getIndexMetadata(foreignKey.getClass());

            HerculesQueryResult<K> queryResult = dataDriver.getSlice(hercules.getKeyspace(), foreignKeyMetadata.getColumnFamily(),
                    new ByteArrayRowSerializer<Object, K>(getForeignKeySerializer(metadata.getIndexMetadata(foreignKey.getClass())), this.<K>getPrimaryKeySerializer(metadata)),
                    foreignKey, new SliceDataSpecificator<K>(null, null, false, count));

            if (queryResult.hasResult()) {
                return get(clazz, queryResult.getEntries().keySet());
            } else {
                return new ArrayList<E>();
            }
        } catch (RuntimeException e) {
            monitoring.inc(HerculesMonitoringGroup.HERCULES_EM, "Error: get list by FK exception");
            logger.error("Entity get list by FK exception", e);
            throw e;
        } finally {
            monitor.stop();
        }
    }

    private <K> void delete(K primaryKey, EntityMetadata metadata) {
        dataDriver.delete(hercules.getKeyspace(), metadata.getColumnFamily(),
                new ByteArrayRowSerializer<K, String>(this.<K>getPrimaryKeySerializer(metadata), dataDriver.<String>getSerializerForClass(String.class)),
                primaryKey);
    }

    private <E, K> E convertToEntity(Class<E> clazz, K primaryKey, Map<String, Object> values) {
        try {
            EntityMetadata metadata = getMetadata(clazz);
            if (!metadata.getEntityClass().equals(clazz)) {
                throw new RuntimeException(MessageFormat.format("Specified entity class differ from metadata class. entity: {0}, metadata: {1}", clazz, metadata.getEntityClass()));
            }
            if (metadata.getEntitySerializer() != null) {
                if (!values.containsKey(SERIALIZED_ENTITY_TOP_KEY)) {
                    throw new RuntimeException("Serialized entity data column doesn't exist for entity class " +
                            metadata.getEntityClass().getSimpleName());
                }

                return (E) values.get(SERIALIZED_ENTITY_TOP_KEY);
            } else {
                Constructor<E> constructor = clazz.getDeclaredConstructor();
                constructor.setAccessible(true);
                E entity = constructor.newInstance();

                E preLoadResult = listenerInvocationHelper.invokePreLoadListener(metadata.getListenerMetadata(), entity);
                if (preLoadResult != null) entity = preLoadResult;

                try {
                    HashSet<String> processedColumns = new HashSet<String>();
                    Field primaryKeyField = metadata.getPrimaryKeyMetadata().getField();

                    if (primaryKeyField != null) {
                        primaryKeyField.set(entity, primaryKey);
                    }

                    for (Map.Entry<Field, String> entry : metadata.getFieldToColumn().entrySet()) {
                        String column = entry.getValue();

                        if (!processedColumns.contains(column)) {
                            Object columnValue = values.get(column);
                            Object entityDefaultValue = entry.getKey().get(entity);

                            if (columnValue == null && metadata.isNotNullColumn(column)) {
                                if (entityDefaultValue == null) {
                                    throw new RuntimeException("Default value empty for entity class " +
                                            metadata.getEntityClass().getSimpleName() + " field " + column);
                                }

                                columnValue = entityDefaultValue;
                            }

                            entry.getKey().set(entity, columnValue);
                            processedColumns.add(column);
                        }
                    }
                } catch (IllegalAccessException e) {
                    throwConvertError(clazz, values, e);
                }

                E postLoadResult = listenerInvocationHelper.invokePostLoadListener(metadata.getListenerMetadata(), entity);
                return postLoadResult == null ? entity : postLoadResult;
            }
        } catch (InstantiationException e) {
            return throwConvertError(clazz, values, e);
        } catch (IllegalAccessException e) {
            return throwConvertError(clazz, values, e);
        } catch (NoSuchMethodException e) {
            return throwConvertError(clazz, values, e);
        } catch (InvocationTargetException e) {
            return throwConvertError(clazz, values, e);
        }
    }

    private <E, K> List<E> convertToEntityList(Class<E> clazz, Map<K, LinkedHashMap<String, Object>> values) {
        List<E> entities = new ArrayList<E>();

        for (Map.Entry<K, LinkedHashMap<String, Object>> entry : values.entrySet()) {
            E entity = convertToEntity(clazz, entry.getKey(), entry.getValue());

            if (entity != null)
                entities.add(entity);
        }

        return entities;
    }

    private <E> E throwConvertError(Class<E> clazz, Map<String, Object> values, Exception e) {
        monitoring.inc(HerculesMonitoringGroup.HERCULES_EM, "Error converting map to " + clazz.getName());
        String descr = "Failed to convert map to " + clazz.getName() + " Values Map: " + values.toString();
        throw new RuntimeException(descr, e);
    }

    private <E, K> void saveEntity(K primaryKey, E entity, FieldFilter fieldFilter) {
        EntityMetadata metadata = getMetadata(entity.getClass());

        if (!entity.getClass().equals(metadata.getEntityClass())) {
            throw new RuntimeException(MessageFormat.format("Specified entity class differ from metadata class. entity: {0}, metadata: {1}", entity.getClass(), metadata.getEntityClass()));
        }

        try {
            Map<String, Object> values = new HashMap<String, Object>();

            Class<? extends AbstractHerculesSerializer> entitySerializerClass = metadata.getEntitySerializer();
            if (entitySerializerClass != null) {
                if (fieldFilter == null) {
                    throw new RuntimeException(MessageFormat.format("Selective save doesn't support for entity {0} with serializers", entity.getClass().getSimpleName()));
                }
                values.put(SERIALIZED_ENTITY_TOP_KEY, entity);
            } else {
                boolean notNullValueExisted = false;

                for (Map.Entry<Field, String> entry : metadata.getFieldToColumn().entrySet()) {
                    Object value = entry.getKey().get(entity);

                    if (fieldFilter == null || fieldFilter.accept(entry.getKey())) {
                        if (value != null) {
                            notNullValueExisted = true;
                        }
                        values.put(entry.getValue(), value);
                    }

                }

                if (!notNullValueExisted) {
                    values.put(METADATA_COLUMN_NAME, new byte[0]);
                } else {
                    values.put(METADATA_COLUMN_NAME, null);
                }
            }

            dataDriver.insert(hercules.getKeyspace(), metadata.getColumnFamily(), getRowSerializerForEntity(metadata), primaryKey, values);
        } catch (IllegalAccessException e) {
            String msg = "Error: build value map for " + entity.getClass().getName();
            monitoring.inc(HerculesMonitoringGroup.HERCULES_EM, msg);
            throw new RuntimeException(msg, e);
        }
    }

    private Serializer getFieldSerializer(String fieldName, EntityMetadata metadata) {
        Class<? extends AbstractHerculesSerializer> serializerClass = metadata.getColumnSerializer(fieldName);
        if (serializerClass != null) {
            return serializerProvider.getSerializer(serializerClass, metadata.getColumnClass(fieldName));
        }

        return dataDriver.getSerializerForClass(metadata.getColumnClass(fieldName));
    }

    private <K> RowSerializer<K, String> getRowSerializerForEntity(EntityMetadata metadata) {
        Map<String, Serializer> columnSerializers = new HashMap<String, Serializer>();

        if (metadata.getEntitySerializer() != null) {
            AbstractHerculesSerializer entitySerializer = serializerProvider.getSerializer(metadata.getEntitySerializer(), metadata.getEntityClass());
            columnSerializers.put(SERIALIZED_ENTITY_TOP_KEY, entitySerializer);
        } else {
            for (Map.Entry<String, Class> entry : metadata.getColumnClasses().entrySet()) {
                Serializer fieldSerializer = getFieldSerializer(entry.getKey(), metadata);
                columnSerializers.put(entry.getKey(), fieldSerializer);
            }
        }

        columnSerializers.put(METADATA_COLUMN_NAME, BytesArraySerializer.get());

        return new ColumnRowSerializer<K, String>(this.<K>getPrimaryKeySerializer(metadata), StringSerializer.get(), columnSerializers);
    }
}
