package com.appmetr.hercules.manager;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.batch.BatchExecutor;
import com.appmetr.hercules.batch.BatchIterator;
import com.appmetr.hercules.batch.BatchProcessor;
import com.appmetr.hercules.batch.iterator.TupleBatchIterator;
import com.appmetr.hercules.driver.DataDriver;
import com.appmetr.hercules.driver.HerculesMultiQueryResult;
import com.appmetr.hercules.driver.serializer.ByteArrayRowSerializer;
import com.appmetr.hercules.keys.CollectionKeysExtractor;
import com.appmetr.hercules.metadata.CollectionIndexMetadata;
import com.appmetr.hercules.metadata.EntityMetadata;
import com.appmetr.hercules.metadata.ForeignKeyMetadata;
import com.appmetr.hercules.profile.DataOperationsProfile;
import com.appmetr.hercules.utils.Tuple2;
import com.appmetr.hercules.wide.SliceDataSpecificator;
import com.google.inject.Inject;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.ddl.ComparatorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.*;

public class IndexManager {
    private Logger logger = LoggerFactory.getLogger(IndexManager.class);

    @Inject private Hercules hercules;
    @Inject private DataDriver dataDriver;
    @Inject private EntityManager entityManager;

    public void checkAndCreateEntityIndexes(EntityMetadata metadata) {
        if (metadata.isCreatePrimaryKeyIndex()) {
            checkAndCreatePKIndex(metadata);
        }

        checkAndCreateFKIndexes(metadata);
    }

    <E, K> void updateIndexOnSave(E entity, E oldEntity, DataOperationsProfile dataOperationsProfile) {
        EntityMetadata metadata = entityManager.getMetadata(entity.getClass());

        K primaryKey = entityManager.getPrimaryKey(entity, metadata);
        Serializer<K> primaryKeySerializer = entityManager.getPrimaryKeySerializer(metadata);

        //create primary key index
        if (metadata.isCreatePrimaryKeyIndex()) {
            insertRowIndex(EntityManager.PRIMARY_KEY_CF_NAME, metadata.getColumnFamily(), StringSerializer.get(), primaryKey, entityManager.getPrimaryKeySerializer(metadata), dataOperationsProfile);
        }

        for (ForeignKeyMetadata foreignKeyMetadata : metadata.getIndexes().values()) {
            Serializer foreignKeySerializer = entityManager.getForeignKeySerializer(foreignKeyMetadata);
            Object foreignKey = entityManager.getForeignKeyFromEntity(entity, metadata, foreignKeyMetadata.getKeyClass());

            Object oldIndexKey = oldEntity == null ? null : entityManager.getForeignKeyFromEntity(oldEntity, metadata, foreignKeyMetadata.getKeyClass());

            if (foreignKey == null) {
                if (oldIndexKey != null) {
                    logger.debug(MessageFormat.format("Deleting index: column {0} from {1} by key {2} ", primaryKey, foreignKeyMetadata.getColumnFamily(), oldIndexKey));
                    deleteRowIndex(foreignKeyMetadata.getColumnFamily(), oldIndexKey, foreignKeySerializer, primaryKey, primaryKeySerializer, dataOperationsProfile);
                }
            } else {

                if (oldIndexKey != null && !foreignKey.equals(oldIndexKey)) {
                    logger.debug(MessageFormat.format("Deleting index: column {0} from {1} by key {2} ", primaryKey, foreignKeyMetadata.getColumnFamily(), oldIndexKey));
                    deleteRowIndex(foreignKeyMetadata.getColumnFamily(), oldIndexKey, foreignKeySerializer, primaryKey, primaryKeySerializer, dataOperationsProfile);
                }

                //don't recreate index if it's value equals to old one
                if (!foreignKey.equals(oldIndexKey)) {
                    insertRowIndex(foreignKeyMetadata.getColumnFamily(), foreignKey, foreignKeySerializer, primaryKey, primaryKeySerializer, dataOperationsProfile);
                }
            }
        }

        for (CollectionIndexMetadata collectionIndexMetadata : metadata.getCollectionIndexes().values()) {
            CollectionKeysExtractor<E, Object> keyExtractor = collectionIndexMetadata.getKeyExtractor();
            Serializer<Object> indexKeySerializer = keyExtractor.getKeySerializer();

            Set<Object> keysToAdd = new HashSet<Object>();
            Set<Object> keysToRemove = new HashSet<Object>();

            for (Object key : keyExtractor.extractKeys(entity)) {
                keysToAdd.add(key);
            }

            if (oldEntity != null) {
                for (Object key : keyExtractor.extractKeys(oldEntity)) {
                    if (keysToAdd.contains(key)) {
                        keysToAdd.remove(key);
                    } else {
                        keysToRemove.add(key);
                    }
                }
            }

            if (keysToAdd.size() > 0) {
                insertCollectionIndex(collectionIndexMetadata.getIndexColumnFamily(), keysToAdd, indexKeySerializer, primaryKey, primaryKeySerializer, dataOperationsProfile);
            }

            if (keysToRemove.size() > 0) {
                logger.debug(MessageFormat.format("Deleting indexes (during update): column {0} from {1} by keys {2} ", primaryKey, collectionIndexMetadata.getIndexColumnFamily(), keysToRemove));
                deleteCollectionIndex(collectionIndexMetadata.getIndexColumnFamily(), keysToRemove, indexKeySerializer, primaryKey, primaryKeySerializer, dataOperationsProfile);

            }
        }
    }

    <E, K> void updateIndexOnDelete(E entity, DataOperationsProfile dataOperationsProfile) {
        EntityMetadata metadata = entityManager.getMetadata(entity.getClass());

        K primaryKey = entityManager.getPrimaryKey(entity, metadata);
        Serializer<K> primaryKeySerializer = entityManager.getPrimaryKeySerializer(metadata);

        for (ForeignKeyMetadata index : metadata.getIndexes().values()) {
            Object indexKey = entityManager.getForeignKeyFromEntity(entity, metadata, index.getKeyClass());

            if (indexKey == null) {
                continue;
            }

            deleteRowIndex(index.getColumnFamily(), indexKey, entityManager.getForeignKeySerializer(index), primaryKey, primaryKeySerializer, dataOperationsProfile);
        }

        for (CollectionIndexMetadata collectionIndexMetadata : metadata.getCollectionIndexes().values()) {
            CollectionKeysExtractor<E, Object> keyExtractor = collectionIndexMetadata.getKeyExtractor();
            Serializer<Object> indexKeySerializer = keyExtractor.getKeySerializer();

            Set<Object> keysToRemove = new HashSet<Object>();

            for (Object key : keyExtractor.extractKeys(entity)) {
                keysToRemove.add(key);
            }
            deleteCollectionIndex(collectionIndexMetadata.getIndexColumnFamily(), keysToRemove, indexKeySerializer, primaryKey, primaryKeySerializer, dataOperationsProfile);
        }
        //redundant check. Only for reducing number of cassandra operations
        //delete primary key index
        if (metadata.isCreatePrimaryKeyIndex()) {
            deleteRowIndex(EntityManager.PRIMARY_KEY_CF_NAME, metadata.getColumnFamily(), StringSerializer.get(), primaryKey, primaryKeySerializer, dataOperationsProfile);
        }
    }


    private void checkAndCreatePKIndex(final EntityMetadata metadata) {
        String cfName = metadata.getColumnFamily();

        logger.debug("Checking PK index table for CF: " + cfName);

        HerculesMultiQueryResult result = dataDriver.getSlice(hercules.getKeyspace(), EntityManager.PRIMARY_KEY_CF_NAME, null,
                new ByteArrayRowSerializer<String, Object>(StringSerializer.get(), entityManager.getPrimaryKeySerializer(metadata)), Arrays.asList(cfName),
                new SliceDataSpecificator<Object>(null, null, false, 1));
        /*
            We convert all row keys into column values of IndexTables with following structure:
            PK_INDEX_CF:
                CFName1: rowKey1, rowKey2, ..., rowKeyN
                ...
                CFNameX: rowKey1, rowKey2, ..., rowKeyN
         */
        if (!result.hasResult()) {
            logger.info("Trying to create PK index row for CF: " + cfName);

            final Map<Object, Object> valuesToInsert = new HashMap<Object, Object>();
            new BatchExecutor<Object, Object>(
                    getEntityClassBatchIterator(metadata.getEntityClass()),
                    new BatchProcessor<Object>() {
                        @Override public void processBatch(List batch) {
                            for (Object entity : batch) {
                                final Object primaryKey = entityManager.getPrimaryKey(entity, metadata);
                                valuesToInsert.put(primaryKey, new byte[0]);
                            }
                        }
                    }
            ).execute();

            dataDriver.insert(hercules.getKeyspace(), EntityManager.PRIMARY_KEY_CF_NAME, null,
                    new ByteArrayRowSerializer<String, Object>(StringSerializer.get(), entityManager.getPrimaryKeySerializer(metadata)),
                    cfName, valuesToInsert, null);
        }
        logger.debug("PK index created for CF: " + cfName);
    }

    private void checkAndCreateFKIndexes(EntityMetadata metadata) {
        for (ForeignKeyMetadata index : metadata.getIndexes().values()) {
            logger.info(MessageFormat.format("Found index {0} for {1}", index.getKeyClass().getName(), metadata.getEntityClass().getName()));

            if (dataDriver.checkAndCreateColumnFamily(hercules.getCluster(), hercules.getKeyspaceName(), index.getColumnFamily(), ComparatorType.BYTESTYPE)) {
                logger.info("Creating index table: " + index.getColumnFamily());
                fillFKIndex(metadata, index);
            }
        }
        for (CollectionIndexMetadata collectionIndex : metadata.getCollectionIndexes().values()) {
            logger.info(MessageFormat.format("Found collection index {0} for {1}", collectionIndex.getIndexColumnFamily(), metadata.getEntityClass().getName()));

            if (dataDriver.checkAndCreateColumnFamily(hercules.getCluster(), hercules.getKeyspaceName(), collectionIndex.getIndexColumnFamily(), ComparatorType.BYTESTYPE)) {
                logger.info("Creating index table: " + collectionIndex.getIndexColumnFamily());
                fillCollectionIndex(metadata, collectionIndex);
            }
        }
    }

    private int fillFKIndex(final EntityMetadata metadata, final ForeignKeyMetadata keyMetadata) {
        final int[] rowsInserted = new int[1];
        final int[] rowsSkipped = new int[1];

        final Serializer primaryKeySerializer = entityManager.getPrimaryKeySerializer(metadata);
        final Serializer foreignKeySerializer = entityManager.getForeignKeySerializer(keyMetadata);

        new BatchExecutor<Object, Object>(
                getEntityClassBatchIterator(metadata.getEntityClass()),
                new BatchProcessor<Object>() {
                    @Override public void processBatch(List batch) {
                        for (Object entity : batch) {

                            Object primaryKey = entityManager.getPrimaryKey(entity, metadata);
                            Object foreignKey = entityManager.getForeignKeyFromEntity(entity, metadata, keyMetadata.getKeyClass());

                            if (foreignKey == null) {
                                rowsSkipped[0]++;
                                continue;
                            }

                            logger.debug("Inserting index [" + foreignKey + "][" + primaryKey + "].");
                            insertRowIndex(keyMetadata.getColumnFamily(), foreignKey, foreignKeySerializer, primaryKey, primaryKeySerializer, null);

                            rowsInserted[0]++;
                        }
                    }
                }
        ).execute();

        logger.info("Inserted " + rowsInserted[0] + " rows into index table: " + keyMetadata.getColumnFamily() + ". Rows skipped: " + rowsSkipped[0]);
        return rowsInserted[0];
    }

    private int fillCollectionIndex(final EntityMetadata metadata, final CollectionIndexMetadata indexMetadata) {
        final int[] rowsInserted = new int[1];
        final int[] rowsSkipped = new int[1];
        final int[] entitiesProcessed = new int[1];

        final Serializer primaryKeySerializer = entityManager.getPrimaryKeySerializer(metadata);
        final CollectionKeysExtractor keyExtractor = indexMetadata.getKeyExtractor();
        final Serializer indexKeySerializer = keyExtractor.getKeySerializer();

        new BatchExecutor<Object, Object>(
                getEntityClassBatchIterator(metadata.getEntityClass()),
                new BatchProcessor<Object>() {
                    @Override public void processBatch(List batch) {
                        for (Object entity : batch) {
                            Object primaryKey = entityManager.getPrimaryKey(entity, metadata);

                            boolean hasIndex = false;
                            for (Object key : keyExtractor.extractKeys(entity)) {
                                logger.debug("Inserting index [" + key + "][" + primaryKey + "].");
                                insertRowIndex(indexMetadata.getIndexColumnFamily(), key, indexKeySerializer, primaryKey, primaryKeySerializer, null);
                                hasIndex = true;
                                rowsInserted[0]++;
                            }

                            if (!hasIndex) {
                                rowsSkipped[0]++;
                            }
                            entitiesProcessed[0]++;
                        }
                    }
                }
        ).execute();

        logger.info("Inserted " + rowsInserted[0] + " rows into index table: " + indexMetadata.getIndexColumnFamily() + ". Entities processed: " + entitiesProcessed[0] + " Rows skipped: " + rowsSkipped[0]);
        return rowsInserted[0];
    }

    private <K, T> void insertRowIndex(String columnFamily, K indexRowKey, Serializer<K> indexRowKeySerializer, T indexValue, Serializer<T> indexValueSerializer, DataOperationsProfile dataOperationsProfile) {
        dataDriver.insert(hercules.getKeyspace(), columnFamily, dataOperationsProfile,
                new ByteArrayRowSerializer<K, T>(indexRowKeySerializer, indexValueSerializer),
                indexRowKey, indexValue, new byte[0], DataDriver.EMPTY_TTL);
    }

    private <K, T> void insertCollectionIndex(String columnFamily, Set<K> indexRowKeys, Serializer<K> indexRowKeySerializer, T indexValue, Serializer<T> indexValueSerializer, DataOperationsProfile dataOperationsProfile) {
        Map<K, Map<T, Object>> multirowInsert = new HashMap<K, Map<T, Object>>();
        Map<T, Object> columns = new HashMap<T, Object>();
        columns.put(indexValue, new byte[0]);

        Map<K, Map<T, Integer>> ttls = new HashMap<K, Map<T, Integer>>();
        Map<T, Integer> emptyTTL = new HashMap<T, Integer>();
        emptyTTL.put(indexValue, DataDriver.EMPTY_TTL);

        for (K key : indexRowKeys) {
            multirowInsert.put(key, new HashMap<T, Object>(columns));
            ttls.put(key, new HashMap<T, Integer>(emptyTTL));
        }

        dataDriver.insert(hercules.getKeyspace(), columnFamily, dataOperationsProfile,
                new ByteArrayRowSerializer<K, T>(indexRowKeySerializer, indexValueSerializer), multirowInsert, ttls);
    }

//    private <K, T> void deleteRowIndexes(String columnFamily, K indexRowKey, Serializer<K> indexRowKeySerializer, List<T> columns, Serializer<T> columnSerializer, DataOperationsProfile dataOperationsProfile) {
//        dataDriver.delete(hercules.getKeyspace(), columnFamily, dataOperationsProfile,
//                new ByteArrayRowSerializer<K, T>(indexRowKeySerializer, columnSerializer),
//                indexRowKey, columns);
//    }

    private <K, T> void deleteRowIndex(String columnFamily, K indexRowKey, Serializer<K> indexRowKeySerializer, T column, Serializer<T> columnSerializer, DataOperationsProfile dataOperationsProfile) {
        dataDriver.delete(hercules.getKeyspace(), columnFamily, dataOperationsProfile,
                new ByteArrayRowSerializer<K, T>(indexRowKeySerializer, columnSerializer),
                indexRowKey, Arrays.<T>asList(column));
    }

    private <K, T> void deleteCollectionIndex(String columnFamily, Set<K> indexRowKeys, Serializer<K> indexRowKeySerializer, T column, Serializer<T> columnSerializer, DataOperationsProfile dataOperationsProfile) {
        Map<K, Iterable<T>> multirowDelete = new HashMap<K, Iterable<T>>();

        for (K key : indexRowKeys) {
            multirowDelete.put(key, Arrays.<T>asList(column));
        }

        dataDriver.delete(hercules.getKeyspace(), columnFamily, dataOperationsProfile,
                new ByteArrayRowSerializer<K, T>(indexRowKeySerializer, columnSerializer), multirowDelete);
    }

    private BatchIterator<Object, Object> getEntityClassBatchIterator(final Class clazz) {
        return new TupleBatchIterator<Object, Object>(null, null) {
            @Override
            protected Tuple2 getRangeTuple(Object from, Object to, int batchSize, DataOperationsProfile dataOperationsProfile) {
                return entityManager.getRange(clazz, from, to, batchSize, dataOperationsProfile);
            }

            @Override protected Object getKey(Object item) {
                return entityManager.getPK(item);
            }
        };
    }
}
