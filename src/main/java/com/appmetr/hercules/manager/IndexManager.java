package com.appmetr.hercules.manager;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.batch.BatchExecutor;
import com.appmetr.hercules.batch.BatchIterator;
import com.appmetr.hercules.batch.BatchProcessor;
import com.appmetr.hercules.batch.iterator.TupleBatchIterator;
import com.appmetr.hercules.driver.DataDriver;
import com.appmetr.hercules.driver.HerculesMultiQueryResult;
import com.appmetr.hercules.driver.serializer.ByteArrayRowSerializer;
import com.appmetr.hercules.metadata.EntityMetadata;
import com.appmetr.hercules.metadata.ForeignKeyMetadata;
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

    <E, K> void updateIndexOnSave(E entity, E oldEntity) {
        EntityMetadata metadata = entityManager.getMetadata(entity.getClass());

        K primaryKey = entityManager.getPrimaryKey(entity, metadata);
        Serializer<K> primaryKeySerializer = entityManager.getPrimaryKeySerializer(metadata);

        //create primary key index
        if (metadata.isCreatePrimaryKeyIndex()) {
            insertRowIndex(EntityManager.PRIMARY_KEY_CF_NAME, metadata.getColumnFamily(), StringSerializer.get(), primaryKey, entityManager.getPrimaryKeySerializer(metadata));
        }

        for (ForeignKeyMetadata foreignKeyMetadata : metadata.getIndexes().values()) {
            Serializer foreignKeySerializer = entityManager.getForeignKeySerializer(foreignKeyMetadata);
            Object foreignKey = entityManager.getForeignKeyFromEntity(entity, metadata, foreignKeyMetadata.getKeyClass());

            Object oldIndexKey = oldEntity == null ? null : entityManager.getForeignKeyFromEntity(oldEntity, metadata, foreignKeyMetadata.getKeyClass());
            List<K> indexedKeys = new ArrayList<K>(1);
            indexedKeys.add(primaryKey);

            if (foreignKey == null) {
                if (oldIndexKey != null) {
                    logger.debug(MessageFormat.format("Deleting index: column {0} from {1} by key {2} ", primaryKey, foreignKeyMetadata.getColumnFamily(), oldIndexKey));
                    deleteRowIndexes(foreignKeyMetadata.getColumnFamily(), oldIndexKey, foreignKeySerializer, indexedKeys, primaryKeySerializer);
                }
            } else {

                if (oldIndexKey != null && !foreignKey.equals(oldIndexKey)) {
                    logger.debug(MessageFormat.format("Deleting index: column {0} from {1} by key {2} ", primaryKey, foreignKeyMetadata.getColumnFamily(), oldIndexKey));
                    deleteRowIndexes(foreignKeyMetadata.getColumnFamily(), oldIndexKey, foreignKeySerializer, indexedKeys, primaryKeySerializer);
                }

                //don't recreate index if it's value equals to old one
                if (!foreignKey.equals(oldIndexKey)) {
                    insertRowIndex(foreignKeyMetadata.getColumnFamily(), foreignKey, foreignKeySerializer, primaryKey, primaryKeySerializer);
                }
            }
        }
    }

    <E, K> void updateIndexOnDelete(E entity) {
        EntityMetadata metadata = entityManager.getMetadata(entity.getClass());

        K primaryKey = entityManager.getPrimaryKey(entity, metadata);
        Serializer<K> primaryKeySerializer = entityManager.getPrimaryKeySerializer(metadata);

        for (ForeignKeyMetadata index : metadata.getIndexes().values()) {
            List<K> indexedId = new ArrayList<K>(1);
            indexedId.add(primaryKey);
            Object indexKey = entityManager.getForeignKeyFromEntity(entity, metadata, index.getKeyClass());

            if (indexKey == null) {
                continue;
            }

            deleteRowIndexes(index.getColumnFamily(), indexKey, entityManager.getForeignKeySerializer(index), indexedId, primaryKeySerializer);
        }

        //redundant check. Only for reducing number of cassandra operations
        //delete primary key index
        if (metadata.isCreatePrimaryKeyIndex()) {
            deleteRowIndexes(EntityManager.PRIMARY_KEY_CF_NAME, metadata.getColumnFamily(), StringSerializer.get(), Arrays.asList(primaryKey), primaryKeySerializer);
        }
    }


    private void checkAndCreatePKIndex(final EntityMetadata metadata) {
        String cfName = metadata.getColumnFamily();

        logger.debug("Checking PK index table for CF: " + cfName);

        HerculesMultiQueryResult result = dataDriver.getSlice(hercules.getKeyspace(), EntityManager.PRIMARY_KEY_CF_NAME,
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

            dataDriver.insert(hercules.getKeyspace(), EntityManager.PRIMARY_KEY_CF_NAME,
                    new ByteArrayRowSerializer<String, Object>(StringSerializer.get(), entityManager.getPrimaryKeySerializer(metadata)),
                    cfName, valuesToInsert);
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
                            insertRowIndex(keyMetadata.getColumnFamily(), foreignKey, foreignKeySerializer, primaryKey, primaryKeySerializer);

                            rowsInserted[0]++;
                        }
                    }
                }
        ).execute();

        logger.info("Inserted " + rowsInserted[0] + " rows into index table: " + keyMetadata.getColumnFamily() + ". Rows skipped: " + rowsSkipped[0]);
        return rowsInserted[0];
    }

    private <K, T> void insertRowIndex(String columnFamily, K indexRowKey, Serializer<K> indexRowKeySerializer, T indexValue, Serializer<T> indexValueSerializer) {
        dataDriver.insert(hercules.getKeyspace(), columnFamily,
                new ByteArrayRowSerializer<K, T>(indexRowKeySerializer, indexValueSerializer),
                indexRowKey, indexValue, new byte[0]);
    }

    private <K, T> void deleteRowIndexes(String columnFamily, K indexRowKey, Serializer<K> indexRowKeySerializer, List<T> columns, Serializer<T> columnSerializer) {
        dataDriver.delete(hercules.getKeyspace(), columnFamily,
                new ByteArrayRowSerializer<K, T>(indexRowKeySerializer, columnSerializer),
                indexRowKey, columns);
    }

    private BatchIterator<Object, Object> getEntityClassBatchIterator(final Class clazz) {
        return new TupleBatchIterator<Object, Object>(null, null) {
            @Override protected Tuple2 getRangeTuple(Object from, Object to, int batchSize) {
                return entityManager.getRange(clazz, from, to, batchSize);
            }

            @Override protected Object getKey(Object item) {
                return entityManager.getPK(item);
            }
        };
    }
}
