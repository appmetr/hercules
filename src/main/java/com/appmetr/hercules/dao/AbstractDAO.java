package com.appmetr.hercules.dao;

import com.appmetr.hercules.FieldFilter;
import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.HerculesProvider;
import com.appmetr.hercules.batch.BatchExecutor;
import com.appmetr.hercules.batch.BatchProcessor;
import com.appmetr.hercules.batch.iterator.DAOBatchIterator;
import com.appmetr.hercules.batch.iterator.ImmutableKeyBatchIterator;
import com.appmetr.hercules.keys.ForeignKey;
import com.appmetr.hercules.profile.DataOperationsProfile;
import com.appmetr.hercules.utils.Tuple2;

import java.util.List;
import java.util.Set;

public abstract class AbstractDAO<E, K> {

    private final Class<E> entityClass;
    private HerculesProvider provider;

    public AbstractDAO(Class<E> entityClass, final Hercules hercules) {
        this(entityClass, () -> hercules);
    }

    public AbstractDAO(Class<E> entityClass, HerculesProvider provider) {
        this.entityClass = entityClass;
        this.provider = provider;
    }

    public Hercules getHercules() {
        return provider.getHercules();
    }

    public K getPK(E entity) {
        return getHercules().getEntityManager().getPK(entity);
    }

    public E get(K key) {
        return get(key, null);
    }

    public E get(K key, DataOperationsProfile dataOperationsProfile) {
        return getHercules().getEntityManager().get(entityClass, key, dataOperationsProfile);
    }

    public List<E> get(Iterable<K> keys) {
        return get(keys, null);
    }

    public List<E> get(Iterable<K> keys, DataOperationsProfile dataOperationsProfile) {
        return getHercules().getEntityManager().get(entityClass, keys, dataOperationsProfile);
    }

    public List<E> getAll() {
        return getAll(null);
    }

    public List<E> getAll(DataOperationsProfile dataOperationsProfile) {
        return getHercules().getEntityManager().getAll(entityClass, dataOperationsProfile);
    }

    public List<E> getRange(K from, K to, Integer count) {
        return getRange(from, to, count, null);
    }

    public List<E> getRange(K from, K to, Integer count, DataOperationsProfile dataOperationsProfile) {
        return getHercules().getEntityManager().getRange(entityClass, from, to, count, dataOperationsProfile).e1;
    }

    public Tuple2<List<E>, K> getRangeWithLastKey(K from, K to, Integer count) {
        return getRangeWithLastKey(from, to, count, null);
    }

    public Tuple2<List<E>, K> getRangeWithLastKey(K from, K to, Integer count, DataOperationsProfile dataOperationsProfile) {
        return getHercules().getEntityManager().getRange(entityClass, from, to, count, dataOperationsProfile);
    }

    public List<E> getByFK(ForeignKey foreignKey) {
        return getByFK(foreignKey, null);
    }

    public List<E> getByFK(ForeignKey foreignKey, DataOperationsProfile dataOperationsProfile) {
        return getHercules().getEntityManager().getByFK(entityClass, foreignKey, dataOperationsProfile);
    }

    public List<E> getByFK(ForeignKey foreignKey, boolean reverse, Integer count) {
        return getHercules().getEntityManager().getByFK(entityClass, foreignKey, reverse, count, null);
    }

    public List<E> getByFK(ForeignKey foreignKey, boolean reverse, Integer count, DataOperationsProfile dataOperationsProfile) {
        return getHercules().getEntityManager().getByFK(entityClass, foreignKey, reverse, count, dataOperationsProfile);
    }

    public List<E> getByFK(ForeignKey foreignKey, Set<K> skipKeys, DataOperationsProfile dataOperationsProfile) {
        return getHercules().getEntityManager().getByFK(entityClass, foreignKey, skipKeys, dataOperationsProfile);
    }

    public E getSingleByFK(ForeignKey foreignKey) {
        return getSingleByFK(foreignKey, null);
    }

    public E getSingleByFK(ForeignKey foreignKey, DataOperationsProfile dataOperationsProfile) {
        return getHercules().getEntityManager().getSingleByFK(entityClass, foreignKey, dataOperationsProfile);
    }

    public <U> List<E> getByCollectionIndex(String indexedFieldName, U indexValue) {
        return getByCollectionIndex(indexedFieldName, indexValue, null);
    }

    public <U> List<E> getByCollectionIndex(String indexedFieldName, U indexValue, DataOperationsProfile operationsProfile) {
        return getHercules().getEntityManager().getByCollectionIndex(entityClass, indexedFieldName, indexValue, operationsProfile);
    }

    public <U> E getSingleByCollectionIndex(String indexedFieldName, U indexValue) {
        return getSingleByCollectionIndex(indexedFieldName, indexValue, null);
    }

    public <U> E getSingleByCollectionIndex(String indexedFieldName, U indexValue, DataOperationsProfile operationsProfile) {
        return getHercules().getEntityManager().getSingleByCollectionIndex(entityClass, indexedFieldName, indexValue, operationsProfile);
    }

    public int getCountByFK(ForeignKey foreignKey) {
        return getCountByFK(foreignKey, null);
    }

    public int getCountByFK(ForeignKey foreignKey, DataOperationsProfile dataOperationsProfile) {
        return getHercules().getEntityManager().getCountByFK(entityClass, foreignKey, dataOperationsProfile);
    }

    public void save(K key, E entity) {
        save(key, entity, (DataOperationsProfile) null);
    }

    public void save(K key, E entity, int ttl) {
        save(key, entity, ttl, null);
    }

    public void save(K key, E entity, DataOperationsProfile dataOperationsProfile) {
        getHercules().getEntityManager().save(key, entity, null, dataOperationsProfile);
    }

    public void save(K key, E entity, int ttl, DataOperationsProfile dataOperationsProfile) {
        getHercules().getEntityManager().save(key, entity, ttl, dataOperationsProfile);
    }

    public void save(K key, E entity, FieldFilter fieldFilter) {
        save(key, entity, fieldFilter, null);
    }

    public void save(K key, E entity, FieldFilter fieldFilter, DataOperationsProfile dataOperationsProfile) {
        getHercules().getEntityManager().save(key, entity, fieldFilter, dataOperationsProfile);
    }

    public void save(E entity) {
        save(entity, (DataOperationsProfile) null);
    }

    public void save(E entity, int ttl) {
        save(entity, ttl, null);
    }

    public void save(E entity, DataOperationsProfile dataOperationsProfile) {
        getHercules().getEntityManager().save(entity, null, dataOperationsProfile);
    }

    public void save(E entity, int ttl, DataOperationsProfile dataOperationsProfile) {
        getHercules().getEntityManager().save(entity, ttl, dataOperationsProfile);
    }

    public void save(E entity, FieldFilter fieldFilter) {
        save(entity, fieldFilter, null);
    }

    public void save(E entity, FieldFilter fieldFilter, DataOperationsProfile dataOperationsProfile) {
        getHercules().getEntityManager().save(entity, fieldFilter, dataOperationsProfile);
    }

    public void save(Iterable<E> entities) {
        save(entities, null);
    }

    public void save(Iterable<E> entities, int ttl) {
        save(entities, ttl, null);
    }

    public void save(Iterable<E> entities, DataOperationsProfile dataOperationsProfile) {
        for (E entity : entities) save(entity, dataOperationsProfile);
    }

    public void save(Iterable<E> entities, int ttl, DataOperationsProfile dataOperationsProfile) {
        for (E entity : entities) save(entity, ttl, dataOperationsProfile);
    }

    public void delete(E entity) {
        delete(entity, null);
    }

    public void delete(E entity, DataOperationsProfile dataOperationsProfile) {
        getHercules().getEntityManager().delete(entity, dataOperationsProfile);
    }

    public void delete(List<E> entities) {
        delete(entities, null);
    }

    public void delete(List<E> entities, DataOperationsProfile dataOperationsProfile) {
        for (E entity : entities) {
            delete(entity, dataOperationsProfile);
        }
    }

    public void deleteByKey(K key) {
        deleteByKey(key, null);
    }

    public void deleteByKey(K key, DataOperationsProfile dataOperationsProfile) {
        getHercules().getEntityManager().delete(entityClass, key, dataOperationsProfile);
    }

    public void deleteByKeys(List<K> keys) {
        deleteByKeys(keys, null);
    }

    public void deleteByKeys(List<K> keys, DataOperationsProfile dataOperationsProfile) {
        for (K key : keys) {
            deleteByKey(key, dataOperationsProfile);
        }
    }

    public int processAll(BatchProcessor<E> processor) {
        return processAll(processor, null);
    }

    public int processAll(BatchProcessor<E> processor, DataOperationsProfile dataOperationsProfile) {
        return processRange(null, null, Hercules.DEFAULT_BATCH_SIZE, processor, dataOperationsProfile);
    }

    public int processAll(Integer batchSize, BatchProcessor<E> processor) {
        return processAll(batchSize, processor, null);
    }

    public int processAll(Integer batchSize, BatchProcessor<E> processor, DataOperationsProfile dataOperationsProfile) {
        return processRange(null, null, batchSize, processor, dataOperationsProfile);
    }

    public int processRange(K from, K to, Integer batchSize, BatchProcessor<E> processor) {
        return processRange(from, to, batchSize, processor, null);
    }

    public int processRange(K from, K to, Integer batchSize, BatchProcessor<E> processor, DataOperationsProfile dataOperationsProfile) {
        return new BatchExecutor<>(new DAOBatchIterator<>(this, from, to, batchSize), processor).execute(dataOperationsProfile);
    }

    public int processAllKeys(BatchProcessor<K> processor) {
        return processAllKeys(processor, null);
    }

    public int processAllKeys(BatchProcessor<K> processor, DataOperationsProfile dataOperationsProfile) {
        return processKeyRange(null, null, Hercules.DEFAULT_BATCH_SIZE, processor, dataOperationsProfile);
    }

    public int processAllKeys(Integer batchSize, BatchProcessor<K> processor) {
        return processAllKeys(batchSize, processor, null);
    }

    public int processAllKeys(Integer batchSize, BatchProcessor<K> processor, DataOperationsProfile dataOperationsProfile) {
        return processKeyRange(null, null, batchSize, processor, dataOperationsProfile);
    }

    public int processKeyRange(K from, K to, Integer batchSize, BatchProcessor<K> processor) {
        return processKeyRange(from, to, batchSize, processor, null);
    }

    public int processKeyRange(K from, K to, Integer batchSize, BatchProcessor<K> processor, DataOperationsProfile dataOperationsProfile) {
        return new BatchExecutor<>(new ImmutableKeyBatchIterator<K>(from, to, batchSize) {
            @Override
            public List<K> getRange(K from, K to, int batchSize, DataOperationsProfile dataOperationsProfile) {
                return getHercules().getEntityManager().getKeyRange(entityClass, from, to, batchSize, dataOperationsProfile);
            }
        }, processor).execute(dataOperationsProfile);
    }
}
