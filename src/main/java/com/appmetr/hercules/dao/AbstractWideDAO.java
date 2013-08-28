package com.appmetr.hercules.dao;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.HerculesProvider;
import com.appmetr.hercules.batch.BatchExecutor;
import com.appmetr.hercules.batch.BatchProcessor;
import com.appmetr.hercules.batch.iterator.WideDAOBatchIterator;
import com.appmetr.hercules.operations.DeleteExecutableOperation;
import com.appmetr.hercules.operations.GetExecutableOperation;
import com.appmetr.hercules.operations.OperationsCollector;
import com.appmetr.hercules.operations.SaveExecutableOperation;
import com.appmetr.hercules.profile.DataOperationsProfile;
import com.appmetr.hercules.wide.SliceDataSpecificator;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public abstract class AbstractWideDAO<E, R, T> {

    private final Class<E> entityClass;
    private HerculesProvider provider;

    public AbstractWideDAO(Class<E> entityClass, final Hercules hercules) {
        this(entityClass, new HerculesProvider() {
            @Override public Hercules getHercules() {
                return hercules;
            }
        });
    }

    public AbstractWideDAO(Class<E> entityClass, HerculesProvider provider) {
        this.entityClass = entityClass;
        this.provider = provider;
    }

    public Hercules getHercules() {
        return provider.getHercules();
    }

    public E get(R rowKey, T topKey) {
        return get(rowKey, topKey, (DataOperationsProfile) null);
    }

    public E get(R rowKey, T topKey, DataOperationsProfile dataOperationsProfile) {
        return getHercules().getWideEntityManager().get(entityClass, rowKey, topKey, dataOperationsProfile);
    }

    public List<E> get(R rowKey) {
        return get(rowKey, (DataOperationsProfile) null);
    }

    public List<E> get(R rowKey, DataOperationsProfile dataOperationsProfile) {
        return getHercules().getWideEntityManager().get(entityClass, rowKey, dataOperationsProfile);
    }

    public List<E> get(R rowKey, T[] columns) {
        return get(rowKey, columns, null);
    }

    public List<E> get(R rowKey, T[] columns, DataOperationsProfile dataOperationsProfile) {
        return getHercules().getWideEntityManager().get(entityClass, rowKey, columns, dataOperationsProfile);
    }

    public List<E> get(R rowKey, Collection<T> columns) {
        return get(rowKey, columns, null);
    }

    public List<E> get(R rowKey, Collection<T> columns, DataOperationsProfile dataOperationsProfile) {
        return getHercules().getWideEntityManager().get(entityClass, rowKey, columns, dataOperationsProfile);
    }

    public List<E> get(R rowKey, T from, T to) {
        return get(rowKey, from, to, null);
    }

    public List<E> get(R rowKey, T from, T to, DataOperationsProfile dataOperationsProfile) {
        return getHercules().getWideEntityManager().get(entityClass, rowKey, from, to, dataOperationsProfile);
    }

    public List<E> get(R rowKey, T from, T to, boolean reverse, Integer count) {
        return get(rowKey, from, to, reverse, count, null);
    }

    public List<E> get(R rowKey, T from, T to, boolean reverse, Integer count, DataOperationsProfile dataOperationsProfile) {
        return getHercules().getWideEntityManager().get(entityClass, rowKey, from, to, reverse, count, dataOperationsProfile);
    }

    public List<E> get(R rowKey, SliceDataSpecificator<T> sliceDataSpecificator) {
        return get(rowKey, sliceDataSpecificator, null);
    }

    public List<E> get(R rowKey, SliceDataSpecificator<T> sliceDataSpecificator, DataOperationsProfile dataOperationsProfile) {
        return getHercules().getWideEntityManager().get(entityClass, rowKey, sliceDataSpecificator, dataOperationsProfile);
    }

    public T getTopKey(E entity) {
        return getHercules().getWideEntityManager().getTopKey(entityClass, entity);
    }

    public void save(R rowKey, E entity) {
        save(rowKey, entity, null);
    }

    public void save(R rowKey, E entity, DataOperationsProfile dataOperationsProfile) {
        save(rowKey, getTopKey(entity), entity, dataOperationsProfile);
    }

    public void save(R rowKey, Iterable<E> entities) {
        save(rowKey, entities, null);
    }

    public void save(R rowKey, Iterable<E> entities, DataOperationsProfile dataOperationsProfile) {
        getHercules().getWideEntityManager().save(entityClass, rowKey, entities, dataOperationsProfile);
    }

    public void save(R rowKey, T topKey, E value) {
        save(rowKey, topKey, value, null);
    }

    public void save(R rowKey, T topKey, E value, DataOperationsProfile dataOperationsProfile) {
        getHercules().getWideEntityManager().save(rowKey, topKey, value, dataOperationsProfile);
    }

    public void delete(R rowKey) {
        delete(rowKey, (DataOperationsProfile) null);
    }

    public void delete(R rowKey, DataOperationsProfile dataOperationsProfile) {
        getHercules().getWideEntityManager().delete(entityClass, rowKey, dataOperationsProfile);
    }

    public void delete(R rowKey, E entity) {
        delete(rowKey, entity, null);
    }

    public void delete(R rowKey, E entity, DataOperationsProfile dataOperationsProfile) {
        getHercules().getWideEntityManager().delete(entityClass, rowKey, entity, dataOperationsProfile);
    }

    public void delete(R rowKey, Iterable<E> entities) {
        delete(rowKey, entities, null);
    }

    public void delete(R rowKey, Iterable<E> entities, DataOperationsProfile dataOperationsProfile) {
        getHercules().getWideEntityManager().delete(entityClass, rowKey, entities, dataOperationsProfile);
    }

    public void deleteByKey(R rowKey, T topKey) {
        deleteByKey(rowKey, topKey, null);
    }

    public void deleteByKey(R rowKey, T topKey, DataOperationsProfile dataOperationsProfile) {
        getHercules().getWideEntityManager().deleteByKey(entityClass, rowKey, topKey, dataOperationsProfile);
    }

    public void deleteByKeys(R rowKey, Iterable<T> topKeys) {
        deleteByKeys(rowKey, topKeys, null);
    }

    public void deleteByKeys(R rowKey, Iterable<T> topKeys, DataOperationsProfile dataOperationsProfile) {
        getHercules().getWideEntityManager().deleteByKeys(entityClass, rowKey, topKeys, dataOperationsProfile);
    }

    public int processAll(R rowKey, BatchProcessor<E> processor) {
        return processRange(rowKey, null, null, Hercules.DEFAULT_BATCH_SIZE, processor);
    }

    public int processAll(R rowKey, int batchSize, BatchProcessor<E> processor) {
        return processRange(rowKey, null, null, batchSize, processor);
    }

    public int processRange(R rowKey, T from, T to, int batchSize, BatchProcessor<E> processor) {
        return new BatchExecutor<E, T>(new WideDAOBatchIterator<E, R, T>(this, rowKey, from, to, batchSize), processor).execute();
    }

    public List<R> getKeyRange(R from, R to, int batchSize) {
        return getKeyRange(from, to, batchSize, null);
    }

    public List<R> getKeyRange(R from, R to, int batchSize, DataOperationsProfile dataOperationsProfile) {
        return getHercules().getWideEntityManager().getKeyRange(entityClass, from, to, batchSize, dataOperationsProfile);
    }

    public List<R> getAllRowKeys() {
        return getAllRowKeys(null);
    }

    public List<R> getAllRowKeys(DataOperationsProfile dataOperationsProfile) {
        return getHercules().getWideEntityManager().getAllRowKeys(entityClass, dataOperationsProfile);
    }

    public OperationsCollector<GetExecutableOperation> getOperationFor(R rowKey, T topKeys) {
        OperationsCollector<GetExecutableOperation> collector = getHercules().getInjector().getInstance(Key.get(new TypeLiteral<OperationsCollector<GetExecutableOperation>>() {
        }));
        collector.add(new GetExecutableOperation<E, R, T>(entityClass, rowKey, Arrays.asList(topKeys)));

        return collector;
    }

    public OperationsCollector<GetExecutableOperation> getOperationFor(R rowKey, T[] topKeys) {
        OperationsCollector<GetExecutableOperation> collector = getHercules().getInjector().getInstance(Key.get(new TypeLiteral<OperationsCollector<GetExecutableOperation>>() {
        }));
        collector.add(new GetExecutableOperation<E, R, T>(entityClass, rowKey, topKeys));

        return collector;
    }

    public OperationsCollector<GetExecutableOperation> getOperationFor(R rowKey, List<T> topKeys) {
        OperationsCollector<GetExecutableOperation> collector = getHercules().getInjector().getInstance(Key.get(new TypeLiteral<OperationsCollector<GetExecutableOperation>>() {
        }));
        collector.add(new GetExecutableOperation<E, R, T>(entityClass, rowKey, topKeys));

        return collector;
    }

    public OperationsCollector<SaveExecutableOperation> saveOperationFor(R rowKey, List<E> entities) {
        OperationsCollector<SaveExecutableOperation> collector = getHercules().getInjector().getInstance(Key.get(new TypeLiteral<OperationsCollector<SaveExecutableOperation>>() {
        }));
        collector.add(new SaveExecutableOperation<E, R, T>(entityClass, rowKey, entities));

        return collector;
    }

    public OperationsCollector<SaveExecutableOperation> saveOperationFor(R rowKey, E entity) {
        OperationsCollector<SaveExecutableOperation> collector = getHercules().getInjector().getInstance(Key.get(new TypeLiteral<OperationsCollector<SaveExecutableOperation>>() {
        }));
        collector.add(new SaveExecutableOperation<E, R, T>(entityClass, rowKey, Arrays.asList(entity)));

        return collector;
    }

    public OperationsCollector<DeleteExecutableOperation> deleteOperationFor(R rowKey, T[] topKeys) {
        OperationsCollector<DeleteExecutableOperation> collector = getHercules().getInjector().getInstance(Key.get(new TypeLiteral<OperationsCollector<DeleteExecutableOperation>>() {
        }));
        collector.add(new DeleteExecutableOperation<E, R, T>(entityClass, rowKey, topKeys));

        return collector;
    }

    public OperationsCollector<DeleteExecutableOperation> deleteOperationFor(R rowKey, List<E> entities) {
        OperationsCollector<DeleteExecutableOperation> collector = getHercules().getInjector().getInstance(Key.get(new TypeLiteral<OperationsCollector<DeleteExecutableOperation>>() {
        }));
        collector.add(new DeleteExecutableOperation<E, R, T>(entityClass, rowKey, entities));

        return collector;
    }

    public OperationsCollector<DeleteExecutableOperation> deleteOperationFor(R rowKey) {
        OperationsCollector<DeleteExecutableOperation> collector = getHercules().getInjector().getInstance(Key.get(new TypeLiteral<OperationsCollector<DeleteExecutableOperation>>() {
        }));
        collector.add(new DeleteExecutableOperation<E, R, T>(entityClass, rowKey));

        return collector;
    }

}
