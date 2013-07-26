package com.appmetr.hercules.dao;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.HerculesProvider;
import com.appmetr.hercules.batch.BatchExecutor;
import com.appmetr.hercules.batch.BatchProcessor;
import com.appmetr.hercules.batch.extractor.WideDAOBatchIterator;
import com.appmetr.hercules.operations.DeleteExecutableOperation;
import com.appmetr.hercules.operations.GetExecutableOperation;
import com.appmetr.hercules.operations.OperationsCollector;
import com.appmetr.hercules.operations.SaveExecutableOperation;
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
        return getHercules().getWideEntityManager().get(entityClass, rowKey, topKey);
    }

    public List<E> get(R rowKey) {
        return getHercules().getWideEntityManager().get(entityClass, rowKey);
    }

    public List<E> get(R rowKey, T[] columns) {
        return getHercules().getWideEntityManager().get(entityClass, rowKey, columns);
    }

    public List<E> get(R rowKey, Collection<T> columns) {
        return getHercules().getWideEntityManager().get(entityClass, rowKey, columns);
    }

    public List<E> get(R rowKey, T from, T to) {
        return getHercules().getWideEntityManager().get(entityClass, rowKey, from, to);
    }

    public List<E> get(R rowKey, T from, T to, boolean reverse, Integer count) {
        return getHercules().getWideEntityManager().get(entityClass, rowKey, from, to, reverse, count);
    }

    public List<E> get(R rowKey, SliceDataSpecificator<T> sliceDataSpecificator) {
        return getHercules().getWideEntityManager().get(entityClass, rowKey, sliceDataSpecificator);
    }

    public T getTopKey(E entity) {
        return getHercules().getWideEntityManager().getTopKey(entityClass, entity);
    }

    public void save(R rowKey, E entity) {
        save(rowKey, getTopKey(entity), entity);
    }

    public void save(R rowKey, Iterable<E> entities) {
        getHercules().getWideEntityManager().save(entityClass, rowKey, entities);
    }

    public void save(R rowKey, T topKey, E value) {
        getHercules().getWideEntityManager().save(rowKey, topKey, value);
    }

    public void delete(R rowKey) {
        getHercules().getWideEntityManager().delete(entityClass, rowKey);
    }

    public void delete(R rowKey, E entity) {
        getHercules().getWideEntityManager().delete(entityClass, rowKey, entity);
    }

    public void delete(R rowKey, Iterable<E> entities) {
        getHercules().getWideEntityManager().delete(entityClass, rowKey, entities);
    }

    public void deleteByKey(R rowKey, T topKey) {
        getHercules().getWideEntityManager().deleteByKey(entityClass, rowKey, topKey);
    }

    public void deleteByKeys(R rowKey, Iterable<T> topKeys) {
        getHercules().getWideEntityManager().deleteByKeys(entityClass, rowKey, topKeys);
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
        return getHercules().getWideEntityManager().getKeyRange(entityClass, from, to, batchSize);
    }

    public List<R> getAllRowKeys() {
        return getHercules().getWideEntityManager().getAllRowKeys(entityClass);
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
