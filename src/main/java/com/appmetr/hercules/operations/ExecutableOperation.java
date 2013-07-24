package com.appmetr.hercules.operations;

import java.util.Arrays;
import java.util.List;

public abstract class ExecutableOperation<E, R, T> {
    private Class<E> clazz;
    private R rowKey;
    private List<T> topKeys;
    private List<E> entities;

    public ExecutableOperation(Class<E> clazz, R rowKey, List<T> topKeys, List<E> entities) {
        this.clazz = clazz;
        this.rowKey = rowKey;
        this.topKeys = topKeys;
        this.entities = entities;
    }

    public ExecutableOperation(Class<E> clazz, R rowKey, T[] topKeys, List<E> entities) {
        this.clazz = clazz;
        this.rowKey = rowKey;
        this.topKeys = topKeys != null ? Arrays.asList(topKeys) : null;
        this.entities = entities;
    }

    public Class<E> getClazz() {
        return clazz;
    }

    public R getRowKey() {
        return rowKey;
    }

    public List<T> getTopKeys() {
        return topKeys;
    }

    public List<E> getEntities() {
        return entities;
    }
}
