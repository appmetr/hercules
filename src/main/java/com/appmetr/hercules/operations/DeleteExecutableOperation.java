package com.appmetr.hercules.operations;

import java.util.List;

public class DeleteExecutableOperation<E, R, T> extends ExecutableOperation<E, R, T> {
    public DeleteExecutableOperation(Class<E> clazz, R rowKey, T[] topKeys) {
        super(clazz, rowKey, topKeys, null);
    }

    public DeleteExecutableOperation(Class<E> clazz, R rowKey, List<E> entities) {
        super(clazz, rowKey, (T[]) null, entities);
    }
    public DeleteExecutableOperation(Class<E> clazz, R rowKey) {
        super(clazz, rowKey, (T[]) null, null);
    }
}
