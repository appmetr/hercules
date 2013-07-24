package com.appmetr.hercules.operations;

import java.util.List;

public class GetExecutableOperation<E, R, T> extends ExecutableOperation<E, R, T> {
    public GetExecutableOperation(Class<E> clazz, R rowKey, T[] topKeys) {
        super(clazz, rowKey, topKeys, null);
    }

    public GetExecutableOperation(Class<E> clazz, R rowKey, List<T> topKeys) {
        super(clazz, rowKey, topKeys, null);
    }
}
