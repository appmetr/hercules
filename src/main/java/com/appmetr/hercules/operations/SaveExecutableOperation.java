package com.appmetr.hercules.operations;

import java.util.List;

public class SaveExecutableOperation<E, R, T> extends ExecutableOperation<E, R, T> {
    private Integer ttl;

    public SaveExecutableOperation(Class<E> clazz, R rowKey, List<E> entities) {
        super(clazz, rowKey, (T[]) null, entities);
    }

    public SaveExecutableOperation(Class<E> clazz, R rowKey, List<E> entities, int ttl) {
        super(clazz, rowKey, (T[]) null, entities);
        this.ttl = ttl;
    }

    public Integer getTTL() {
        return ttl;
    }
}
