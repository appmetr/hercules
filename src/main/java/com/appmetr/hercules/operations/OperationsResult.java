package com.appmetr.hercules.operations;

import java.util.List;

public class OperationsResult<E> {

    private List<E> entities;

    public OperationsResult() {
        this(null);
    }

    public OperationsResult(List<E> entities) {
        this.entities = entities;
    }

    public List<E> getEntities() {
        return entities;
    }

    @Override public String toString() {
        return "OperationsResult{" +
                "entities=" + entities +
                '}';
    }
}
