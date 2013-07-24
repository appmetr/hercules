package com.appmetr.hercules.operations;

import com.appmetr.hercules.manager.WideEntityManager;
import com.google.inject.Inject;

import java.util.ArrayList;
import java.util.List;

public class OperationsCollector<T extends ExecutableOperation> {
    @Inject WideEntityManager entityManager;

    public enum Type {
        GET, SAVE, DELETE
    }

    private List<T> operations = new ArrayList<T>();

    public void add(T operation) {
        operations.add(operation);
    }

    public void add(OperationsCollector<T> collector) {
        operations.addAll(collector.getOperations());
    }

    public OperationsResult execute() {
        return entityManager.executeOperations(this);
    }

    public List<T> getOperations() {
        return operations;
    }

    public Type getOperationType(ExecutableOperation operation) {
        if (operation instanceof GetExecutableOperation) {
            return Type.GET;
        } else if (operation instanceof SaveExecutableOperation) {
            return Type.SAVE;
        } else if (operation instanceof DeleteExecutableOperation) {
            return Type.DELETE;
        } else {
            throw new RuntimeException("Unknown operation type");
        }
    }
}
