package com.appmetr.hercules.dao;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.column.TestDatedColumn;
import com.appmetr.hercules.model.TestPartitionedEntity;

public class TestPartitionedEntityDAO extends AbstractWideDAO<TestPartitionedEntity, String, TestDatedColumn> {
    private Hercules hercules;

    public TestPartitionedEntityDAO(Hercules hercules) {
        super(TestPartitionedEntity.class);

        this.hercules = hercules;
    }

    @Override public Hercules getHercules() {
        return hercules;
    }
}
