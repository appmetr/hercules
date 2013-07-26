package com.appmetr.hercules.dao;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.column.TestDatedColumn;
import com.appmetr.hercules.model.TestPartitionedEntity;

public class TestPartitionedEntityDAO extends AbstractWideDAO<TestPartitionedEntity, String, TestDatedColumn> {
    public TestPartitionedEntityDAO(Hercules hercules) {
        super(TestPartitionedEntity.class, hercules);
    }
}
