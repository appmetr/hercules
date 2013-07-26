package com.appmetr.hercules.dao;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.model.TestEntity;

public class TestEntityDAO extends AbstractDAO<TestEntity, String> {
    public TestEntityDAO(Hercules hercules) {
        super(TestEntity.class, hercules);
    }
}
