package com.appmetr.hercules.dao;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.model.TestEntity;
import com.google.inject.Inject;

public class TestEntityDAO extends AbstractDAO<TestEntity, String> {
    @Inject
    public TestEntityDAO(Hercules hercules) {
        super(TestEntity.class, hercules);
    }
}
