package com.appmetr.hercules.dao;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.model.TestEntity;

public class TestEntityDAO extends AbstractDAO<TestEntity, String> {
    private Hercules hercules;

    public TestEntityDAO(Hercules hercules) {
        super(TestEntity.class);

        this.hercules = hercules;
    }

    @Override public Hercules getHercules() {
        return hercules;
    }
}
