package com.appmetr.hercules.dao;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.model.TestWideEntity;

public class TestWideEntityDAO extends AbstractWideDAO<TestWideEntity, String, String> {
    private Hercules hercules;

    public TestWideEntityDAO(Hercules hercules) {
        super(TestWideEntity.class);

        this.hercules = hercules;
    }

    @Override public Hercules getHercules() {
        return hercules;
    }
}
