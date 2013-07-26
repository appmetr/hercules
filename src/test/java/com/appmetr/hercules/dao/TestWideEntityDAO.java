package com.appmetr.hercules.dao;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.model.TestWideEntity;

public class TestWideEntityDAO extends AbstractWideDAO<TestWideEntity, String, String> {
    public TestWideEntityDAO(Hercules hercules) {
        super(TestWideEntity.class, hercules);
    }
}
