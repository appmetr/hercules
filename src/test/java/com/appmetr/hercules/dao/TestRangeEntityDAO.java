package com.appmetr.hercules.dao;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.model.TestRangeEntity;

public class TestRangeEntityDAO extends AbstractDAO<TestRangeEntity, Integer> {
    public TestRangeEntityDAO(Hercules hercules) {
        super(TestRangeEntity.class, hercules);
    }
}
