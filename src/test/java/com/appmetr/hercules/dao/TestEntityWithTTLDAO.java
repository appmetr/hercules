package com.appmetr.hercules.dao;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.model.TestEntityWithTtl;

public class TestEntityWithTtlDAO extends AbstractDAO<TestEntityWithTtl, String> {
    public TestEntityWithTtlDAO(Hercules hercules) {
        super(TestEntityWithTtl.class, hercules);
    }
}
