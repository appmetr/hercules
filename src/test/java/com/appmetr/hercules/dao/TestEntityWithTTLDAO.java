package com.appmetr.hercules.dao;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.model.TestEntityWithTTL;

public class TestEntityWithTTLDAO extends AbstractDAO<TestEntityWithTTL, String> {
    public TestEntityWithTTLDAO(Hercules hercules) {
        super(TestEntityWithTTL.class, hercules);
    }
}
