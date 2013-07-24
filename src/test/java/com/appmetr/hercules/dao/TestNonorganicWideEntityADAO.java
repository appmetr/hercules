package com.appmetr.hercules.dao;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.model.nonorganic.TestNonorganicWideEntityA;

public class TestNonorganicWideEntityADAO extends AbstractWideDAO<TestNonorganicWideEntityA, String, String> {
    private Hercules hercules;

    public TestNonorganicWideEntityADAO(Hercules hercules) {
        super(TestNonorganicWideEntityA.class);

        this.hercules = hercules;
    }

    @Override public Hercules getHercules() {
        return hercules;
    }
}
