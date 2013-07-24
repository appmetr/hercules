package com.appmetr.hercules.dao;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.model.nonorganic.TestNonorganicWideEntityB;

public class TestNonorganicWideEntityBDAO extends AbstractWideDAO<TestNonorganicWideEntityB, String, String> {
    private Hercules hercules;

    public TestNonorganicWideEntityBDAO(Hercules hercules) {
        super(TestNonorganicWideEntityB.class);

        this.hercules = hercules;
    }

    @Override public Hercules getHercules() {
        return hercules;
    }
}
