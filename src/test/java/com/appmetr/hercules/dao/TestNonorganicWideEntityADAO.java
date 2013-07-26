package com.appmetr.hercules.dao;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.model.nonorganic.TestNonorganicWideEntityA;

public class TestNonorganicWideEntityADAO extends AbstractWideDAO<TestNonorganicWideEntityA, String, String> {
    public TestNonorganicWideEntityADAO(Hercules hercules) {
        super(TestNonorganicWideEntityA.class, hercules);
    }
}
