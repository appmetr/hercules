package com.appmetr.hercules;

import com.appmetr.hercules.model.*;
import com.appmetr.hercules.model.nonorganic.TestNonorganicWideEntityA;
import com.appmetr.hercules.model.nonorganic.TestNonorganicWideEntityB;

import java.util.HashSet;
import java.util.Set;

public class TestMain {
    public static void main(String[] args) throws InterruptedException {
        Set<Class> entityClasses = new HashSet<Class>();
        entityClasses.add(TestEntity.class);
        entityClasses.add(ParentEntity.class);
        entityClasses.add(TestRangeEntity.class);

        Set<Class> wideEntityClasses = new HashSet<Class>();
        wideEntityClasses.add(TestNonorganicWideEntityA.class);
        wideEntityClasses.add(TestNonorganicWideEntityB.class);
        wideEntityClasses.add(TestPartitionedEntity.class);
        wideEntityClasses.add(TestWideEntity.class);

        HerculesConfig config = new HerculesConfig(
                "Test",
                "localhost:9160",
                1,
                true,
                entityClasses,
                wideEntityClasses
        );

        Hercules hercules = HerculesFactory.create(config);
        hercules.init();

        Thread.sleep(20000);

        hercules.shutdown();
    }
}
