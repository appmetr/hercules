package com.appmetr.hercules;

import com.appmetr.hercules.model.*;
import com.appmetr.hercules.model.nonorganic.TestNonorganicWideEntityA;
import com.appmetr.hercules.model.nonorganic.TestNonorganicWideEntityB;

import java.util.HashSet;
import java.util.Set;

public class TestMain {
    public static void main(String[] args) throws InterruptedException {
        Set<Class> classes = new HashSet<Class>();

        //Entity
        classes.add(TestEntity.class);
        classes.add(ParentEntity.class);
        classes.add(TestRangeEntity.class);

        //WideEntity
        classes.add(TestNonorganicWideEntityA.class);
        classes.add(TestNonorganicWideEntityB.class);
        classes.add(TestPartitionedEntity.class);
        classes.add(TestWideEntity.class);

        HerculesConfig config = new HerculesConfig(
                "Test Cluster",
                "Test",
                "localhost:9160",
                100,
                1,
                true,
                classes
        );

        Hercules hercules = HerculesFactory.create(config);
        hercules.init();

        Thread.sleep(20000);

        hercules.shutdown();
    }
}
