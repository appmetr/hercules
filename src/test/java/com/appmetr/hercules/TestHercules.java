package com.appmetr.hercules;

import com.appmetr.hercules.model.EntityWithCollection;
import com.appmetr.hercules.model.TestEntity;
import com.appmetr.hercules.model.TestEntityWithTTL;
import com.appmetr.hercules.model.TestWideEntity;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.HashSet;
import java.util.Set;

public abstract class TestHercules {

    protected static Hercules hercules;

    @BeforeClass
    public static void init() throws Exception {
        Set<Class> classes = new HashSet<>();

        //Entity
        classes.add(TestEntity.class);
        classes.add(EntityWithCollection.class);
        classes.add(TestEntityWithTTL.class);
        classes.add(TestWideEntity.class);

        HerculesConfig config = new HerculesConfig(
                "Test Cluster",
                "Test",
                "localhost",
                100,
                1,
                true,
                classes
        );

        hercules = HerculesFactory.create(config);
        hercules.init();
    }

    @AfterClass
    public static void shutdown() throws Exception {
        hercules.shutdown();
    }
}
