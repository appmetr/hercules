package com.appmetr.hercules;

import com.appmetr.hercules.model.TestEntity;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.HashSet;
import java.util.Set;

public abstract class TestHercules {

    protected static Hercules hercules;

    @BeforeClass
    public static void init() throws Exception {
        Set<Class> classes = new HashSet<Class>();

        //Entity
        classes.add(TestEntity.class);

        HerculesConfig config = new HerculesConfig(
                "Test",
                "localhost:9160",
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
