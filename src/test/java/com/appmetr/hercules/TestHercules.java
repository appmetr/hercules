package com.appmetr.hercules;

import com.appmetr.hercules.model.TestEntity;
import junit.framework.TestCase;

import java.util.HashSet;
import java.util.Set;

public abstract class TestHercules extends TestCase {

    protected Hercules hercules;

    @Override protected void setUp() throws Exception {
        Set<Class> classes = new HashSet<Class>();

        //Entity
        classes.add(TestEntity.class);

        HerculesConfig config = new HerculesConfig(
                "Test",
                "localhost:9160",
                1,
                true,
                classes
        );

        hercules = HerculesFactory.create(config);
        hercules.init();
    }

}
