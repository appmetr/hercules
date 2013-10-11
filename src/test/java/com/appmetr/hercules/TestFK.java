package com.appmetr.hercules;

import com.appmetr.hercules.dao.TestEntityDAO;
import com.appmetr.hercules.keys.ParentFK;
import com.appmetr.hercules.model.TestEntity;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestFK extends TestHercules {

    @Test
    public void testSimple() throws Exception {

        TestEntityDAO dao = new TestEntityDAO(hercules);

        List<TestEntity> entities = dao.getByFK(new ParentFK("magic"));

        TestEntity entity = new TestEntity();
        entity.id = "TEST";
        entity.stringValue = "Hello";
        entity.longValue = 16L;
        entity.parent = "magic";

        dao.save(entity);

        entities = dao.getByFK(new ParentFK("magic"));

        Assert.assertEquals(entities.size(), 1);
        Assert.assertEquals(entities.get(0), entity);

    }
}
