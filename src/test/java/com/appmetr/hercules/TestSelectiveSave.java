package com.appmetr.hercules;

import com.appmetr.hercules.dao.TestEntityDAO;
import com.appmetr.hercules.model.TestEntity;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

public class TestSelectiveSave extends TestHercules {

    @Test
    public void testSimple() throws Exception {

        TestEntity entity = new TestEntity();
        TestEntityDAO dao = new TestEntityDAO(hercules);

        Long target = 32L;

        entity.id = "TEST";
        entity.stringValue = "Hello";
        entity.longValue = target;

        //full save
        dao.save(entity);

        entity.stringValue = "World";
        entity.longValue = 64L;

        //selective
        dao.save(entity, field -> "stringValue".equals(field.getName()));

        //reload
        entity = dao.get(entity.id);

        //check
        Assert.assertEquals("World", entity.stringValue);
        Assert.assertEquals(target, entity.longValue);
    }
}
