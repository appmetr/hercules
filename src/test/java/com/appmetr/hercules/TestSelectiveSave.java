package com.appmetr.hercules;

import com.appmetr.hercules.dao.TestEntityDAO;
import com.appmetr.hercules.model.TestEntity;

import java.lang.reflect.Field;

public class TestSelectiveSave extends TestHercules {

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
        dao.save(entity, new FieldFilter() {
            @Override public boolean accept(Field field) {
                return "stringValue".equals(field.getName());
            }
        });

        //reload
        entity = dao.get(entity.id);

        //check
        assert "World".equals(entity.stringValue);
        assert target.equals(entity.longValue);
    }

    protected void tearDown() throws Exception {
        hercules.shutdown();
    }
}
