package com.appmetr.hercules;

import com.appmetr.hercules.dao.TestEntityDAO;
import com.appmetr.hercules.dao.TestEntityWithTtlDAO;
import com.appmetr.hercules.dao.TestWideEntityDAO;
import com.appmetr.hercules.model.TestEntity;
import com.appmetr.hercules.model.TestEntityWithTtl;
import com.appmetr.hercules.model.TestWideEntity;
import com.appmetr.hercules.operations.OperationsCollector;
import com.appmetr.hercules.operations.SaveExecutableOperation;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class TestTtl extends TestHercules {

    public static final int SECOND_TTL = 1;

    // ids
    public static final String ONE_SECOND = "one_second";
    public static final String FOREVER = "forever";
    public static final String WITH_ANNOTATION = "with_annotation";
    public static final String PARAMETER_PREFERABLE = "parameter_preferable";
    public static final String WIDE_ROW_KEY = "WIDE_ROW_KEY";
    public static final String WIDE_ROW_KEY2 = "WIDE_ROW_KEY2";

    @Test
    public void testPlainEntity() throws Exception {
        TestEntity liveForever = new TestEntity();
        TestEntityDAO dao = new TestEntityDAO(hercules);
        liveForever.id = FOREVER;
        dao.save(liveForever);

        liveForever.id = ONE_SECOND;
        dao.save(liveForever, SECOND_TTL);
        Thread.sleep(SECOND_TTL * 1000);
        TestEntity entity = dao.get(FOREVER);
        assertNotNull(entity);
        entity = dao.get(ONE_SECOND);
        assertNull(entity);
    }

    @Test(expected = RuntimeException.class)
    public void testException() {
        TestEntity liveForever = new TestEntity();
        TestEntityDAO dao = new TestEntityDAO(hercules);
        liveForever.id = ONE_SECOND;
        dao.save(liveForever, -1);
    }

    @Test
    public void testWithAnnotation() throws Exception {
        TestEntityWithTtl withTtl = new TestEntityWithTtl(WITH_ANNOTATION);
        TestEntityWithTtlDAO dao = new TestEntityWithTtlDAO(hercules);
        dao.save(withTtl);
        withTtl.id = PARAMETER_PREFERABLE;
        dao.save(withTtl, SECOND_TTL);
        Thread.sleep(SECOND_TTL * 1000);

        assertNull(dao.get(PARAMETER_PREFERABLE));
        assertNotNull(dao.get(WITH_ANNOTATION));
        Thread.sleep(SECOND_TTL * 1000);
        assertNull(dao.get(WITH_ANNOTATION));
    }

    @Test
    public void testWideEntity() throws Exception {
        TestWideEntity entity = new TestWideEntity();
        TestWideEntityDAO dao = new TestWideEntityDAO(hercules);
        entity.topKey = FOREVER;
        dao.save(WIDE_ROW_KEY, entity);
        entity.topKey = ONE_SECOND;
        dao.save(WIDE_ROW_KEY, entity, SECOND_TTL);
        Thread.sleep(SECOND_TTL * 1000);

        List<TestWideEntity> entities = dao.get(WIDE_ROW_KEY);
        assertEquals(1, entities.size());
        assertEquals(FOREVER, entities.iterator().next().topKey);
    }

    @Test
    public void testExecutableOperations() throws Exception {
        TestWideEntity liveForever = new TestWideEntity();
        TestWideEntityDAO dao = new TestWideEntityDAO(hercules);
        liveForever.topKey = FOREVER;
        OperationsCollector<SaveExecutableOperation> collector = dao.saveOperationFor(WIDE_ROW_KEY2, liveForever);
        TestWideEntity secondEntity = new TestWideEntity();
        secondEntity.topKey = ONE_SECOND;
        collector.add(dao.saveOperationFor(WIDE_ROW_KEY2, secondEntity, SECOND_TTL));
        collector.execute();
        Thread.sleep(SECOND_TTL * 1000);
        List<TestWideEntity> entities = dao.get(WIDE_ROW_KEY2);
        assertEquals(1, entities.size());
        assertEquals(FOREVER, entities.iterator().next().topKey);
    }
}
