package com.appmetr.hercules;

import com.appmetr.hercules.column.TestDatedColumn;
import com.appmetr.hercules.dao.EntityWithCollectionDAO;
import com.appmetr.hercules.dao.TestEntityDAO;
import com.appmetr.hercules.keys.ParentFK;
import com.appmetr.hercules.metadata.CollectionIndexMetadata;
import com.appmetr.hercules.metadata.EntityMetadata;
import com.appmetr.hercules.model.EntityWithCollection;
import com.appmetr.hercules.model.TestEntity;
import com.appmetr.hercules.serializers.ParentFKSerializer;
import com.appmetr.hercules.serializers.TestDatedColumnSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Serializer;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestCollectionIndexes extends TestHercules {


    @Test
    public void testMetadataParsing() throws Exception {

        //in fact at this point metadata is already extracted once during hercules init
        EntityMetadata metadata = hercules.metadataExtractor.extract(EntityWithCollection.class);

        Map<String, CollectionIndexMetadata> collIndexes = metadata.getCollectionIndexes();

        assertEquals(4, collIndexes.size());
        EntityWithCollection testEntity = new EntityWithCollection();
        Map<String, Object> keys = new HashMap<String, Object>();
        Map<String, Class<? extends Serializer>> serializers = new HashMap<String, Class<? extends Serializer>>();

        TestEntity entityKey = new TestEntity();
        entityKey.id = "entityKey!11!";
        testEntity.getEntities().add(entityKey);
        keys.put("entities", entityKey.id);
        serializers.put("entities", StringSerializer.class);

        ParentFK parentFk = new ParentFK("parentFKKey");
        testEntity.getSerializableKeys().add(parentFk);
        keys.put("serializableKeys", parentFk);
        serializers.put("serializableKeys", ParentFKSerializer.class);

        TestDatedColumn tdc = new TestDatedColumn(System.currentTimeMillis());
        testEntity.getCollectionWithExplicitSerializer().add(tdc);
        keys.put("collectionWithExplicitSerializer", tdc);
        serializers.put("collectionWithExplicitSerializer", TestDatedColumnSerializer.class);

        String keyInJson = "keyInjson2121";
        testEntity.setJsonCollection("{" + keyInJson + "}");
        keys.put("jsonCollection", keyInJson);
        serializers.put("jsonCollection", StringSerializer.class);


        for (Map.Entry<String, CollectionIndexMetadata> indexMetadata : collIndexes.entrySet()) {
            CollectionIndexMetadata m = indexMetadata.getValue();
            assertEquals(m.getIndexedField().getName(), indexMetadata.getKey());
            assertEquals(serializers.get(indexMetadata.getKey()), m.getKeyExtractor().getKeySerializer().getClass());
            int count = 0;
            for (Object o : m.getKeyExtractor().extractKeys(testEntity)) {
                assertEquals(keys.get(indexMetadata.getKey()), o);
                count++;
            }
            assertEquals(1, count);
        }

    }

    @Test
    public void testSaveAndRead() throws Exception {
        TestEntityDAO testEntityDao = new TestEntityDAO(hercules);
        EntityWithCollectionDAO daoForCollection = new EntityWithCollectionDAO(hercules, testEntityDao);


        TestEntity entity = new TestEntity();
        entity.id = "testEnt1";
        entity.stringValue = "Hello";
        entity.longValue = 16L;
        entity.parent = "magic123";
        testEntityDao.save(entity);

        EntityWithCollection e1 = new EntityWithCollection();

        e1.setId("ewc1");
        e1.setFooField("foo");
        e1.getEntities().add(entity);

        ParentFK parentFk = new ParentFK("parentFKKey");
        e1.getSerializableKeys().add(parentFk);

        TestDatedColumn tdc = new TestDatedColumn(System.currentTimeMillis());
        e1.getCollectionWithExplicitSerializer().add(tdc);

        String keyInJson = "keyInjson1";
        e1.setJsonCollection("{" + keyInJson + "}");
        daoForCollection.save(e1);

        EntityWithCollection e2 = new EntityWithCollection();
        e2.setId("ewc2");
        e2.setFooField("bar");
        e2.getSerializableKeys().add(parentFk);

        e2.getCollectionWithExplicitSerializer().add(tdc);
        TestDatedColumn tdc2 = new TestDatedColumn(System.currentTimeMillis() + 10000);
        e2.getCollectionWithExplicitSerializer().add(tdc2);

        String keyInJson2 = "keyInjson2";
        e2.setJsonCollection("{" + keyInJson + ", " + keyInJson2 + "}");


        daoForCollection.save(e2);

        {
            List<EntityWithCollection> l1 = daoForCollection.getEntitiesByEntityCollection(entity);
            assertEquals(1, l1.size());
            assertEquals(e1, l1.get(0));
        }
        {
            List<EntityWithCollection> l2 = daoForCollection.getEntitiesByKeyCollection(parentFk);
            assertEquals(2, l2.size());
            Set<EntityWithCollection> asSet = new HashSet<EntityWithCollection>(l2);
            assertTrue(asSet.contains(e1));
            assertTrue(asSet.contains(e2));
        }
        {
            List<EntityWithCollection> l3 = daoForCollection.getEntitiesBySerializableCollection(tdc);
            assertEquals(2, l3.size());
            Set<EntityWithCollection> asSet2 = new HashSet<EntityWithCollection>(l3);
            assertTrue(asSet2.contains(e1));
            assertTrue(asSet2.contains(e2));
        }
        {
            List<EntityWithCollection> l4 = daoForCollection.getEntitiesBySerializableCollection(tdc2);
            assertEquals(1, l4.size());
            assertEquals(e2, l4.get(0));
        }
        {
            List<EntityWithCollection> l4 = daoForCollection.getEntitiesByKEyFromJson(keyInJson);
            assertEquals(2, l4.size());
            Set<EntityWithCollection> asSet2 = new HashSet<EntityWithCollection>(l4);
            assertTrue(asSet2.contains(e1));
            assertTrue(asSet2.contains(e2));
        }

        {
            List<EntityWithCollection> l4 = daoForCollection.getEntitiesByKEyFromJson(keyInJson2);
            assertEquals(1, l4.size());
            assertEquals(e2, l4.get(0));
        }
    }


}
