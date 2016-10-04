package com.appmetr.hercules.dao;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.annotations.listeners.PostLoad;
import com.appmetr.hercules.annotations.listeners.PrePersist;
import com.appmetr.hercules.column.TestDatedColumn;
import com.appmetr.hercules.keys.ParentFK;
import com.appmetr.hercules.model.EntityWithCollection;
import com.appmetr.hercules.model.TestEntity;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import org.apache.commons.lang.StringUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class EntityWithCollectionDAO extends AbstractDAO<EntityWithCollection, String> {

    private TestEntityDAO testEntityDAO;

    @Inject
    public EntityWithCollectionDAO(Hercules hercules, TestEntityDAO testEntityDAO) {
        super(EntityWithCollection.class, hercules);
        this.testEntityDAO = testEntityDAO;
    }

    @PostLoad public EntityWithCollection postLoad(EntityWithCollection entity) {
        String[] entitiesIds = entity.getEntitiesPersistField() != null ? entity.getEntitiesPersistField().split(",") : new String[0];
        for (String eId : entitiesIds) {
            entity.getEntities().add(testEntityDAO.get(eId));
        }

        String[] serKeys = entity.getSerializableKeysPersistField() != null ? entity.getSerializableKeysPersistField().split(",") : new String[0];
        for (String k : serKeys) {
            entity.getSerializableKeys().add(new ParentFK(k));
        }

        byte[] datedCols = entity.getCollectionWithExplicitSerializerPersistField();
        if (datedCols != null) {
            for (int i = 0; i < datedCols.length; i += 8) {
                entity.getCollectionWithExplicitSerializer().add(new TestDatedColumn(Longs.fromByteArray(Arrays.copyOfRange(datedCols, i, i + 8))));
            }
        }
        return entity;
    }

    @PrePersist public EntityWithCollection preSave(EntityWithCollection entity) {
        if (entity.getEntities() != null && entity.getEntities().size() > 0) {
            List<String> keys = new ArrayList<String>(entity.getEntities().size());
            for (TestEntity e : entity.getEntities()) {
                keys.add(e.id);
            }
            entity.setEntitiesPersistField(StringUtils.join(keys, ","));
        } else {
            entity.setEntitiesPersistField(null);
        }

        if (entity.getSerializableKeys() != null && entity.getSerializableKeys().size() > 0) {
            List<String> keys = new ArrayList<String>(entity.getSerializableKeys().size());
            for (ParentFK k : entity.getSerializableKeys()) {
                keys.add(k.parent);
            }
            entity.setSerializableKeysPersistField(StringUtils.join(keys, ","));
        } else {
            entity.setSerializableKeysPersistField(null);
        }

        if (entity.getCollectionWithExplicitSerializer() != null && entity.getCollectionWithExplicitSerializer().size() > 0) {
            ByteBuffer buff = ByteBuffer.allocate(8 * entity.getCollectionWithExplicitSerializer().size());

            for (TestDatedColumn c : entity.getCollectionWithExplicitSerializer()) {
                buff.putLong(c.getDate());
            }
            entity.setCollectionWithExplicitSerializerPersistField(buff.array());
        } else {
            entity.setEntitiesPersistField(null);
        }

        return entity;
    }

    public List<EntityWithCollection> getEntitiesByEntityCollection(TestEntity entity) {
        return getByCollectionIndex("entities", entity.id);
    }

    public List<EntityWithCollection> getEntitiesByKeyCollection(ParentFK key) {
        return getByCollectionIndex("serializableKeys", key);
    }

    public List<EntityWithCollection> getEntitiesBySerializableCollection(TestDatedColumn key) {
        return getByCollectionIndex("collectionWithExplicitSerializer", key);
    }

    public List<EntityWithCollection> getEntitiesByKEyFromJson(String key) {
        return getByCollectionIndex("jsonCollection", key);
    }

}
