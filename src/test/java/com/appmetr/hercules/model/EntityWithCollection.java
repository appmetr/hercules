package com.appmetr.hercules.model;

import com.appmetr.hercules.annotations.*;
import com.appmetr.hercules.annotations.listeners.EntityListener;
import com.appmetr.hercules.column.TestDatedColumn;
import com.appmetr.hercules.dao.EntityWithCollectionDAO;
import com.appmetr.hercules.keys.ParentFK;
import com.appmetr.hercules.serializers.TestDatedColumnSerializer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Entity
@EntityListener(EntityWithCollectionDAO.class)
public class EntityWithCollection implements Serializable {


    @Id @GeneratedGUID private String id;

    private String fooField;

    @Transient
    @IndexedCollection(itemClass = TestEntity.class)
    private Set<TestEntity> entities = new HashSet<>();
    String entitiesPersistField;

    @Transient
    @IndexedCollection(itemClass = ParentFK.class)
    private List<ParentFK> serializableKeys = new ArrayList<>();
    String serializableKeysPersistField;

    @Transient
    @IndexedCollection(name = "haveSerializer", itemClass = TestDatedColumn.class, serializer = TestDatedColumnSerializer.class)
    private Set<TestDatedColumn> collectionWithExplicitSerializer = new HashSet<>(); // EntityWithCollection_collectionWithExplicitSerializer is 53 chars long. cassandra limit is 48, so using short name is necessary
    byte[] collectionWithExplicitSerializerPersistField;


    @IndexedCollection(keyExtractorClass = TestJsonKeyExtractor.class)
    private String jsonCollection = "{}";


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getFooField() {
        return fooField;
    }

    public void setFooField(String fooField) {
        this.fooField = fooField;
    }

    public Set<TestEntity> getEntities() {
        return entities;
    }

    public void setEntities(Set<TestEntity> entities) {
        this.entities = entities;
    }

    public List<ParentFK> getSerializableKeys() {
        return serializableKeys;
    }

    public void setSerializableKeys(List<ParentFK> serializableKeys) {
        this.serializableKeys = serializableKeys;
    }

    public Set<TestDatedColumn> getCollectionWithExplicitSerializer() {
        return collectionWithExplicitSerializer;
    }

    public void setCollectionWithExplicitSerializer(Set<TestDatedColumn> collectionWithExplicitSerializer) {
        this.collectionWithExplicitSerializer = collectionWithExplicitSerializer;
    }

    public String getJsonCollection() {
        return jsonCollection;
    }

    public void setJsonCollection(String jsonCollection) {
        this.jsonCollection = jsonCollection;
    }

    public String getEntitiesPersistField() {
        return entitiesPersistField;
    }

    public void setEntitiesPersistField(String entitiesPersistField) {
        this.entitiesPersistField = entitiesPersistField;
    }

    public String getSerializableKeysPersistField() {
        return serializableKeysPersistField;
    }

    public void setSerializableKeysPersistField(String serializableKeysPersistField) {
        this.serializableKeysPersistField = serializableKeysPersistField;
    }

    public byte[] getCollectionWithExplicitSerializerPersistField() {
        return collectionWithExplicitSerializerPersistField;
    }

    public void setCollectionWithExplicitSerializerPersistField(byte[] collectionWithExplicitSerializerPersistField) {
        this.collectionWithExplicitSerializerPersistField = collectionWithExplicitSerializerPersistField;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EntityWithCollection that = (EntityWithCollection) o;

        if (fooField != null ? !fooField.equals(that.fooField) : that.fooField != null) return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (jsonCollection != null ? !jsonCollection.equals(that.jsonCollection) : that.jsonCollection != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (fooField != null ? fooField.hashCode() : 0);
        result = 31 * result + (jsonCollection != null ? jsonCollection.hashCode() : 0);
        return result;
    }
}
