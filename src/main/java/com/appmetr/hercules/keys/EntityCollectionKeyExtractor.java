package com.appmetr.hercules.keys;

import com.appmetr.hercules.manager.EntityManager;
import me.prettyprint.hector.api.Serializer;

import javax.inject.Inject;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class EntityCollectionKeyExtractor<E, K> implements CollectionKeysExtractor<E, K> {

    private EntityManager em;
    private Field collectionField;
    private Class<?> collectionEntityClass;

    @Inject
    public EntityCollectionKeyExtractor(Field collectionField, Class<?> collectionEntityClass, EntityManager em) {
        this.collectionField = collectionField;
        this.collectionEntityClass = collectionEntityClass;
        this.em = em;
    }

    @Override public Iterable<K> extractKeys(E entity) {
        try {
            Collection indexedCollection = (Collection) collectionField.get(entity);
            if (indexedCollection != null) {
                List<K> keys = new ArrayList<K>(indexedCollection.size());
                for (Object entityInIndex : indexedCollection) {
                    keys.add(em.<Object, K>getPK(entityInIndex));
                }
                return keys;
            } else {
                return Collections.emptyList();
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Unable to read indexed collection field for entity of class " + entity.getClass().getName() + " and field " + collectionField.getName());
        }
    }

    @Override public Serializer<K> getKeySerializer() {
        return em.getPKSerializer(collectionEntityClass);
    }
}
