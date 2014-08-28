package com.appmetr.hercules.keys;

import me.prettyprint.hector.api.Serializer;

import javax.inject.Inject;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;

public class SerializableKeyCollectionKeyExtractor<E, K> implements CollectionKeysExtractor<E, K> {

    private Field collectionField;
    private Serializer<K> keySerializer;

    @Inject
    public SerializableKeyCollectionKeyExtractor(Field collectionField, Serializer<K> serializer) {
        this.collectionField = collectionField;
        this.keySerializer = serializer;
    }

    @Override public Iterable<K> extractKeys(E entity) {
        try {
            Collection<K> keys = (Collection<K>) collectionField.get(entity);
            if (keys!=null){
                return keys;
            }else{
                return Collections.emptyList();
                        
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Unable to read indexed collection field for entity of class " + entity.getClass().getName() + " and field " + collectionField.getName());
        }
    }

    @Override public Serializer<K> getKeySerializer() {
        return keySerializer;
    }

}