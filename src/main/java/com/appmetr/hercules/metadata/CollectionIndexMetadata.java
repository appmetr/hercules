package com.appmetr.hercules.metadata;

import com.appmetr.hercules.keys.CollectionKeysExtractor;

import java.lang.reflect.Field;

public class CollectionIndexMetadata {

    private String indexColumnFamily;
    private Field indexedField;

    private CollectionKeysExtractor keyExtractor;

    public String getIndexColumnFamily() { return indexColumnFamily; }

    public void setIndexColumnFamily(String indexColumnFamily) { this.indexColumnFamily = indexColumnFamily; }

    public Field getIndexedField() {return indexedField;}

    public void setIndexedField(Field indexedField) {this.indexedField = indexedField;}

    public CollectionKeysExtractor getKeyExtractor() { return keyExtractor; }

    public void setKeyExtractor(CollectionKeysExtractor keyExtractor) { this.keyExtractor = keyExtractor; }

}
