package com.appmetr.hercules.metadata;

import com.appmetr.hercules.keys.ForeignKey;
import com.datastax.driver.core.TypeCodec;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EntityMetadata extends AbstractMetadata {
    private KeyMetadata primaryKeyMetadata;
    private boolean isPrimaryKeyGenerated = false;

    private Map<Class<? extends ForeignKey>, ForeignKeyMetadata> indexes = new HashMap<>();
    private boolean createPrimaryKeyIndex = false;
    private Map<String, CollectionIndexMetadata> collectionIndexes = new HashMap<>();

    private Map<Field, String> fieldToColumn = new HashMap<>();
    private Map<String, Class> columnClasses = new HashMap<>();
    private Map<String, Class<? extends TypeCodec>> columnSerializers = new HashMap<>();
    List<String> notNullColumns = new ArrayList<>();

    public KeyMetadata getPrimaryKeyMetadata() {
        return primaryKeyMetadata;
    }

    public void setPrimaryKeyMetadata(KeyMetadata primaryKeyMetadata) {
        this.primaryKeyMetadata = primaryKeyMetadata;
    }

    public boolean isPrimaryKeyGenerated() {
        return isPrimaryKeyGenerated;
    }

    public void setPrimaryKeyGenerated(boolean primaryKeyGenerated) {
        isPrimaryKeyGenerated = primaryKeyGenerated;
    }

    public Map<Class<? extends ForeignKey>, ForeignKeyMetadata> getIndexes() {
        return indexes;
    }

    public void setIndexes(Map<Class<? extends ForeignKey>, ForeignKeyMetadata> indexes) {
        this.indexes = indexes;
    }

    public ForeignKeyMetadata getIndexMetadata(Class<? extends ForeignKey> keyClass) {
        return indexes.get(keyClass);
    }

    public void addIndex(Class<? extends ForeignKey> keyClass, ForeignKeyMetadata keyMetadata) { indexes.put(keyClass, keyMetadata); }

    public boolean isCreatePrimaryKeyIndex() {
        return createPrimaryKeyIndex;
    }

    public void setCreatePrimaryKeyIndex(boolean createPrimaryKeyIndex) { this.createPrimaryKeyIndex = createPrimaryKeyIndex; }

    public Map<String, CollectionIndexMetadata> getCollectionIndexes() { return collectionIndexes; }

    public void setCollectionIndexes(Map<String, CollectionIndexMetadata> collectionIndexes) { this.collectionIndexes = collectionIndexes; }

    public Map<Field, String> getFieldToColumn() {
        return fieldToColumn;
    }

    public void setFieldToColumn(Map<Field, String> fieldToColumn) {
        this.fieldToColumn = fieldToColumn;
    }

    public String getFieldColumn(Field field) { return fieldToColumn.get(field); }

    public void setFieldColumn(Field fieldName, String columnName) { fieldToColumn.put(fieldName, columnName); }

    public Map<String, Class> getColumnClasses() {
        return columnClasses;
    }

    public void setColumnClasses(Map<String, Class> columnClasses) {
        this.columnClasses = columnClasses;
    }

    public Class getColumnClass(String name) { return columnClasses.get(name); }

    public void setColumnClass(String name, Class clazz) { columnClasses.put(name, clazz); }

    public Map<String, Class<? extends TypeCodec>> getColumnSerializers() {
        return columnSerializers;
    }

    public void setColumnSerializers(Map<String, Class<? extends TypeCodec>> columnSerializers) { this.columnSerializers = columnSerializers; }

    public Class<? extends TypeCodec> getColumnSerializer(String name) { return columnSerializers.get(name); }

    public void setColumnSerializer(String name, Class<? extends TypeCodec> clazz) { columnSerializers.put(name, clazz); }

    public List<String> getNotNullColumns() {
        return notNullColumns;
    }

    public void setNotNullColumns(List<String> notNullColumns) {
        this.notNullColumns = notNullColumns;
    }

    public Boolean isNotNullColumn(String name) { return notNullColumns.contains(name); }

    public void addNotNullColumn(String name) { notNullColumns.add(name); }
}
