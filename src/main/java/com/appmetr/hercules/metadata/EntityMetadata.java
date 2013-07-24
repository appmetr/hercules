package com.appmetr.hercules.metadata;

import com.appmetr.hercules.keys.ForeignKey;
import com.appmetr.hercules.serializers.AbstractHerculesSerializer;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EntityMetadata extends AbstractMetadata {
    private KeyMetadata primaryKeyMetadata;
    private boolean isPrimaryKeyGenerated = false;

    private Map<Class<? extends ForeignKey>, ForeignKeyMetadata> indexes = new HashMap<Class<? extends ForeignKey>, ForeignKeyMetadata>();
    private boolean createPrimaryKeyIndex = false;

    private Map<Field, String> fieldToColumn = new HashMap<Field, String>();
    private Map<String, Class> columnClasses = new HashMap<String, Class>();
    private Map<String, Class<? extends AbstractHerculesSerializer>> columnSerializers = new HashMap<String, Class<? extends AbstractHerculesSerializer>>();
    List<String> notNullColumns = new ArrayList<String>();

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

    public void addIndex(Class<? extends ForeignKey> keyClass, ForeignKeyMetadata keyMetadata) {
        indexes.put(keyClass, keyMetadata);
    }

    public boolean isCreatePrimaryKeyIndex() {
        return createPrimaryKeyIndex;
    }

    public void setCreatePrimaryKeyIndex(boolean createPrimaryKeyIndex) {
        this.createPrimaryKeyIndex = createPrimaryKeyIndex;
    }

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

    public Map<String, Class<? extends AbstractHerculesSerializer>> getColumnSerializers() {
        return columnSerializers;
    }

    public void setColumnSerializers(Map<String, Class<? extends AbstractHerculesSerializer>> columnSerializers) {
        this.columnSerializers = columnSerializers;
    }

    public Class<? extends AbstractHerculesSerializer> getColumnSerializer(String name) { return columnSerializers.get(name); }

    public void setColumnSerializer(String name, Class<? extends AbstractHerculesSerializer> clazz) { columnSerializers.put(name, clazz); }

    public List<String> getNotNullColumns() {
        return notNullColumns;
    }

    public void setNotNullColumns(List<String> notNullColumns) {
        this.notNullColumns = notNullColumns;
    }

    public Boolean isNotNullColumn(String name) { return notNullColumns.contains(name); }

    public void addNotNullColumn(String name) { notNullColumns.add(name); }
}
