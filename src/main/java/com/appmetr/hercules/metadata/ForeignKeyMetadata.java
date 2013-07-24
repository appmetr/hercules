package com.appmetr.hercules.metadata;

import com.appmetr.hercules.keys.ForeignKey;
import com.appmetr.hercules.serializers.AbstractHerculesSerializer;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class ForeignKeyMetadata {
    private Class<? extends ForeignKey> keyClass; // used for all keys include primitive
    private String columnFamily; // used for foreign keys (indexes)

    private Class<? extends AbstractHerculesSerializer> serializer;

    private List<Field> keyFields = new ArrayList<Field>();  // used for all composite keys (PrimaryKeys and ForeignKeys)

    public Class<? extends ForeignKey> getKeyClass() { return keyClass; }

    public void setKeyClass(Class<? extends ForeignKey> keyClass) { this.keyClass = keyClass; }

    public String getColumnFamily() { return columnFamily; }

    public void setColumnFamily(String columnFamily) { this.columnFamily = columnFamily; }

    public Class<? extends AbstractHerculesSerializer> getSerializer() { return serializer; }

    public void setSerializer(Class<? extends AbstractHerculesSerializer> serializer) { this.serializer = serializer; }

    public List<Field> getKeyFields() { return keyFields; }

    public void setKeyFields(List<Field> keyFields) { this.keyFields = keyFields; }

    public void addKeyField(Field field) { keyFields.add(field); }

}
