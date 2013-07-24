package com.appmetr.hercules.metadata;

import com.appmetr.hercules.serializers.AbstractHerculesSerializer;
import me.prettyprint.hector.api.ddl.ComparatorType;

public abstract class AbstractMetadata {
    private Class entityClass;
    private Class<? extends AbstractHerculesSerializer> entitySerializer;

    private String columnFamily;
    private ComparatorType comparatorType;

    private EntityListenerMetadata listenerMetadata;

    public Class getEntityClass() {
        return entityClass;
    }

    public void setEntityClass(Class entityClass) {
        this.entityClass = entityClass;
    }

    public Class<? extends AbstractHerculesSerializer> getEntitySerializer() {
        return entitySerializer;
    }

    public void setEntitySerializer(Class<? extends AbstractHerculesSerializer> entitySerializer) {
        this.entitySerializer = entitySerializer;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    public ComparatorType getComparatorType() {
        return comparatorType;
    }

    public void setComparatorType(ComparatorType comparatorType) {
        this.comparatorType = comparatorType;
    }

    public EntityListenerMetadata getListenerMetadata() {
        return listenerMetadata;
    }

    public void setListenerMetadata(EntityListenerMetadata listenerMetadata) {
        this.listenerMetadata = listenerMetadata;
    }
}
