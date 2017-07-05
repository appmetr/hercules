package com.appmetr.hercules.metadata;

import com.appmetr.hercules.driver.DataDriver;
import com.datastax.driver.core.TypeCodec;

public abstract class AbstractMetadata {
    private Class entityClass;
    private Class<? extends TypeCodec> entitySerializer;

    private String columnFamily;
    //private ComparatorType comparatorType;

    private EntityListenerMetadata listenerMetadata;
    private int entityTTL = DataDriver.EMPTY_TTL;

    public Class getEntityClass() {
        return entityClass;
    }

    public void setEntityClass(Class entityClass) {
        this.entityClass = entityClass;
    }

    public Class<? extends TypeCodec> getEntitySerializer() {
        return entitySerializer;
    }

    public void setEntitySerializer(Class<? extends TypeCodec> entitySerializer) {
        this.entitySerializer = entitySerializer;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    public EntityListenerMetadata getListenerMetadata() {
        return listenerMetadata;
    }

    public void setListenerMetadata(EntityListenerMetadata listenerMetadata) {
        this.listenerMetadata = listenerMetadata;
    }

    public int getEntityTTL() {
        return entityTTL;
    }

    public void setEntityTTL(int entityTTL) {
        this.entityTTL = entityTTL;
    }
}
