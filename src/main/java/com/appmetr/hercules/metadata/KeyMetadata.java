package com.appmetr.hercules.metadata;

import com.datastax.driver.core.TypeCodec;

import java.lang.reflect.Field;

public class KeyMetadata {

    private Class keyClass;
    private Field field;
    private Class<? extends TypeCodec> serializer;

    public Class getKeyClass() {
        return keyClass;
    }

    public void setKeyClass(Class keyClass) {
        this.keyClass = keyClass;
    }

    public Field getField() {
        return field;
    }

    public void setField(Field field) {
        this.field = field;
    }

    public Class<? extends TypeCodec> getSerializer() {
        return serializer;
    }

    public void setSerializer(Class<? extends TypeCodec> serializer) {
        this.serializer = serializer;
    }
}
