package com.appmetr.hercules.metadata;

import com.appmetr.hercules.serializers.AbstractHerculesSerializer;

import java.lang.reflect.Field;

public class KeyMetadata {

    private Class keyClass;
    private Field field;
    private Class<? extends AbstractHerculesSerializer> serializer;

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

    public Class<? extends AbstractHerculesSerializer> getSerializer() {
        return serializer;
    }

    public void setSerializer(Class<? extends AbstractHerculesSerializer> serializer) {
        this.serializer = serializer;
    }
}
