package com.appmetr.hercules.driver.serializer;

import me.prettyprint.hector.api.Serializer;

public class UniversalRowSerializer<R, T> extends AbstractRowSerializer<R, T> {
    Serializer universalSerializer;

    public UniversalRowSerializer(Serializer<R> rowKeySerializer, Serializer<T> topKeySerializer, Serializer universalSerializer) {
        super(rowKeySerializer, topKeySerializer);

        this.universalSerializer = universalSerializer;
    }

    @Override public boolean hasValueSerializer(T topKey) {
        return true;
    }

    @Override public Serializer getValueSerializer(T topKey) {
        return universalSerializer;
    }
}
