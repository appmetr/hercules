package com.appmetr.hercules.driver.serializer;

import com.datastax.driver.core.TypeCodec;

public class UniversalRowSerializer<R, T> extends AbstractRowSerializer<R, T> {
    TypeCodec universalSerializer;

    public UniversalRowSerializer(TypeCodec<R> rowKeySerializer, TypeCodec<T> topKeySerializer, TypeCodec universalSerializer) {
        super(rowKeySerializer, topKeySerializer);

        this.universalSerializer = universalSerializer;
    }

    @Override public boolean hasValueSerializer(T topKey) {
        return true;
    }

    @Override public TypeCodec getValueSerializer(T topKey) {
        return universalSerializer;
    }
}
