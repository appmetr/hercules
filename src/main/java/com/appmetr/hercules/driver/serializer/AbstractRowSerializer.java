package com.appmetr.hercules.driver.serializer;

import com.datastax.driver.core.TypeCodec;

public abstract class AbstractRowSerializer<K, T> implements RowSerializer<K, T> {
    private TypeCodec<K> rowKeySerializer;
    private TypeCodec<T> topKeySerializer;

    public AbstractRowSerializer(TypeCodec<K> rowKeySerializer, TypeCodec<T> topKeySerializer) {
        this.rowKeySerializer = rowKeySerializer;
        this.topKeySerializer = topKeySerializer;
    }

    public TypeCodec<K> getRowKeySerializer() {
        return rowKeySerializer;
    }

    public TypeCodec<T> getTopKeySerializer() {
        return topKeySerializer;
    }
}
