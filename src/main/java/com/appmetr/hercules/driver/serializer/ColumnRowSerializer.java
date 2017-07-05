package com.appmetr.hercules.driver.serializer;

import com.datastax.driver.core.TypeCodec;

import java.util.HashMap;
import java.util.Map;

public class ColumnRowSerializer<K, T> extends AbstractRowSerializer<K, T> {
    private Map<T, TypeCodec> columnSerializers = new HashMap<>();

    public ColumnRowSerializer(TypeCodec<K> rowKeySerializer, TypeCodec<T> topKeySerializer, Map<T, TypeCodec> columnSerializers) {
        super(rowKeySerializer, topKeySerializer);

        this.columnSerializers = columnSerializers;
    }

    @Override public boolean hasValueSerializer(T topKey) {
        return columnSerializers.containsKey(topKey);
    }

    @Override public TypeCodec getValueSerializer(T topKey) {
        return columnSerializers.get(topKey);
    }
}
