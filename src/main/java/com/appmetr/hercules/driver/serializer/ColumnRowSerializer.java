package com.appmetr.hercules.driver.serializer;

import me.prettyprint.hector.api.Serializer;

import java.util.HashMap;
import java.util.Map;

public class ColumnRowSerializer<K, T> extends AbstractRowSerializer<K, T> {
    private Map<T, Serializer> columnSerializers = new HashMap<T, Serializer>();

    public ColumnRowSerializer(Serializer<K> rowKeySerializer, Serializer<T> topKeySerializer, Map<T, Serializer> columnSerializers) {
        super(rowKeySerializer, topKeySerializer);

        this.columnSerializers = columnSerializers;
    }

    @Override public boolean hasValueSerializer(T topKey) {
        return columnSerializers.containsKey(topKey);
    }

    @Override public Serializer getValueSerializer(T topKey) {
        return columnSerializers.get(topKey);
    }
}
