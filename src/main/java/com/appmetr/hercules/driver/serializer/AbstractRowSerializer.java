package com.appmetr.hercules.driver.serializer;

import me.prettyprint.hector.api.Serializer;

public abstract class AbstractRowSerializer<K, T> implements RowSerializer<K, T> {
    private Serializer<K> rowKeySerializer;
    private Serializer<T> topKeySerializer;

    public AbstractRowSerializer(Serializer<K> rowKeySerializer, Serializer<T> topKeySerializer) {
        this.rowKeySerializer = rowKeySerializer;
        this.topKeySerializer = topKeySerializer;
    }

    public Serializer<K> getRowKeySerializer() {
        return rowKeySerializer;
    }

    public Serializer<T> getTopKeySerializer() {
        return topKeySerializer;
    }
}
