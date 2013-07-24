package com.appmetr.hercules.driver.serializer;

import me.prettyprint.hector.api.Serializer;

public interface RowSerializer<K, T> {
    public Serializer<K> getRowKeySerializer();
    public Serializer<T> getTopKeySerializer();
    public boolean hasValueSerializer(T topKey);
    public Serializer getValueSerializer(T topKey);
}
