package com.appmetr.hercules.driver.serializer;

import com.datastax.driver.core.TypeCodec;

public interface RowSerializer<K, T> {
    TypeCodec<K> getRowKeySerializer();
    TypeCodec<T> getTopKeySerializer();
    boolean hasValueSerializer(T topKey);
    TypeCodec getValueSerializer(T topKey);
}
