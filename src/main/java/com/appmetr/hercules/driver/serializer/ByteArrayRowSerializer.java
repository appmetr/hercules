package com.appmetr.hercules.driver.serializer;

import com.datastax.driver.core.TypeCodec;

public class ByteArrayRowSerializer<K, T> extends UniversalRowSerializer<K, T> {
    public ByteArrayRowSerializer(TypeCodec<K> rowKeySerializer, TypeCodec<T> topKeySerializer) {
        super(rowKeySerializer, topKeySerializer, TypeCodec.blob());
    }
}
