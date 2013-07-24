package com.appmetr.hercules.driver.serializer;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.hector.api.Serializer;

public class ByteArrayRowSerializer<K, T> extends UniversalRowSerializer<K, T> {
    public ByteArrayRowSerializer(Serializer<K> rowKeySerializer, Serializer<T> topKeySerializer) {
        super(rowKeySerializer, topKeySerializer, new BytesArraySerializer());
    }
}
