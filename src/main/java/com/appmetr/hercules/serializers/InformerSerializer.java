package com.appmetr.hercules.serializers;

import com.appmetr.hercules.profile.DataOperationsProfile;
import com.appmetr.monblank.StopWatch;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.ddl.ComparatorType;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InformerSerializer<T> implements Serializer<T> {

    private Serializer<T> serializer;
    private DataOperationsProfile dataOperationsProfile;

    public InformerSerializer(Serializer<T> serializer, DataOperationsProfile dataOperationsProfile) {
        this.serializer = serializer;
        this.dataOperationsProfile = dataOperationsProfile;
    }

    @Override public ByteBuffer toByteBuffer(T obj) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        ByteBuffer byteBuffer = serializer.toByteBuffer(obj);
        long time = stopWatch.stop();

        if (dataOperationsProfile != null) {
            dataOperationsProfile.bytes += byteBuffer.remaining();
            dataOperationsProfile.serializationMs += time;
        }

        return byteBuffer;
    }

    @Override public T fromByteBuffer(ByteBuffer byteBuffer) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        long bytes = byteBuffer.remaining();

        T obj = serializer.fromByteBuffer(byteBuffer);
        long time = stopWatch.stop();

        if (dataOperationsProfile != null) {
            dataOperationsProfile.bytes += bytes;
            dataOperationsProfile.deserializationMs += time;
        }
        return obj;
    }

    @Override public byte[] toBytes(T obj) {
        return serializer.toBytes(obj);
    }

    @Override public T fromBytes(byte[] bytes) {
        return serializer.fromBytes(bytes);
    }

    @Override public Set<ByteBuffer> toBytesSet(List<T> list) {
        return serializer.toBytesSet(list);
    }

    @Override public List<T> fromBytesSet(Set<ByteBuffer> list) {
        return serializer.fromBytesSet(list);
    }

    @Override public <V> Map<ByteBuffer, V> toBytesMap(Map<T, V> map) {
        return serializer.toBytesMap(map);
    }

    @Override public <V> Map<T, V> fromBytesMap(Map<ByteBuffer, V> map) {
        return serializer.fromBytesMap(map);
    }

    @Override public List<ByteBuffer> toBytesList(List<T> list) {
        return serializer.toBytesList(list);
    }

    @Override public List<T> fromBytesList(List<ByteBuffer> list) {
        return serializer.fromBytesList(list);
    }

    @Override public ComparatorType getComparatorType() {
        return serializer.getComparatorType();
    }
}
