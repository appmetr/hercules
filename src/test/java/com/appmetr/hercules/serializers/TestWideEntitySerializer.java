package com.appmetr.hercules.serializers;

import com.appmetr.hercules.model.TestWideEntity;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;


import java.nio.ByteBuffer;

public class TestWideEntitySerializer extends TypeCodec<TestWideEntity> {
    protected TestWideEntitySerializer(DataType cqlType, Class<TestWideEntity> javaClass) {
        super(cqlType, javaClass);
    }

    @Override public ByteBuffer serialize(TestWideEntity testWideEntity, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return null;
    }

    @Override public TestWideEntity deserialize(ByteBuffer byteBuffer, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return null;
    }

    @Override public TestWideEntity parse(String s) throws InvalidTypeException {
        return null;
    }

    @Override public String format(TestWideEntity testWideEntity) throws InvalidTypeException {
        return null;
    }
    /*@Override public ByteBuffer toByteBuffer(TestWideEntity obj) {
        return ByteBuffer.wrap(SerializationUtils.serialize(obj));
    }

    @Override public TestWideEntity fromByteBuffer(ByteBuffer byteBuffer) {
        TestWideEntity entity = new TestWideEntity();
        SerializationUtils.deserialize(BytesArraySerializer.get().fromByteBuffer(byteBuffer), entity);

        return entity;
    }*/
}
