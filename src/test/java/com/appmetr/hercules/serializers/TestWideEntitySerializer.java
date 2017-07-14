package com.appmetr.hercules.serializers;

import com.appmetr.hercules.model.TestWideEntity;

import com.appmetr.hercules.utils.SerializationUtils;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;


import java.nio.ByteBuffer;

public class TestWideEntitySerializer extends TypeCodec<TestWideEntity> {
    public TestWideEntitySerializer() {
        super(DataType.blob(), TestWideEntity.class);
    }

    @Override public ByteBuffer serialize(TestWideEntity testWideEntity, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return ByteArrayCodec.bytearray().serialize(SerializationUtils.serialize(testWideEntity), protocolVersion);
    }

    @Override public TestWideEntity deserialize(ByteBuffer byteBuffer, ProtocolVersion protocolVersion) throws InvalidTypeException {
        TestWideEntity entity = new TestWideEntity();
        SerializationUtils.deserialize(ByteArrayCodec.bytearray().deserialize(byteBuffer, protocolVersion), entity);

        return entity;
    }

    @Override public TestWideEntity parse(String s) throws InvalidTypeException {
        return null;
    }

    @Override public String format(TestWideEntity testWideEntity) throws InvalidTypeException {
        return null;
    }
}
