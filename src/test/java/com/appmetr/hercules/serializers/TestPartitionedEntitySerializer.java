package com.appmetr.hercules.serializers;

import com.appmetr.hercules.model.TestPartitionedEntity;
import com.appmetr.hercules.utils.SerializationUtils;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.nio.ByteBuffer;

public class TestPartitionedEntitySerializer extends TypeCodec<TestPartitionedEntity> {


    protected TestPartitionedEntitySerializer(DataType cqlType, Class<TestPartitionedEntity> javaClass) {
        super(cqlType, javaClass);
    }

    @Override public ByteBuffer serialize(TestPartitionedEntity obj, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return TypeCodec.blob().serialize(ByteBuffer.wrap(SerializationUtils.serialize(obj)), protocolVersion);
    }

    @Override public TestPartitionedEntity deserialize(ByteBuffer byteBuffer, ProtocolVersion protocolVersion) throws InvalidTypeException {
        TestPartitionedEntity entity = new TestPartitionedEntity();
        SerializationUtils.deserialize(TypeCodec.blob().deserialize(byteBuffer, protocolVersion).array(), entity);

        return entity;
    }

    @Override public TestPartitionedEntity parse(String s) throws InvalidTypeException {
        return null;
    }

    @Override public String format(TestPartitionedEntity testPartitionedEntity) throws InvalidTypeException {
        return null;
    }
}
