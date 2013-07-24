package com.appmetr.hercules.serializers;

import com.appmetr.hercules.model.TestPartitionedEntity;
import com.appmetr.hercules.utils.SerializationUtils;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;

import java.nio.ByteBuffer;

public class TestPartitionedEntitySerializer extends AbstractHerculesSerializer<TestPartitionedEntity> {
    @Override public ByteBuffer toByteBuffer(TestPartitionedEntity obj) {
        return ByteBuffer.wrap(SerializationUtils.serialize(obj));
    }

    @Override public TestPartitionedEntity fromByteBuffer(ByteBuffer byteBuffer) {
        TestPartitionedEntity entity = new TestPartitionedEntity();
        SerializationUtils.deserialize(BytesArraySerializer.get().fromByteBuffer(byteBuffer), entity);

        return entity;
    }
}
