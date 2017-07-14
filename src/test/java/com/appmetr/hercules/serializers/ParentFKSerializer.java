package com.appmetr.hercules.serializers;

import com.appmetr.hercules.keys.ParentFK;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.nio.ByteBuffer;

public class ParentFKSerializer extends TypeCodec<ParentFK> {

    public ParentFKSerializer(Class<ParentFK> javaClass) {
        super(DataType.text(), javaClass);
    }

    @Override public ByteBuffer serialize(ParentFK parentFK, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return TypeCodec.varchar().serialize(parentFK.parent, protocolVersion);
    }

    @Override public ParentFK deserialize(ByteBuffer byteBuffer, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return new ParentFK(TypeCodec.varchar().deserialize(byteBuffer, protocolVersion));
    }

    @Override public ParentFK parse(String s) throws InvalidTypeException {
        return null;
    }

    @Override public String format(ParentFK parentFK) throws InvalidTypeException {
        return null;
    }
}
