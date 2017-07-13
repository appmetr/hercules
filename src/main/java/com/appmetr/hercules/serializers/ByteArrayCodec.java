package com.appmetr.hercules.serializers;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.nio.ByteBuffer;

public class ByteArrayCodec extends TypeCodec<byte[]>{

    private static TypeCodec instance = new ByteArrayCodec();

    private ByteArrayCodec() {
        super(DataType.blob(), byte[].class);
    }

    @Override public ByteBuffer serialize(byte[] value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (value == null) {
            return null;
        }
        return ByteBuffer.wrap(value);
    }

    @Override public byte[] deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (bytes == null) {
            return null;
        }
        byte[] copy = new byte[bytes.remaining()];
        bytes.get(copy, 0,copy.length);
        return copy;
    }

    @Override public byte[] parse(String value) throws InvalidTypeException {
        throw new UnsupportedOperationException();
    }

    @Override public String format(byte[] value) throws InvalidTypeException {
        throw new UnsupportedOperationException();
    }

    public static TypeCodec bytearray() {
        return instance;
    }
}
