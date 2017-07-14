package com.appmetr.hercules.serializers;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Basically it has same behavior as String serializer, except we cant have static instance and
 * must gain class type info from outside as constructor parameter
 * @param <T> Enum object you want to serialize
 */
public class EnumSerializer<T extends Enum<T>> extends TypeCodec<T> {

    private static final String UTF_8 = "UTF-8";
    private static final Charset charset = Charset.forName(UTF_8);

    private final Class<T> enumClass;

    public EnumSerializer(Class<T> javaClass) {
        super(DataType.blob(), javaClass);
        this.enumClass = javaClass;
    }


    @Override public ByteBuffer serialize(T value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (value == null) {
            return null;
        }
        return TypeCodec.blob().serialize(ByteBuffer.wrap(value.name().getBytes(charset)), protocolVersion);

    }

    @Override public T deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (bytes == null) {
            return null;
        }
        String enumValue = charset.decode(TypeCodec.blob().deserialize(bytes, protocolVersion)).toString();
        try {
            return Enum.valueOf(enumClass, enumValue);
        } catch (Exception e) {
            throw new RuntimeException("Read value is not of enum type: '" + enumValue + "'", e);
        }
    }

    @Override public T parse(String value) throws InvalidTypeException {
        throw new UnsupportedOperationException();
    }

    @Override public String format(T value) throws InvalidTypeException {
        throw new UnsupportedOperationException();
    }
}
