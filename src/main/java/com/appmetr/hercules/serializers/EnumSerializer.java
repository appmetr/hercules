package com.appmetr.hercules.serializers;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.hector.api.ddl.ComparatorType;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static me.prettyprint.hector.api.ddl.ComparatorType.UTF8TYPE;

/**
 * Basically it has same behavior as String serializer, except we cant have static instance and
 * must gain class type info from outside as constructor parameter
 * @param <T> Enum object you want to serialize
 */
public class EnumSerializer<T extends Enum<T>> extends AbstractSerializer<T> {

    private static final String UTF_8 = "UTF-8";
    private static final Charset charset = Charset.forName(UTF_8);

    private final Class<T> enumClass;

    public EnumSerializer(Class<T> enumClass) {
        this.enumClass = enumClass;
    }

    @Override public ByteBuffer toByteBuffer(Enum obj) {
        if (obj == null) {
            return null;
        }
        return ByteBuffer.wrap(obj.name().getBytes(charset));
    }

    @Override public T fromByteBuffer(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return null;
        }
        String enumValue = charset.decode(byteBuffer).toString();
        try {
            return Enum.valueOf(enumClass, enumValue);
        } catch (Exception e) {
            throw new RuntimeException("Read value is not of enum type: '" + enumValue + "'", e);
        }
    }

    @Override
    public ComparatorType getComparatorType() {
        return UTF8TYPE;
    }
}
