package com.appmetr.hercules.serializers;

import com.appmetr.hercules.Hercules;
import com.datastax.driver.core.TypeCodec;
import com.google.inject.Inject;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

public class SerializerProvider {
    @Inject Hercules hercules;

    public <T> TypeCodec<T> getSerializer(Class<? extends TypeCodec> serializerClass, Class valueClass) {
        TypeCodec serializer = null;
        if (valueClass == BigInteger.class) {
            serializer = TypeCodec.varint();
        } else if (valueClass.equals(Boolean.class) || valueClass.equals(boolean.class)) {
            serializer = TypeCodec.cboolean();
        } else if (valueClass.equals(byte[].class)) {
            serializer = TypeCodec.blob();
        } else if (valueClass.equals(ByteBuffer.class)) {
            serializer = TypeCodec.blob();
        } else if (valueClass.equals(Character.class)) {
            serializer = TypeCodec.varchar();
        } else if (valueClass.equals(Date.class)) {
            serializer = TypeCodec.timestamp();
        } else if (valueClass.equals(Double.class) || valueClass.equals(double.class)) {
            serializer = TypeCodec.cdouble();
        } else if (valueClass.equals(Float.class) || valueClass.equals(float.class)) {
            serializer = TypeCodec.cfloat();
        } else if (valueClass.equals(Integer.class) || valueClass.equals(int.class)) {
            serializer = TypeCodec.cint();
        } else if (valueClass.equals(Long.class) || valueClass.equals(long.class)) {
            serializer = TypeCodec.bigint();
        } else if (valueClass.equals(Short.class) || valueClass.equals(short.class)) {
            serializer = TypeCodec.smallInt();
        } else if (valueClass.equals(String.class)) {
            serializer = TypeCodec.varchar();
        } else if (valueClass.equals(UUID.class)) {
            serializer = TypeCodec.uuid();
        } else {
            serializer = hercules.getCodecByClassType().get(valueClass);
        }
        // Add other serializers here
        return serializer;
    }
}
