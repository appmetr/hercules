package com.appmetr.hercules.serializers;

import com.datastax.driver.core.TypeCodec;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class SerializerProvider {

    public TypeCodec getSerializer(Class<? extends TypeCodec> serializerClass, Class valueClass) {
        if (serializerClass != null) {
            return tryToCreateObject(serializerClass, valueClass);
        } else {
            return getSerializer(valueClass);
        }
        //todo what will we do with typeCodecs with args. like CompositeString*Codec
    }

    private TypeCodec tryToCreateObject(Class<? extends TypeCodec> serializerClass, Class valueClass) {
        try {
            Constructor<?>[] constructors = serializerClass.getConstructors();
            List<Constructor<?>> constructorList = Arrays.stream(constructors)
                    .sorted(Comparator.comparingInt(Constructor::getParameterCount))
                    .collect(Collectors.toList());
            if (constructorList.isEmpty()) {
                throw new RuntimeException("no available public constructor");
            }
            Constructor<?> constructor = constructorList.get(0);
            switch (constructor.getParameterCount()) {
                case 0:
                    return (TypeCodec) constructor.newInstance();
                case 1:
                    return (TypeCodec) constructor.newInstance(valueClass);
                default:
                    throw new UnsupportedOperationException("don't support constructor with more than 1 arg. available only for " + constructor.getParameterCount());
            }
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("cannot construct serializer" + serializerClass, e);
        }
    }

    public TypeCodec getSerializer(Class valueClass) {
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
        }
        return serializer;
    }
}
