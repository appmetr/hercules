package com.appmetr.hercules.serializers;

import com.google.inject.Inject;
import com.google.inject.Injector;

public class SerializerProvider {
    @Inject Injector injector;

    public <T> AbstractHerculesSerializer<T> getSerializer(Class<? extends AbstractHerculesSerializer> serializerClass, Class instanceClass) {
        AbstractHerculesSerializer<T> serializer = (AbstractHerculesSerializer<T>) injector.getInstance(serializerClass);

        serializer.setInstanceClass(instanceClass);

        return serializer;
    }
}
