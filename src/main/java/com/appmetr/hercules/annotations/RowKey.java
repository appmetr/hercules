package com.appmetr.hercules.annotations;

import com.appmetr.hercules.serializers.AbstractHerculesSerializer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface RowKey {
    Class<? extends AbstractHerculesSerializer> value() default AbstractHerculesSerializer.class;

    Class keyClass() default Object.class;
    Class<? extends AbstractHerculesSerializer> serializer() default AbstractHerculesSerializer.class;
}
