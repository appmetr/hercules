package com.appmetr.hercules.annotations;

import com.datastax.driver.core.TypeCodec;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface RowKey {
    Class<? extends TypeCodec> value() default TypeCodec.class;

    Class keyClass() default Object.class;
    Class<? extends TypeCodec> serializer() default TypeCodec.class;
}
