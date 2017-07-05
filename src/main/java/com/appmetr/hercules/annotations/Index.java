package com.appmetr.hercules.annotations;

import com.appmetr.hercules.keys.ForeignKey;
import com.datastax.driver.core.TypeCodec;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Index {
    Class<? extends ForeignKey> keyClass();

    Class<? extends TypeCodec> serializer() default TypeCodec.class;
}
