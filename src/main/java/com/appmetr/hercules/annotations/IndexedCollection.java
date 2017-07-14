package com.appmetr.hercules.annotations;

import com.appmetr.hercules.keys.CollectionKeysExtractor;
import com.datastax.driver.core.TypeCodec;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface IndexedCollection {

    String name() default "";

    Class itemClass() default Object.class;

    Class<? extends TypeCodec> serializer() default TypeCodec.class;

    Class<? extends CollectionKeysExtractor> keyExtractorClass() default CollectionKeysExtractor.class;

}
