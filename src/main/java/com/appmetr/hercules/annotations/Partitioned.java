package com.appmetr.hercules.annotations;

import com.appmetr.hercules.partition.NoPartitionProvider;
import com.appmetr.hercules.partition.PartitionProvider;
import com.google.inject.BindingAnnotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@BindingAnnotation
public @interface Partitioned {
    Class<? extends PartitionProvider> value() default NoPartitionProvider.class;
}
