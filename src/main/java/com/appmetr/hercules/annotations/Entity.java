package com.appmetr.hercules.annotations;

import com.appmetr.hercules.annotations.comparator.EntityComparatorType;
import com.google.inject.BindingAnnotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Target({ElementType.TYPE, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@BindingAnnotation
public @interface Entity {
    String columnFamily() default "";
    EntityComparatorType comparatorType() default EntityComparatorType.UTF8TYPE;
}
