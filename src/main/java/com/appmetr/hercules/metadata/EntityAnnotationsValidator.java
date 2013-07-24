package com.appmetr.hercules.metadata;

import com.appmetr.hercules.annotations.*;

import java.lang.reflect.Field;

public class EntityAnnotationsValidator {
    public static void isClassEntity(Class<?> entityClass) {
        if (!entityClass.isAnnotationPresent(Entity.class)) {
            throw new RuntimeException("Entity must be annotated with " + Entity.class.getName() + ". Entity class:" + entityClass.getName());
        }
    }

    public static void isClassWideEntity(Class<?> entityClass) {
        if (!entityClass.isAnnotationPresent(WideEntity.class)) {
            throw new RuntimeException("Wide entity must be annotated with " + WideEntity.class.getName() + ". Entity class:" + entityClass.getName());
        }
    }

    public static void validateThatOnlyOneIdPresent(Class clazz) {
        boolean isIdAnnotationFound = false;

        if (clazz.isAnnotationPresent(Id.class)) {
            isIdAnnotationFound = true;
        }

        for (Field field : clazz.getDeclaredFields()) {
            if (field.isAnnotationPresent(Id.class)) {
                if (isIdAnnotationFound) {
                    throw new RuntimeException(Id.class.getSimpleName() + " annotation for class " + clazz.getSimpleName() + " should be present only on field or class level");
                }
                isIdAnnotationFound = true;
            }
        }

        if (!isIdAnnotationFound) {
            throw new RuntimeException("One field must be annotated with " + Id.class.getName() + ". Entity class:" + clazz.getName());
        }
    }

    public static void validateIndexes(Class clazz) {
        if (clazz.isAnnotationPresent(Index.class) && clazz.isAnnotationPresent(Indexes.class)) {
            throw new RuntimeException("Class can not use both: " + Index.class.getSimpleName() + " and "
                    + Indexes.class.getSimpleName() + " annotations. Class: " + clazz.getSimpleName());
        }
    }

    public static void validateWideEntitySerializer(Class<?> clazz) {
        if (!clazz.isAnnotationPresent(Serializer.class)) {
            throw new RuntimeException("Class " + clazz.getSimpleName() + " must be declared with " + Serializer.class.getSimpleName() + " annotation");
        }
    }

}
