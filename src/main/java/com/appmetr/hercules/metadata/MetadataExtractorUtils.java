package com.appmetr.hercules.metadata;

import com.appmetr.hercules.annotations.Serializer;
import com.appmetr.hercules.annotations.comparator.EntityComparatorType;
import com.appmetr.hercules.annotations.listeners.*;
import me.prettyprint.hector.api.ddl.ComparatorType;

import java.lang.reflect.Method;

public class MetadataExtractorUtils {
    public static void setEntityComparatorType(Class<?> clazz, AbstractMetadata metadata, EntityComparatorType entityComparatorType) {
        if (entityComparatorType.equals(EntityComparatorType.ASCIITYPE)) {
            metadata.setComparatorType(ComparatorType.ASCIITYPE);
        } else if (entityComparatorType.equals(EntityComparatorType.BOOLEANTYPE)) {
            metadata.setComparatorType(ComparatorType.BOOLEANTYPE);
        } else if (entityComparatorType.equals(EntityComparatorType.BYTESTYPE)) {
            metadata.setComparatorType(ComparatorType.BYTESTYPE);
        } else if (entityComparatorType.equals(EntityComparatorType.DATETYPE)) {
            metadata.setComparatorType(ComparatorType.DATETYPE);
        } else if (entityComparatorType.equals(EntityComparatorType.DECIMALTYPE)) {
            metadata.setComparatorType(ComparatorType.DECIMALTYPE);
        } else if (entityComparatorType.equals(EntityComparatorType.FLOATTYPE)) {
            metadata.setComparatorType(ComparatorType.FLOATTYPE);
        } else if (entityComparatorType.equals(EntityComparatorType.INTEGERTYPE)) {
            metadata.setComparatorType(ComparatorType.INTEGERTYPE);
        } else if (entityComparatorType.equals(EntityComparatorType.INT32TYPE)) {
            metadata.setComparatorType(ComparatorType.INT32TYPE);
        } else if (entityComparatorType.equals(EntityComparatorType.LEXICALUUIDTYPE)) {
            metadata.setComparatorType(ComparatorType.LEXICALUUIDTYPE);
        } else if (entityComparatorType.equals(EntityComparatorType.LOCALBYPARTITIONERTYPE)) {
            metadata.setComparatorType(ComparatorType.LOCALBYPARTITIONERTYPE);
        } else if (entityComparatorType.equals(EntityComparatorType.LONGTYPE)) {
            metadata.setComparatorType(ComparatorType.LONGTYPE);
        } else if (entityComparatorType.equals(EntityComparatorType.TIMEUUIDTYPE)) {
            metadata.setComparatorType(ComparatorType.TIMEUUIDTYPE);
        } else if (entityComparatorType.equals(EntityComparatorType.UTF8TYPE)) {
            metadata.setComparatorType(ComparatorType.UTF8TYPE);
        } else if (entityComparatorType.equals(EntityComparatorType.COMPOSITETYPE)) {
            metadata.setComparatorType(ComparatorType.COMPOSITETYPE);
        } else if (entityComparatorType.equals(EntityComparatorType.DYNAMICCOMPOSITETYPE)) {
            metadata.setComparatorType(ComparatorType.DYNAMICCOMPOSITETYPE);
        } else if (entityComparatorType.equals(EntityComparatorType.UUIDTYPE)) {
            metadata.setComparatorType(ComparatorType.UUIDTYPE);
        } else if (entityComparatorType.equals(EntityComparatorType.COUNTERTYPE)) {
            metadata.setComparatorType(ComparatorType.COUNTERTYPE);
        } else if (entityComparatorType.equals(EntityComparatorType.REVERSEDTYPE)) {
            metadata.setComparatorType(ComparatorType.REVERSEDTYPE);
        } else {
            throw new RuntimeException("Bad ComparatorType for class " + clazz.getSimpleName());
        }
    }

    public static void setEntitySerializer(Class<?> clazz, AbstractMetadata metadata) {
        if (clazz.isAnnotationPresent(Serializer.class)) {
            Serializer serializerAnnotation = clazz.getAnnotation(Serializer.class);
            metadata.setEntitySerializer(serializerAnnotation.value());
        }
    }

    public static EntityListenerMetadata getListenerMetadata(Class clazz) {
        EntityListenerMetadata listenerMetadata = new EntityListenerMetadata();

        if (clazz.isAnnotationPresent(EntityListener.class)) {
            listenerMetadata.setEntityListenerClass(((EntityListener) clazz.getAnnotation(EntityListener.class)).value());

            setListenersFromClass(listenerMetadata.getEntityListenerClass(), listenerMetadata);
        } else {
            setListenersFromClass(clazz, listenerMetadata);
        }

        return listenerMetadata;
    }

    public static void setListenersFromClass(Class clazz, EntityListenerMetadata metadata) {

        for (Method method : clazz.getDeclaredMethods()) {
            if (method.isAnnotationPresent(PreLoad.class)) {
                if (metadata.getPreLoadMethod() != null) {
                    throw new RuntimeException("Multiply PreLoad method declaration in class " + clazz.getSimpleName());
                }

                metadata.setPreLoadMethod(method);
            }
            if (method.isAnnotationPresent(PostLoad.class)) {
                if (metadata.getPostLoadMethod() != null) {
                    throw new RuntimeException("Multiply PostLoad method declaration in class " + clazz.getSimpleName());
                }

                metadata.setPostLoadMethod(method);
            }
            if (method.isAnnotationPresent(PrePersist.class)) {
                if (metadata.getPrePersistMethod() != null) {
                    throw new RuntimeException("Multiply PrePersist method declaration in class " + clazz.getSimpleName());
                }


                metadata.setPrePersistMethod(method);
            }
            if (method.isAnnotationPresent(PostPersist.class)) {
                if (metadata.getPostPersistMethod() != null) {
                    throw new RuntimeException("Multiply PostPersist method declaration in class " + clazz.getSimpleName());
                }


                metadata.setPostPersistMethod(method);
            }
            if (method.isAnnotationPresent(PreDelete.class)) {
                if (metadata.getPreDeleteMethod() != null) {
                    throw new RuntimeException("Multiply PreDelete method declaration in class " + clazz.getSimpleName());
                }

                metadata.setPreDeleteMethod(method);
            }
            if (method.isAnnotationPresent(PostDelete.class)) {
                if (metadata.getPostDeleteMethod() != null) {
                    throw new RuntimeException("Multiply PostDelete method declaration in class " + clazz.getSimpleName());
                }

                metadata.setPostDeleteMethod(method);
            }
        }
    }
}
