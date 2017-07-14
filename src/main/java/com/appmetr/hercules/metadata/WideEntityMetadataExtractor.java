package com.appmetr.hercules.metadata;

import com.appmetr.hercules.annotations.*;
import com.appmetr.hercules.partition.NoPartitionProvider;
import com.appmetr.hercules.partition.PartitionProvider;
import com.datastax.driver.core.TypeCodec;

import java.lang.reflect.Field;

public class WideEntityMetadataExtractor {
    public WideEntityMetadata extract(Class<?> clazz) {
        WideEntityMetadata metadata = new WideEntityMetadata();

        EntityAnnotationsValidator.isClassWideEntity(clazz);
        EntityAnnotationsValidator.validateWideEntitySerializer(clazz);

        parseClassLevelMetadata(clazz, metadata);
        parseFieldLevelMetadata(clazz, metadata);

        return metadata;
    }

    private void parseClassLevelMetadata(Class<?> clazz, WideEntityMetadata metadata) {
        metadata.setEntityClass(clazz);

        WideEntity wideEntityAnnotation = clazz.getAnnotation(WideEntity.class);
        metadata.setColumnFamily(wideEntityAnnotation.columnFamily().length() == 0 ? clazz.getSimpleName() : wideEntityAnnotation.columnFamily());

        MetadataExtractorUtils.setEntityComparatorType(clazz, metadata, wideEntityAnnotation.comparatorType());
        MetadataExtractorUtils.setEntitySerializer(clazz, metadata);
        MetadataExtractorUtils.setEntityTTL(clazz, metadata);

        if (metadata.getEntitySerializer() == null) {
            throw new RuntimeException("Wide entity " + clazz.getSimpleName() + " should contain " + Serializer.class.getSimpleName());
        }

        metadata.setListenerMetadata(MetadataExtractorUtils.getListenerMetadata(clazz));

        if (metadata.getListenerMetadata().getPreLoadMethod() != null) {
            throw new RuntimeException("Wide entity " + clazz.getSimpleName() + " shouldn't contain PreLoad method listener");
        }
        if (metadata.getListenerMetadata().getPreDeleteMethod() != null) {
            throw new RuntimeException("Wide entity " + clazz.getSimpleName() + " shouldn't contain PreDelete method listener");
        }
        if (metadata.getListenerMetadata().getPostDeleteMethod() != null) {
            throw new RuntimeException("Wide entity " + clazz.getSimpleName() + " shouldn't contain PostDelete method listener");
        }

        Class<? extends PartitionProvider> providerClass = null;
        if (clazz.isAnnotationPresent(Partitioned.class)) {
            Partitioned partitionedAnnotation = clazz.getAnnotation(Partitioned.class);

            if (!partitionedAnnotation.value().equals(NoPartitionProvider.class)) {
                providerClass = partitionedAnnotation.value();
            }
        }

        if (providerClass == null) {
            providerClass = NoPartitionProvider.class;
        }

        metadata.setPartitionProviderClass(providerClass);

        //Row key metadata
        if (clazz.isAnnotationPresent(RowKey.class)) {
            RowKey rowKeyAnnotation = clazz.getAnnotation(RowKey.class);
            KeyMetadata rowKeyMetadata = new KeyMetadata();

            if (rowKeyAnnotation.keyClass().equals(Object.class)) {
                throw new RuntimeException("Class level " + RowKey.class.getSimpleName() + " annotation for class " + clazz.getSimpleName() + " should contain keyClass field");
            }

            Class rowKeyClass = rowKeyAnnotation.keyClass();
            rowKeyMetadata.setKeyClass(rowKeyClass);

            if (!rowKeyAnnotation.serializer().equals(TypeCodec.class)) {
                rowKeyMetadata.setSerializer(rowKeyAnnotation.serializer());
            } else if (rowKeyClass.isAnnotationPresent(Serializer.class)) {
                rowKeyMetadata.setSerializer(((Serializer) rowKeyClass.getAnnotation(Serializer.class)).value());
            }

            metadata.setRowKeyMetadata(rowKeyMetadata);
        }

        //Top key metadata
        if (clazz.isAnnotationPresent(TopKey.class)) {
            TopKey topKeyAnnotation = clazz.getAnnotation(TopKey.class);
            KeyMetadata topKeyMetadata = new KeyMetadata();

            if (topKeyAnnotation.keyClass().equals(Object.class)) {
                throw new RuntimeException("Class level " + TopKey.class.getSimpleName() + " annotation for class " + clazz.getSimpleName() + " should contain keyClass field");
            }

            Class topKeyClass = topKeyAnnotation.keyClass();
            topKeyMetadata.setKeyClass(topKeyAnnotation.keyClass());

            if (!topKeyAnnotation.serializer().equals(TypeCodec.class)) {
                topKeyMetadata.setSerializer(topKeyAnnotation.serializer());
            } else if (topKeyClass.isAnnotationPresent(Serializer.class)) {
                topKeyMetadata.setSerializer(((Serializer) topKeyClass.getAnnotation(Serializer.class)).value());
            }

            metadata.setTopKeyMetadata(topKeyMetadata);
        }
    }

    private void parseFieldLevelMetadata(Class<?> clazz, WideEntityMetadata metadata) {
        for (Field field : clazz.getDeclaredFields()) {

            //Row key metadata
            if (field.isAnnotationPresent(RowKey.class)) {
                field.setAccessible(true);

                if (metadata.getRowKeyMetadata() != null) {
                    throw new RuntimeException(RowKey.class.getSimpleName() + " annotation " + clazz.getSimpleName() + "should be present only on field or class level");
                }

                KeyMetadata rowKeyMetadata = new KeyMetadata();

                Class rowKeyClass = field.getType();

                rowKeyMetadata.setField(field);
                rowKeyMetadata.setKeyClass(rowKeyClass);

                RowKey rowKeyAnnotation = field.getAnnotation(RowKey.class);
                if (!rowKeyAnnotation.serializer().equals(TypeCodec.class)) {
                    rowKeyMetadata.setSerializer(rowKeyAnnotation.serializer());
                } else if (rowKeyClass.isAnnotationPresent(Serializer.class)) {
                    rowKeyMetadata.setSerializer(((Serializer) rowKeyClass.getAnnotation(Serializer.class)).value());
                }

                metadata.setRowKeyMetadata(rowKeyMetadata);
            }

            //Top key metadata
            if (field.isAnnotationPresent(TopKey.class)) {
                field.setAccessible(true);

                if (metadata.getTopKeyMetadata() != null) {
                    throw new RuntimeException(TopKey.class.getSimpleName() + " annotation for class " + clazz.getSimpleName() + " should be present only on field or class level");
                }

                KeyMetadata topKeyMetadata = new KeyMetadata();

                Class topKeyClass = field.getType();

                topKeyMetadata.setField(field);
                topKeyMetadata.setKeyClass(field.getType());

                TopKey topKeyAnnotation = field.getAnnotation(TopKey.class);
                if (!topKeyAnnotation.serializer().equals(TypeCodec.class)) {
                    topKeyMetadata.setSerializer(topKeyAnnotation.serializer());
                } else if (topKeyClass.isAnnotationPresent(Serializer.class)) {
                    topKeyMetadata.setSerializer(((Serializer) topKeyClass.getAnnotation(Serializer.class)).value());
                }

                metadata.setTopKeyMetadata(topKeyMetadata);
            }
        }
    }
}
