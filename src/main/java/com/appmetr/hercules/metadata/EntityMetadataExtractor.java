package com.appmetr.hercules.metadata;

import com.appmetr.hercules.annotations.*;
import com.appmetr.hercules.keys.ForeignKey;
import com.appmetr.hercules.serializers.AbstractHerculesSerializer;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * Clients of this class should call only extract() method.
 * Other methods visible(public) only for testing purposes.
 */
public class EntityMetadataExtractor {
    /**
     * Use this method to extract metadata from entity class.
     *
     * @return extracted metadata.
     */
    public EntityMetadata extract(Class<?> clazz) {
        EntityMetadata metadata = new EntityMetadata();

        EntityAnnotationsValidator.isClassEntity(clazz);
        EntityAnnotationsValidator.validateThatOnlyOneIdPresent(clazz);
        EntityAnnotationsValidator.validateIndexes(clazz);

        parseClassLevelMetadata(clazz, metadata);
        parseFieldLevelMetadata(clazz, metadata);

        return metadata;
    }

    private void parseClassLevelMetadata(Class<?> clazz, EntityMetadata metadata) {
        Entity entityAnnotation = clazz.getAnnotation(Entity.class);
        metadata.setColumnFamily(entityAnnotation.columnFamily().length() == 0 ? clazz.getSimpleName() : entityAnnotation.columnFamily());
        metadata.setEntityClass(clazz);

        MetadataExtractorUtils.setEntityComparatorType(clazz, metadata, entityAnnotation.comparatorType());
        MetadataExtractorUtils.setEntitySerializer(clazz, metadata);
        MetadataExtractorUtils.setEntityTTL(clazz, metadata);
        metadata.setListenerMetadata(MetadataExtractorUtils.getListenerMetadata(clazz));

        //Parse primary key metadata
        metadata.setCreatePrimaryKeyIndex(clazz.isAnnotationPresent(PKIndex.class));

        if (clazz.isAnnotationPresent(Id.class)) {
            Id primaryKeyAnnotation = clazz.getAnnotation(Id.class);
            KeyMetadata primaryKeyMetadata = new KeyMetadata();

            if (primaryKeyAnnotation.keyClass().equals(Object.class)) {
                throw new RuntimeException("Class level " + Id.class.getSimpleName() + " annotation for class " + clazz.getSimpleName() + " should contain keyClass field");
            }

            Class primaryKeyClass = primaryKeyAnnotation.keyClass();
            primaryKeyMetadata.setKeyClass(primaryKeyClass);

            if (!primaryKeyAnnotation.serializer().equals(AbstractHerculesSerializer.class)) {
                primaryKeyMetadata.setSerializer(primaryKeyAnnotation.serializer());
            } else if (primaryKeyClass.isAnnotationPresent(Serializer.class)) {
                primaryKeyMetadata.setSerializer(((Serializer) primaryKeyClass.getAnnotation(Serializer.class)).value());
            }

            metadata.setPrimaryKeyMetadata(primaryKeyMetadata);
        }

        //Parse indexes metadata
        if (clazz.isAnnotationPresent(Index.class)) {
            Index indexAnnotation = clazz.getAnnotation(Index.class);
            parseFKMetadata(indexAnnotation, metadata);
        }

        if (clazz.isAnnotationPresent(Indexes.class)) {
            Indexes indexesAnnotation = clazz.getAnnotation(Indexes.class);
            Index[] indexAnnotations = indexesAnnotation.value();
            for (Index index : indexAnnotations) {
                parseFKMetadata(index, metadata);
            }
        }
    }

    private void parseFieldLevelMetadata(Class<?> clazz, EntityMetadata metadata) {
        for (Field field : clazz.getDeclaredFields()) {

            if (field.isAnnotationPresent(Transient.class)) continue;
            if (Modifier.isStatic(field.getModifiers())) continue;

            field.setAccessible(true);

            //primary key
            if (field.isAnnotationPresent(Id.class)) {
                if (field.isAnnotationPresent(GeneratedGUID.class)) {
                    metadata.setPrimaryKeyGenerated(true);
                }

                KeyMetadata primaryKeyMetadata = new KeyMetadata();
                Class primaryKeyClass = field.getType();

                primaryKeyMetadata.setField(field);
                primaryKeyMetadata.setKeyClass(primaryKeyClass);

                Id idAnnotation = field.getAnnotation(Id.class);
                if (!idAnnotation.serializer().equals(AbstractHerculesSerializer.class)) {
                    primaryKeyMetadata.setSerializer(idAnnotation.serializer());
                } else if (primaryKeyClass.isAnnotationPresent(Serializer.class)) {
                    primaryKeyMetadata.setSerializer(((Serializer) primaryKeyClass.getAnnotation(Serializer.class)).value());
                }

                metadata.setPrimaryKeyMetadata(primaryKeyMetadata);
            } else {
                String columnName = getColumnName(field);
                metadata.setColumnClass(columnName, field.getType());
                metadata.setFieldColumn(field, columnName);

                if (field.isAnnotationPresent(Serializer.class)) {
                    metadata.setColumnSerializer(columnName, field.getAnnotation(Serializer.class).value());
                }

                if (field.isAnnotationPresent(NotNullField.class)) {
                    metadata.addNotNullColumn(columnName);
                }
            }
        }

        if (metadata.getFieldToColumn().size() == 0) {
            throw new RuntimeException("Entity " + clazz.getSimpleName() + " should contain at least one field different from Id");
        }
    }

    private void parseFKMetadata(Index indexAnnotation, EntityMetadata metadata) {
        Class<? extends ForeignKey> keyClass = indexAnnotation.keyClass();
        ForeignKeyMetadata keyMetadata = new ForeignKeyMetadata();

        keyMetadata.setColumnFamily(metadata.getColumnFamily() + "_" + keyClass.getSimpleName());
        keyMetadata.setKeyClass(keyClass);

        Field[] declaredFields = keyClass.getDeclaredFields();
        for (Field field : declaredFields) {
            if (!field.isAnnotationPresent(Transient.class)) {
                field.setAccessible(true);

                keyMetadata.addKeyField(field);
            }
        }

        if (!indexAnnotation.serializer().equals(AbstractHerculesSerializer.class)) {
            keyMetadata.setSerializer(indexAnnotation.serializer());
        } else if (keyClass.isAnnotationPresent(Serializer.class)) {
            keyMetadata.setSerializer((keyClass.getAnnotation(Serializer.class)).value());
        } else {
            throw new RuntimeException("Could not find serializer for class \"" + keyClass.getSimpleName() + "\"");
        }

        metadata.addIndex(keyClass, keyMetadata);
    }

    private String getColumnName(Field field) {
        Column column = field.getAnnotation(Column.class);
        if (column != null) {
            return column.name();
        } else {
            return field.getName();
        }
    }
}
