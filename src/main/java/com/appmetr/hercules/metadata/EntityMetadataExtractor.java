package com.appmetr.hercules.metadata;

import com.appmetr.hercules.HerculesConfig;
import com.appmetr.hercules.annotations.*;
import com.appmetr.hercules.driver.DataDriver;
import com.appmetr.hercules.keys.CollectionKeysExtractor;
import com.appmetr.hercules.keys.EntityCollectionKeyExtractor;
import com.appmetr.hercules.keys.ForeignKey;
import com.appmetr.hercules.keys.SerializableKeyCollectionKeyExtractor;
import com.appmetr.hercules.manager.EntityManager;
import com.appmetr.hercules.serializers.SerializerProvider;
import com.datastax.driver.core.TypeCodec;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.google.inject.Injector;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Clients of this class should call only extract() method.
 * Other methods visible(public) only for testing purposes.
 */
public class EntityMetadataExtractor {

    @Inject private HerculesConfig herculesConfig;
    @Inject private EntityManager em;
    @Inject private DataDriver dataDriver;
    @Inject private SerializerProvider serializerProvider;
    @Inject private Injector injector;


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

        Set<String> indexColumnFamilies = new HashSet<>();
        parseClassLevelMetadata(clazz, metadata, indexColumnFamilies);
        parseFieldLevelMetadata(clazz, metadata, indexColumnFamilies);

        return metadata;
    }

    private void parseClassLevelMetadata(Class<?> clazz, EntityMetadata metadata, Set<String> indexColumnFamilies) {
        Entity entityAnnotation = clazz.getAnnotation(Entity.class);
        metadata.setColumnFamily(entityAnnotation.columnFamily().length() == 0 ? clazz.getSimpleName() : entityAnnotation.columnFamily());
        metadata.setEntityClass(clazz);

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

            if (!primaryKeyAnnotation.serializer().equals(TypeCodec.class)) {
                primaryKeyMetadata.setSerializer(primaryKeyAnnotation.serializer());
            } else if (primaryKeyClass.isAnnotationPresent(Serializer.class)) {
                primaryKeyMetadata.setSerializer(((Serializer) primaryKeyClass.getAnnotation(Serializer.class)).value());
            }

            metadata.setPrimaryKeyMetadata(primaryKeyMetadata);
        }

        //Parse indexes metadata
        if (clazz.isAnnotationPresent(Index.class)) {
            Index indexAnnotation = clazz.getAnnotation(Index.class);
            indexColumnFamilies.add(parseFKMetadata(indexAnnotation, metadata));
        }

        if (clazz.isAnnotationPresent(Indexes.class)) {
            Indexes indexesAnnotation = clazz.getAnnotation(Indexes.class);
            Index[] indexAnnotations = indexesAnnotation.value();
            for (Index index : indexAnnotations) {
                String addedIndexName = parseFKMetadata(index, metadata);
                if (indexColumnFamilies.contains(addedIndexName)) {
                    throw new RuntimeException("Index configuration problem for class " + clazz.getName() + ". Several indexes with the same name present. Name = " + addedIndexName);
                }
                indexColumnFamilies.add(addedIndexName);
            }
        }
    }

    private void parseFieldLevelMetadata(Class<?> clazz, EntityMetadata metadata, Set<String> indexColumnFamilies) {
        for (Field field : clazz.getDeclaredFields()) {

            if (Modifier.isStatic(field.getModifiers())) continue;

            field.setAccessible(true);

            //primary key
            if (field.isAnnotationPresent(Id.class)) {
                if (field.isAnnotationPresent(Transient.class)) {
                    throw new RuntimeException("field annotated with @Id cannot be @Transient. field: [" + field.getName() + "] in class: " + clazz.getName());
                }
                if (field.isAnnotationPresent(GeneratedGUID.class)) {
                    metadata.setPrimaryKeyGenerated(true);
                }

                KeyMetadata primaryKeyMetadata = new KeyMetadata();
                Class primaryKeyClass = field.getType();

                primaryKeyMetadata.setField(field);
                primaryKeyMetadata.setKeyClass(primaryKeyClass);

                Id idAnnotation = field.getAnnotation(Id.class);
                if (!idAnnotation.serializer().equals(TypeCodec.class)) {
                    primaryKeyMetadata.setSerializer(idAnnotation.serializer());
                } else if (primaryKeyClass.isAnnotationPresent(Serializer.class)) {
                    primaryKeyMetadata.setSerializer(((Serializer) primaryKeyClass.getAnnotation(Serializer.class)).value());
                }

                metadata.setPrimaryKeyMetadata(primaryKeyMetadata);
            } else {

                if (!field.isAnnotationPresent(Transient.class)) {
                    // create column only for non Transient fields
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

                if (field.isAnnotationPresent(IndexedCollection.class)) {
                    String addedIndexCF = parseCollectionIndexMetadata(field, field.getAnnotation(IndexedCollection.class), metadata);
                    if (indexColumnFamilies.contains(addedIndexCF)) {
                        throw new RuntimeException("Incorrect @IndexedCollection usage for field [" + field.getName() + "] of class " + metadata.getEntityClass().getName() + " Several indexes with the same name present. Name=" + addedIndexCF);
                    }
                    indexColumnFamilies.add(addedIndexCF);
                }
            }
        }

        if (metadata.getFieldToColumn().size() == 0) {
            throw new RuntimeException("Entity " + clazz.getSimpleName() + " should contain at least one field different from Id");
        }
    }

    private String parseFKMetadata(Index indexAnnotation, EntityMetadata metadata) {
        Class<? extends ForeignKey> keyClass = indexAnnotation.keyClass();
        ForeignKeyMetadata keyMetadata = new ForeignKeyMetadata();

        String cfName = metadata.getColumnFamily() + "_" + keyClass.getSimpleName();
        keyMetadata.setColumnFamily(cfName);
        keyMetadata.setKeyClass(keyClass);

        Field[] declaredFields = keyClass.getDeclaredFields();
        for (Field field : declaredFields) {
            if (!field.isAnnotationPresent(Transient.class)) {
                field.setAccessible(true);

                keyMetadata.addKeyField(field);
            }
        }

        if (!indexAnnotation.serializer().equals(TypeCodec.class)) {
            keyMetadata.setSerializer(indexAnnotation.serializer());
        } else if (keyClass.isAnnotationPresent(Serializer.class)) {
            keyMetadata.setSerializer((keyClass.getAnnotation(Serializer.class)).value());
        } else {
            throw new RuntimeException("Could not find serializer for class \"" + keyClass.getSimpleName() + "\"");
        }

        metadata.addIndex(keyClass, keyMetadata);
        return cfName;
    }

    private String parseCollectionIndexMetadata(Field field, IndexedCollection indexAnnotation, EntityMetadata metadata) {

        CollectionIndexMetadata indexMetadata = new CollectionIndexMetadata();
        Class<?> itemClass = null;
        if (!Object.class.equals(indexAnnotation.itemClass())) {
            itemClass = indexAnnotation.itemClass();
        }
        Class<? extends TypeCodec> serializerClass = null;
        if (!TypeCodec.class.equals(indexAnnotation.serializer())) {
            serializerClass = indexAnnotation.serializer();
        }
        Class<? extends CollectionKeysExtractor> keyExtractorClass = null;
        if (!CollectionKeysExtractor.class.equals(indexAnnotation.keyExtractorClass())) {
            keyExtractorClass = indexAnnotation.keyExtractorClass();
        }
        if (keyExtractorClass != null) {
            if (serializerClass != null || itemClass != null) {
                throw new RuntimeException("Incorrect @IndexedCollection usage for field [" + field.getName() + "] of class " + metadata.getEntityClass().getName()
                        + ". If keyExtractorClass is specified, serializer and itemClass shouldn't be set");
            }
        } else {
            if (itemClass == null) {
                throw new RuntimeException("Incorrect @IndexedCollection usage for field [" + field.getName() + "] of class " + metadata.getEntityClass().getName()
                        + ". itemClass or keyExtractorClass should be specified");
            }
        }


        CollectionKeysExtractor keysExtractor;
        if (keyExtractorClass != null) {
            keysExtractor = injector.getInstance(keyExtractorClass);
        } else {
            if (!Collection.class.isAssignableFrom(field.getType())) {
                throw new RuntimeException("Incorrect @IndexedCollection usage for field [" + field.getName() + "] of class " + metadata.getEntityClass().getName()
                        + ". Indexed field should be a collection");
            }

            if (serializerClass != null) {
                //collection of objects with special serializer
                keysExtractor = new SerializableKeyCollectionKeyExtractor(field, serializerProvider.getSerializer(serializerClass, itemClass));
            } else {
                if (herculesConfig.getEntityClasses().contains(itemClass)) {
                    //collection of hercules entities
                    keysExtractor = new EntityCollectionKeyExtractor(field, itemClass, em);
                } else if (itemClass.getAnnotation(Serializer.class) != null) {
                    //collection of primary keys with serializers
                    Serializer s = itemClass.getAnnotation(Serializer.class);
                    keysExtractor = new SerializableKeyCollectionKeyExtractor(field, serializerProvider.getSerializer(s.value(), itemClass));
                } else {
                    //collection of well known objects with hector serializers
                    keysExtractor = new SerializableKeyCollectionKeyExtractor(field, serializerProvider.getSerializer(itemClass));
                }
            }
        }
        indexMetadata.setKeyExtractor(keysExtractor);

        indexMetadata.setIndexedField(field);
        String name = field.getName();
        if (!Strings.isNullOrEmpty(indexAnnotation.name())) {
            name = indexAnnotation.name();
        }
        name = metadata.getColumnFamily() + "_" + name;
        indexMetadata.setIndexColumnFamily(name);
        metadata.getCollectionIndexes().put(field.getName(), indexMetadata);
        return name;
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
