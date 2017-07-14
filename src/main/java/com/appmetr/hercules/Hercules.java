/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2013, AppMetr
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.appmetr.hercules;

import com.appmetr.hercules.annotations.Partitioned;
import com.appmetr.hercules.driver.DataDriver;
import com.appmetr.hercules.manager.EntityManager;
import com.appmetr.hercules.manager.IndexManager;
import com.appmetr.hercules.manager.WideEntityManager;
import com.appmetr.hercules.metadata.EntityMetadata;
import com.appmetr.hercules.metadata.EntityMetadataExtractor;
import com.appmetr.hercules.metadata.WideEntityMetadata;
import com.appmetr.hercules.metadata.WideEntityMetadataExtractor;
import com.appmetr.hercules.mutations.ExecutableMutation;
import com.appmetr.hercules.mutations.MutationsQueue;
import com.appmetr.hercules.partition.PartitioningStarter;
import com.datastax.driver.core.AbstractTableMetadata;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TypeCodec;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class Hercules {

    protected Logger logger = LoggerFactory.getLogger(Hercules.class);

    public static final int DEFAULT_BATCH_SIZE = 1000;

    @Inject HerculesConfig config;

    private Map<Class, EntityMetadata> entityClassMetadataCache = new HashMap<>();
    private Map<Class, WideEntityMetadata> wideEntityClassMetadataCache = new HashMap<>();

    @Inject EntityMetadataExtractor metadataExtractor;
    @Inject WideEntityMetadataExtractor wideMetadataExtractor;

    private Cluster cluster;
    private String keyspace;

    @Inject private Injector injector;
    @Inject private DataDriver dataDriver;
    @Inject private WideEntityManager wideEntityManager;
    @Inject private EntityManager entityManager;
    @Inject private IndexManager indexManager;
    @Inject private MutationsQueue mutationsQueue;
    @Inject private PartitioningStarter partitioningStarter;


    public void init() {
        cluster = dataDriver.getOrCreateCluster(
                config.getClusterName(),
                config.getCassandraHost(),
                config.getMaxActiveConnections(),
                config.getMaxConnectTimeMillis()
        );
        initCodecs();

        cluster.connect();
        keyspace = dataDriver.getOrCreateKeyspace(config.getKeyspaceName(), config.getReplicationFactor(), cluster);

        initEntities();

        new Thread(mutationsQueue).start();
        new Thread(partitioningStarter).start();
    }

    private void initCodecs() {
        for (TypeCodec typeCodec : config.getCodecs()) {
            cluster.getConfiguration().getCodecRegistry().register(typeCodec);
        }
    }

    public void shutdown() {
        partitioningStarter.stop();
        mutationsQueue.stop();

        dataDriver.shutdownCluster(cluster);
    }

    public Set<String> getColumnFamilies() {
        Collection<TableMetadata> tables = cluster.getMetadata().getKeyspace(getKeyspaceName()).getTables();
        return tables
                .stream()
                .map(AbstractTableMetadata::getName)
                .collect(Collectors.toSet());
    }

    public boolean checkAndCreateColumnFamily(String cfName) {
        return dataDriver.checkAndCreateColumnFamily(cluster, config.getKeyspaceName(), cfName);
    }

    public boolean deleteColumnFamily(String cfName) {
        return dataDriver.deleteColumnFamily(cluster, config.getKeyspaceName(), cfName);
    }

    private void initEntities() {
        initPlainEntities();
        initiateWideEntities();
    }

    private void initPlainEntities() {
        logger.info("Initializing plain entity classes...");

        checkAndCreateColumnFamily(EntityManager.PRIMARY_KEY_CF_NAME);

        for (Class<?> entityClass : config.getEntityClasses()) {
            logger.info("Extracting metadata for entity " + entityClass.getName());

            EntityMetadata metadata = metadataExtractor.extract(entityClass);
            entityClassMetadataCache.put(entityClass, metadata);

            checkAndCreateColumnFamily(metadata.getColumnFamily());

        }
        //We should have extracted metadata before create indexes
        for (EntityMetadata metadata : entityClassMetadataCache.values()) {
            indexManager.checkAndCreateEntityIndexes(metadata);
        }
    }

    private void initiateWideEntities() {
        logger.info("Initializing wide entity classes...");

        for (Class<?> wideEntityClass : config.getWideEntityClasses()) {
            logger.info("Extracting metadata for wide entity " + wideEntityClass.getName());

            WideEntityMetadata metadata = wideMetadataExtractor.extract(wideEntityClass);
            wideEntityClassMetadataCache.put(wideEntityClass, metadata);

            checkAndCreateColumnFamily(metadata.getColumnFamily());
        }
    }

    public EntityMetadata getMetadata(Class entityClass) {
        EntityMetadata meta = entityClassMetadataCache.get(entityClass);

        if (meta == null) {
            throw new RuntimeException("Can't find metadata for plain entity " + entityClass.getName());
        }

        return meta;
    }

    public WideEntityMetadata getWideMetadata(Class wideEntityClass) {
        WideEntityMetadata meta = wideEntityClassMetadataCache.get(wideEntityClass);

        if (meta == null) {
            throw new RuntimeException("Can't find metadata for wide entity " + wideEntityClass.getName());
        }

        return meta;
    }

    public Set<ExecutableMutation> getPartitionMutations() {
        final Set<String> columnFamilies = getColumnFamilies();

        Set<ExecutableMutation> mutations = new HashSet<>();

        for (final Class partitionEntityClass : config.getWideEntityClasses()) {
            if (!partitionEntityClass.isAnnotationPresent(Partitioned.class)) {
                continue;
            }

            for (String cfPartitionName : getWideEntityManager().getCFForPartitionCreation(partitionEntityClass)) {
                String cfFullName = getWideMetadata(partitionEntityClass).getColumnFamily() + cfPartitionName;

                if (!columnFamilies.contains(cfFullName)) {
                    mutations.add(new ExecutableMutation(ExecutableMutation.MutationType.CREATE, cfFullName) {
                        @Override public void execute() throws Exception {
                            checkAndCreateColumnFamily(getCfName());
                            columnFamilies.add(getCfName());
                            logger.info("Created partition: " + getCfName());
                        }

                        @Override public void skipped() {
                            logger.info("Skipped partition creation: " + getCfName());
                        }
                    });
                }
            }
        }

        return mutations;
    }

    /*Getters and Setters*/

    public Cluster getCluster() { return cluster; }

    public String getKeyspace() { return keyspace; }

    public String getKeyspaceName() { return config.getKeyspaceName(); }

    public String getCassandraHost() { return config.getCassandraHost(); }

    public int getReplicationFactor() { return config.getReplicationFactor(); }

    public boolean isSchemaModificationEnabled() { return config.isSchemaModificationEnabled(); }
    public boolean isSchemaModificationDisabled() { return !isSchemaModificationEnabled(); }

    public Injector getInjector() { return injector; }

    public DataDriver getDataDriver() { return dataDriver; }

    public EntityManager getEntityManager() { return entityManager; }

    public WideEntityManager getWideEntityManager() { return wideEntityManager; }

}
