package com.appmetr.hercules;

import com.appmetr.hercules.annotations.Entity;
import com.appmetr.hercules.annotations.WideEntity;
import com.datastax.driver.core.TypeCodec;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class HerculesConfig {
    /* Constants */
    private String clusterName;
    private String keyspaceName;
    private String cassandraHost;
    private int maxActiveConnections;
    private int replicationFactor;
    private Boolean schemaModificationEnabled;
    private long maxConnectTimeMillis = -1;
    private int cassandraThriftSocketTimeout;
    private long maxWaitTimeWhenExhausted;

    /* Fields */
    private Set<Class> entityClasses = Collections.emptySet();
    private Set<Class> wideEntityClasses = Collections.emptySet();
    private Set<TypeCodec> codecs = Collections.emptySet();

    public HerculesConfig() {
    }

    public HerculesConfig(String clusterName, String keyspaceName, String cassandraHost, int maxActiveConnections, int replicationFactor, Boolean schemaModificationEnabled, Set<Class> entityClasses) {
        this.keyspaceName = clusterName;
        this.keyspaceName = keyspaceName;
        this.cassandraHost = cassandraHost;
        this.maxActiveConnections = maxActiveConnections;
        this.replicationFactor = replicationFactor;
        this.schemaModificationEnabled = schemaModificationEnabled;

        this.entityClasses = new HashSet<>();
        this.wideEntityClasses = new HashSet<>();

        for (Class entityClass : entityClasses) {
            if (entityClass.isAnnotationPresent(Entity.class)) {
                this.entityClasses.add(entityClass);
            } else if (entityClass.isAnnotationPresent(WideEntity.class)) {
                this.wideEntityClasses.add(entityClass);
            } else {
                throw new RuntimeException("Wrong class for entity " + entityClass.getSimpleName());
            }
        }
    }

    public String getClusterName() { return clusterName; }

    public void setClusterName(String clusterName) { this.clusterName = clusterName; }

    public String getKeyspaceName() {
        return keyspaceName;
    }

    public void setKeyspaceName(String keyspaceName) {
        this.keyspaceName = keyspaceName;
    }

    public String getCassandraHost() {
        return cassandraHost;
    }

    public void setCassandraHost(String cassandraHost) {
        this.cassandraHost = cassandraHost;
    }

    public int getMaxActiveConnections() {
        return maxActiveConnections;
    }

    public void setMaxActiveConnections(int maxActiveConnections) {
        this.maxActiveConnections = maxActiveConnections;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public Boolean isSchemaModificationEnabled() { return schemaModificationEnabled; }

    public void setSchemaModificationEnabled(Boolean schemaModificationEnabled) {
        this.schemaModificationEnabled = schemaModificationEnabled;
    }

    public Set<Class> getEntityClasses() {
        return entityClasses;
    }

    public Set<TypeCodec> getCodecs() {
        return codecs;
    }

    public void setCodecs(Set<TypeCodec> codecs) {
        this.codecs = codecs;
    }

    public void setEntityClasses(Set<Class> entityClasses) {
        this.entityClasses = entityClasses;
    }

    public Set<Class> getWideEntityClasses() {
        return wideEntityClasses;
    }

    public void setWideEntityClasses(Set<Class> wideEntityClasses) {
        this.wideEntityClasses = wideEntityClasses;
    }

    public long getMaxConnectTimeMillis() {
        return maxConnectTimeMillis;
    }

    public void setMaxConnectTimeMillis(long maxConnectTimeMillis) {
        this.maxConnectTimeMillis = maxConnectTimeMillis;
    }

    public int getCassandraThriftSocketTimeout() {
        return cassandraThriftSocketTimeout;
    }

    public void setCassandraThriftSocketTimeout(int cassandraThriftSocketTimeout) {
        this.cassandraThriftSocketTimeout = cassandraThriftSocketTimeout;
    }

    public long getMaxWaitTimeWhenExhausted() {
        return maxWaitTimeWhenExhausted;
    }

    public void setMaxWaitTimeWhenExhausted(long maxWaitTimeWhenExhausted) {
        this.maxWaitTimeWhenExhausted = maxWaitTimeWhenExhausted;
    }
}
