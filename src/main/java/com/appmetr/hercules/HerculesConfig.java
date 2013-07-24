package com.appmetr.hercules;

import java.util.Set;

public class HerculesConfig {
    /* Constants */
    private String keyspaceName;
    private String cassandraHost;
    private int replicationFactor;
    private Boolean schemaModificationEnabled;

    /* Fields */
    private Set<Class> entityClasses;
    private Set<Class> wideEntityClasses;

    public HerculesConfig() {
    }

    public HerculesConfig(String keyspaceName, String cassandraHost, int replicationFactor, Boolean schemaModificationEnabled, Set<Class> entityClasses, Set<Class> wideEntityClasses) {
        this.keyspaceName = keyspaceName;
        this.cassandraHost = cassandraHost;
        this.replicationFactor = replicationFactor;
        this.schemaModificationEnabled = schemaModificationEnabled;
        this.entityClasses = entityClasses;
        this.wideEntityClasses = wideEntityClasses;
    }

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

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public Boolean isSchemaModificationEnabled() {
        return schemaModificationEnabled;
    }

    public void setSchemaModificationEnabled(Boolean schemaModificationEnabled) {
        this.schemaModificationEnabled = schemaModificationEnabled;
    }

    public Set<Class> getEntityClasses() {
        return entityClasses;
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
}
