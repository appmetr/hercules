package com.appmetr.hercules.metadata;

import com.appmetr.hercules.partition.PartitionProvider;

public class WideEntityMetadata extends AbstractMetadata {
    private Class<? extends PartitionProvider> partitionProviderClass;
    private PartitionProvider partitionProvider;

    private KeyMetadata rowKeyMetadata;
    private KeyMetadata topKeyMetadata;

    public Class<? extends PartitionProvider> getPartitionProviderClass() {
        return partitionProviderClass;
    }

    public void setPartitionProviderClass(Class<? extends PartitionProvider> partitionProviderClass) {
        this.partitionProviderClass = partitionProviderClass;
    }

    public PartitionProvider getPartitionProvider() {
        return partitionProvider;
    }

    public void setPartitionProvider(PartitionProvider partitionProvider) {
        this.partitionProvider = partitionProvider;
    }

    public KeyMetadata getRowKeyMetadata() {
        return rowKeyMetadata;
    }

    public void setRowKeyMetadata(KeyMetadata rowKeyMetadata) {
        this.rowKeyMetadata = rowKeyMetadata;
    }

    public KeyMetadata getTopKeyMetadata() {
        return topKeyMetadata;
    }

    public void setTopKeyMetadata(KeyMetadata topKey) {
        this.topKeyMetadata = topKey;
    }
}
