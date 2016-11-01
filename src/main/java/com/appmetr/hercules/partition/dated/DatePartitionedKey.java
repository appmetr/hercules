package com.appmetr.hercules.partition.dated;

public interface DatePartitionedKey<R>{
    R copyWithPartitionKey(long partitionKey);
}
