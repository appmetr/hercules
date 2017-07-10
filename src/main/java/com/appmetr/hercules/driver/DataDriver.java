package com.appmetr.hercules.driver;

import com.appmetr.hercules.driver.serializer.RowSerializer;
import com.appmetr.hercules.profile.DataOperationsProfile;
import com.appmetr.hercules.wide.SliceDataSpecificator;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.TypeCodec;

import java.util.List;
import java.util.Map;

public interface DataDriver {

    int MAX_ROW_COUNT = 100000;
    int MAX_TOP_COUNT = 100000;

    int EMPTY_TTL = 0;

    Cluster getOrCreateCluster(String clusterName, String host, int maxActiveConnections);

    Cluster getOrCreateCluster(String clusterName, String host, int maxActiveConnections, int maxConnectTimeMillis);

    Cluster getOrCreateCluster(String clusterName, String host, int maxActiveConnections, long maxConnectTimeMillis, int cassandraThriftSocketTimeout, long maxWaitTimeWhenExhausted);

    void shutdownCluster(Cluster cluster);

    String getOrCreateKeyspace(String keyspaceName, int replicationFactor, Cluster cluster);

    boolean checkAndCreateColumnFamily(Cluster cluster, String keyspaceName, String cfName);

    boolean deleteColumnFamily(Cluster cluster, String keyspaceName, String cfName, boolean awaitAgreement);

    boolean deleteColumnFamily(Cluster cluster, String keyspaceName, String cfName);

    <T> TypeCodec<T> getSerializerForObject(Object obj);

    <T> TypeCodec<T> getSerializerForClass(Class clazz);

    <R, T> int getRowCount(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R from, R to, Integer count);

    <R, T> int getTopCount(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey, T from, T to, Integer count);

    <R, T> HerculesQueryResult<T> getRow(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey);

    <R, T> HerculesMultiQueryResult<R, T> getRows(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, Iterable<R> rowKeys);

    <R, T> HerculesMultiQueryResult<R, T> getAllRows(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer);


    <R, T> HerculesQueryResult<T> getSlice(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer,
                                           R rowKey, SliceDataSpecificator<T> sliceDataSpecificator);

    <R, T> HerculesMultiQueryResult<R, T> getSlice(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer,
                                                   Iterable<R> rowKeys, SliceDataSpecificator<T> sliceDataSpecificator);

    <R, T> HerculesMultiQueryResult<R, T> getRangeSlice(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer,
                                                        R rowFrom, R rowTo, Integer rowCount, SliceDataSpecificator<T> sliceDataSpecificator);

    <R, T> List<R> getKeyRange(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer,
                               R from, R to, Integer count);

    <R, T> void insert(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey, T topKey, Object value, int ttl);

    <R, T> void insert(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey, Map<T, Object> values, Map<T, Integer> ttls);

    <R, T> void insert(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey, Map<T, Object> values, int ttl);

    <R, T> void insert(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, Map<R, Map<T, Object>> values, Map<R, Map<T, Integer>> ttls);

    <R, T> void delete(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey);

    <R, T> void delete(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey, Iterable<T> topKeys);

    <R, T> void delete(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, Map<R, Iterable<T>> topKeysInRows);

}
