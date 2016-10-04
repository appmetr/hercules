package com.appmetr.hercules.driver;

import com.appmetr.hercules.driver.serializer.RowSerializer;
import com.appmetr.hercules.profile.DataOperationsProfile;
import com.appmetr.hercules.wide.SliceDataSpecificator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.ddl.ComparatorType;

import java.util.List;
import java.util.Map;

public interface DataDriver {

    public static final int MAX_ROW_COUNT = 100000;
    public static final int MAX_TOP_COUNT = 100000;

    public static final int EMPTY_TTL = 0;

    Cluster getOrCreateCluster(String clusterName, String host, int maxActiveConnections);

    Cluster getOrCreateCluster(String clusterName, String host, int maxActiveConnections, long maxConnectTimeMillis, int cassandraThriftSocketTimeout);

    void shutdownCluster(Cluster cluster);

    Keyspace getOrCreateKeyspace(String keyspaceName, int replicationFactor, Cluster cluster);

    boolean checkAndCreateColumnFamily(Cluster cluster, String keyspaceName, String cfName, ComparatorType comparator, boolean awaitAgreement);

    boolean checkAndCreateColumnFamily(Cluster cluster, String keyspaceName, String cfName, ComparatorType comparator);

    boolean deleteColumnFamily(Cluster cluster, String keyspaceName, String cfName, boolean awaitAgreement);

    boolean deleteColumnFamily(Cluster cluster, String keyspaceName, String cfName);

    <T> Serializer<T> getSerializerForObject(Object obj);

    <T> Serializer<T> getSerializerForClass(Class clazz);

    <R, T> int getRowCount(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R from, R to, Integer count);

    <R, T> int getTopCount(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey, T from, T to, Integer count);

    <R, T> HerculesQueryResult<T> getRow(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey);

    <R, T> HerculesMultiQueryResult<R, T> getRows(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, Iterable<R> rowKeys);

    <R, T> HerculesMultiQueryResult<R, T> getAllRows(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer);


    <R, T> HerculesQueryResult<T> getSlice(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer,
                                           R rowKey, SliceDataSpecificator<T> sliceDataSpecificator);

    <R, T> HerculesMultiQueryResult<R, T> getSlice(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer,
                                                   Iterable<R> rowKeys, SliceDataSpecificator<T> sliceDataSpecificator);

    <R, T> HerculesMultiQueryResult<R, T> getRangeSlice(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer,
                                                        R rowFrom, R rowTo, Integer rowCount, SliceDataSpecificator<T> sliceDataSpecificator);

    <R, T> List<R> getKeyRange(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer,
                               R from, R to, Integer count);

    <R, T> void insert(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey, T topKey, Object value, int ttl);

    <R, T> void insert(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey, Map<T, Object> values, Map<T, Integer> ttls);

    <R, T> void insert(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey, Map<T, Object> values, int ttl);

    <R, T> void insert(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, Map<R, Map<T, Object>> values, Map<R, Map<T, Integer>> ttls);

    <R, T> void delete(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey);

    <R, T> void delete(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey, Iterable<T> topKeys);

    <R, T> void delete(Keyspace keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, Map<R, Iterable<T>> topKeysInRows);

}
