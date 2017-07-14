package com.appmetr.hercules.driver;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.HerculesMonitoringGroup;
import com.appmetr.hercules.driver.serializer.RowSerializer;
import com.appmetr.hercules.profile.DataOperationsProfile;
import com.appmetr.hercules.serializers.SerializerProvider;
import com.appmetr.hercules.wide.SliceDataSpecificator;
import com.appmetr.monblank.Monitoring;
import com.appmetr.monblank.StopWatch;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.*;
import com.datastax.driver.core.schemabuilder.CreateKeyspace;
import com.datastax.driver.core.schemabuilder.Drop;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.datastax.driver.core.ConsistencyLevel.QUORUM;
import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static com.datastax.driver.core.schemabuilder.SchemaBuilder.createTable;
import static com.datastax.driver.core.schemabuilder.SchemaBuilder.dropTable;
import static java.util.Collections.singletonList;

public class CqlDataDriver implements DataDriver {

    @Inject private Monitoring monitoring;
    @Inject private Hercules hercules;
    @Inject private SerializerProvider serializerProvider;
    private Logger logger = LoggerFactory.getLogger(CqlDataDriver.class);

    public Cluster getOrCreateCluster(String clusterName, String host, int maxActiveConnections) {
        return getOrCreateCluster(clusterName, host, maxActiveConnections, 10_000);
    }

    public Cluster getOrCreateCluster(String clusterName,
                                      String host,
                                      int maxActiveConnections,
                                      int maxConnectTimeMillis) {
        return Cluster.builder()
                .addContactPoint(host)
                .withQueryOptions(new QueryOptions().setFetchSize(1000))
                .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(maxConnectTimeMillis))
                .withClusterName(clusterName)
                .build();
    }

    public void shutdownCluster(Cluster cluster) {
        cluster.close();
    }

    public String getOrCreateKeyspace(String keyspaceName,
                                      int replicationFactor,
                                      Cluster cluster) {
        Map<String, Object> replication = new HashMap<>();
        replication.put("class", "SimpleStrategy");
        replication.put("replication_factor", replicationFactor);

        Statement statement = new CreateKeyspace(keyspaceName)
                .ifNotExists()
                .with()
                .replication(replication)
                .setConsistencyLevel(QUORUM)
                .setSerialConsistencyLevel(ConsistencyLevel.SERIAL);

        execute(session -> session.execute(statement));
        return keyspaceName;
    }

    @Override public boolean checkAndCreateColumnFamily(Cluster cluster,
                                                        String keyspaceName,
                                                        String cfName) {
        if (hercules.isSchemaModificationDisabled()) return false;

        KeyspaceMetadata keyspace = cluster.getMetadata().getKeyspace(keyspaceName);

        if (shouldNotCreate(quote(cfName), keyspace)) return false;

        logger.info("Create column family {} in keyspace {}", cfName, keyspaceName);

        MutableBoolean created = new MutableBoolean(false);
        Statement table = createTable(keyspaceName, quote(cfName))
                .ifNotExists()
                .addPartitionKey("key", DataType.blob())
                .addClusteringColumn("column1", DataType.text())
                .addColumn("value", DataType.blob())
                .withOptions()
                .compactStorage();

        execute(session -> session.execute(table), rc -> created.setTrue());

        return created.booleanValue();
    }

    private boolean shouldNotCreate(String cfName,
                                    KeyspaceMetadata keyspace) {
        if (keyspace != null) {
            boolean found = keyspace.getTables()
                    .stream()
                    .map(AbstractTableMetadata::getName)
                    .anyMatch(s -> s.equals(cfName));

            if (found) {
                return true;
            }
        } else {
            return true;
        }
        return false;
    }

    public boolean deleteColumnFamily(Cluster cluster,
                                      String keyspaceName,
                                      String cfName,
                                      boolean awaitAgreement) {
        if (!hercules.isSchemaModificationEnabled()) return false;

        Drop dropCF = dropTable(keyspaceName, quote(cfName)).ifExists();

        MutableBoolean deleted = new MutableBoolean(false);

        execute(session -> session.execute(dropCF), rs -> deleted.setTrue());
        return deleted.booleanValue();
    }

    public boolean deleteColumnFamily(Cluster cluster,
                                      String keyspaceName,
                                      String cfName) {
        return deleteColumnFamily(cluster, keyspaceName, cfName, true);
    }

    public <R, T> int getRowCount(String keyspace,
                                  String columnFamily,
                                  DataOperationsProfile profile,
                                  RowSerializer<R, T> rowSerializer,
                                  R from,
                                  R to,
                                  Integer count) {
        Select select = select().from(keyspace, quote(columnFamily));
        if (from != null) {
            select.where(gte(token(primaryKey(keyspace, columnFamily)), token(from)));
        }
        if (to != null) {
            select.where(lte(token(primaryKey(keyspace, columnFamily)), token(to)));
        }
        MutableInt totalRows = new MutableInt();

        execute(executeAndProfile(
                select,
                monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Get row count " + columnFamily),
                profile),
                rs -> totalRows.add(rs.one().get("count", Integer.class)));
        return totalRows.intValue();
    }

    public <R, T> int getTopCount(String keyspace,
                                  String columnFamily,
                                  DataOperationsProfile dataOperationsProfile,
                                  RowSerializer<R, T> rowSerializer,
                                  R rowKey,
                                  T from,
                                  T to,
                                  Integer count) {
        throw new UnsupportedOperationException("not used in appmetr");
    }

    public <R, T> HerculesQueryResult<T> getRow(String keyspace,
                                                String columnFamily,
                                                DataOperationsProfile profile,
                                                RowSerializer<R, T> rowSerializer,
                                                R rowKey) {
        String primaryKey = primaryKey(keyspace, columnFamily);
        String topKey = topKey(keyspace, columnFamily);

        Statement stmt = select()
                .from(keyspace, quote(columnFamily))
                .where(eq(primaryKey, rowSerializer.getRowKeySerializer().serialize(rowKey, protocol())));

        HerculesMultiQueryResult<R, T> results = new HerculesMultiQueryResult<>();
        execute(executeAndProfile(stmt,
                monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Get slice " + columnFamily),
                profile),
                rs -> buildQueryResult(
                        primaryKey,
                        topKey,
                        rs,
                        Integer.MAX_VALUE,
                        rowSerializer,
                        results,
                        profile));

        //todo: refactor
        HerculesQueryResult<T> result = new HerculesQueryResult<>();
        LinkedHashMap<T, Object> entries = results.getEntries().get(rowKey);
        if (entries != null) {
            result.setEntries(entries);
        }
        return result;
    }

    public <R, T> HerculesMultiQueryResult<R, T> getRows(String keyspace,
                                                         String columnFamily,
                                                         DataOperationsProfile profile,
                                                         RowSerializer<R, T> rowSerializer,
                                                         Iterable<R> rowKeys) {
        String primaryKey = primaryKey(keyspace, columnFamily);
        String topKey = topKey(keyspace, columnFamily);

        List<ByteBuffer> serializedKeys = serializedKeys(rowKeys, rowSerializer.getRowKeySerializer());

        HerculesMultiQueryResult<R, T> result = new HerculesMultiQueryResult<>();
        for (List<ByteBuffer> chunk : Iterables.partition(serializedKeys, 100)) {
            Statement stmt = select()
                    .from(keyspace, quote(columnFamily))
                    .where(in(primaryKey, chunk));

            //
            execute(executeAndProfile(
                    stmt,
                    monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Get slice " + columnFamily),
                    profile),
                    rs -> buildQueryResult(
                            primaryKey,
                            topKey,
                            rs,
                            Integer.MAX_VALUE,
                            rowSerializer,
                            result,
                            profile));
        }

        return result;
    }

    public <R, T> HerculesMultiQueryResult<R, T> getAllRows(String keyspace,
                                                            String columnFamily,
                                                            DataOperationsProfile profile,
                                                            RowSerializer<R, T> rowSerializer) {
        Select stmt = select().from(keyspace, quote(columnFamily));

        HerculesMultiQueryResult<R, T> results = new HerculesMultiQueryResult<>();

        execute(executeAndProfile(stmt,
                monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Get range slice " + columnFamily),
                profile),
                rs -> buildQueryResult(
                        primaryKey(keyspace, columnFamily),
                        topKey(keyspace, columnFamily),
                        rs, Integer.MAX_VALUE,
                        rowSerializer,
                        results,
                        profile));

        return results;
    }

    public <R, T> HerculesQueryResult<T> getSlice(String keyspace,
                                                  String columnFamily,
                                                  DataOperationsProfile profile,
                                                  RowSerializer<R, T> rowSerializer,
                                                  R rowKey,
                                                  SliceDataSpecificator<T> sliceDataSpecificator) {
        HerculesMultiQueryResult<R, T> queryResult = getSlice(keyspace, columnFamily, profile, rowSerializer, singletonList(rowKey), sliceDataSpecificator);

        if (queryResult.hasResult() && queryResult.containsKey(rowKey)) {
            return new HerculesQueryResult<>(queryResult.getEntries().get(rowKey));
        }

        return new HerculesQueryResult<>();
    }

    public <R, T> HerculesMultiQueryResult<R, T> getSlice(String keyspace,
                                                          String columnFamily,
                                                          DataOperationsProfile profile,
                                                          RowSerializer<R, T> rowSerializer,
                                                          Iterable<R> rowKeys,
                                                          SliceDataSpecificator<T> sliceDataSpecificator) {

        String primaryKey = primaryKey(keyspace, columnFamily);
        String topKey = topKey(keyspace, columnFamily);

        List<ByteBuffer> serializedKeys = serializedKeys(rowKeys, rowSerializer.getRowKeySerializer());

        HerculesMultiQueryResult<R, T> result = new HerculesMultiQueryResult<>();

        for (List<ByteBuffer> chunk : Iterables.partition(serializedKeys, 100)) {
            Select select = select().from(keyspace, quote(columnFamily));
            select.where(in(primaryKey, chunk));


            execute(executeAndProfile(
                    select,
                    monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Get range slice " + columnFamily),
                    profile),
                    rs -> buildQueryResult(
                            primaryKey,
                            topKey,
                            rs,
                            Integer.MAX_VALUE,
                            rowSerializer,
                            result,
                            profile));

        }
        return result;
    }

    private <R> ByteBuffer serializedKey(R rowKey, TypeCodec<R> serializer) {
        return serializer.serialize(rowKey, protocol());
    }

    private <R> List<ByteBuffer> serializedKeys(Iterable<R> rowKeys, TypeCodec<R> serializer) {
        List<R> keys = new ArrayList<>();
        rowKeys.forEach(keys::add);
        return keys
                .stream()
                .map(v -> serializer.serialize(v, protocol()))
                .collect(Collectors.toList());
    }

    public <R, T> HerculesMultiQueryResult<R, T> getRangeSlice(String keyspace, String columnFamily,
                                                               DataOperationsProfile profile,
                                                               RowSerializer<R, T> rowSerializer,
                                                               R rowFrom,
                                                               R rowTo,
                                                               Integer rowCount,
                                                               SliceDataSpecificator<T> sliceDataSpecificator) {

        String primaryKey = primaryKey(keyspace, columnFamily);
        String topKey = topKey(keyspace, columnFamily);

        Select select = select().from(keyspace, quote(columnFamily));
        if (rowFrom != null) {
            select.where(gte(token(primaryKey), token(rowFrom)));
        }
        if (rowTo != null) {
            select.where(lte(token(primaryKey), token(rowTo)));
        }
        //
        HerculesMultiQueryResult<R, T> result = new HerculesMultiQueryResult<>();

        execute(executeAndProfile(
                select,
                monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Get range slice " + columnFamily),
                profile),
                rs -> buildQueryResult(
                        primaryKey,
                        topKey,
                        rs,
                        rowCount,
                        rowSerializer,
                        result,
                        profile));

        return result;
    }

    public <R, T> List<R> getKeyRange(String keyspace,
                                      String columnFamily,
                                      DataOperationsProfile profile,
                                      RowSerializer<R, T> rowSerializer,
                                      R from,
                                      R to,
                                      Integer count) {
        String primaryKey = primaryKey(keyspace, columnFamily);

        Select select = select().from(keyspace, quote(columnFamily));

        List<R> keys = new ArrayList<>();
        execute(executeAndProfile(
                select,
                monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Get key range " + columnFamily),
                profile)
                ,
                rs -> buildQueryResult(
                        rs,
                        count,
                        row -> {
                            ByteBuffer primaryKeyValue = row.get(primaryKey, codecForColumn(rs.getColumnDefinitions().getType(primaryKey)));
                            keys.add(rowSerializer.getRowKeySerializer().deserialize(primaryKeyValue, protocol()));
                        }));
        return keys;
    }

    public <R, T> void insert(String keyspace,
                              String columnFamily,
                              DataOperationsProfile profile,
                              RowSerializer<R, T> rowSerializer,
                              R rowKeyValue,
                              T topKeyValue,
                              Object value,
                              int ttl) {
        MutableInt serializedDataSize = new MutableInt(0);


        Statement stmt;
        String primaryKeyName = primaryKey(keyspace, columnFamily);
        String topKeyName = topKey(keyspace, columnFamily);
        if (value == null) {
            stmt = QueryBuilder.delete()
                    .from(keyspace, quote(columnFamily))
                    .where(eq(primaryKeyName, serializedKey(rowKeyValue, rowSerializer.getRowKeySerializer())));

        } else {
            TypeCodec serializer = rowSerializer.hasValueSerializer(topKeyValue) ? rowSerializer.getValueSerializer(topKeyValue) : serializerProvider.getSerializer(value);
            ByteBuffer serializedValue = serializer.serialize(value, protocol());
            serializedDataSize.add(serializedValue.remaining());
            stmt = insertInto(keyspace, quote(columnFamily))
                    .value(primaryKeyName, serializedKey(rowKeyValue, rowSerializer.getRowKeySerializer()))
                    .value(topKeyName, serializedKey(topKeyValue, rowSerializer.getTopKeySerializer()))
                    .value(quote("value"), serializedValue)
                    .using(ttl(ttl));
        }
        profile.bytes += serializedDataSize.intValue();
        executeAndProfile(
                stmt,
                monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Insert " + columnFamily),
                profile);

    }

    public <R, T> void insert(String keyspace, String columnFamily, DataOperationsProfile dataOperationsProfile, RowSerializer<R, T> rowSerializer, R rowKey, Map<T, Object> values, Map<T, Integer> ttls) {
        Map<R, Map<T, Object>> valuesToInsert = Collections.singletonMap(rowKey, values);
        insert(keyspace, columnFamily, dataOperationsProfile, rowSerializer, valuesToInsert, (row, top) -> {
            if (ttls == null) {
                return DataDriver.EMPTY_TTL;
            }
            return ttls.getOrDefault(top, DataDriver.EMPTY_TTL);
        });
    }

    public <R, T> void insert(String keyspace,
                              String columnFamily,
                              DataOperationsProfile dataOperationsProfile,
                              RowSerializer<R, T> rowSerializer,
                              R rowKey,
                              Map<T, Object> values,
                              int ttl) {
        Map<R, Map<T, Object>> valuesToInsert = Collections.singletonMap(rowKey, values);

        insert(keyspace, columnFamily, dataOperationsProfile, rowSerializer, valuesToInsert, (row, top) -> ttl);
    }

    public <R, T> void insert(String keyspace, String columnFamily,
                              DataOperationsProfile dataOperationsProfile,
                              RowSerializer<R, T> rowSerializer,
                              Map<R, Map<T, Object>> values,
                              Map<R, Map<T, Integer>> ttls) {
        insert(keyspace, columnFamily, dataOperationsProfile, rowSerializer, values, (row, top) -> {
            if (ttls == null) {
                return DataDriver.EMPTY_TTL;
            }
            return ttls
                    .getOrDefault(row, Collections.emptyMap())
                    .getOrDefault(top, DataDriver.EMPTY_TTL);
        });
    }

    private <R, T> void insert(String keyspace, String columnFamily,
                               DataOperationsProfile profile,
                               RowSerializer<R, T> rowSerializer,
                               Map<R, Map<T, Object>> values,
                               TTLProvider<R, T> ttlProvider) {

        MutableInt serializedDataSize = new MutableInt(0);
        for (R rowKey : values.keySet()) {
            Batch batch = batch();
            for (Map.Entry<T, Object> entry : values.get(rowKey).entrySet()) {
                T topKey = entry.getKey();
                Object value = entry.getValue();

                if (value == null) {
                    Delete.Where delete = QueryBuilder
                            .delete()
                            .from(keyspace, quote(columnFamily))
                            .where(eq(primaryKey(keyspace, columnFamily), serializedKey(rowKey, rowSerializer.getRowKeySerializer())))
                            .and(eq(topKey(keyspace, columnFamily), serializedKey(topKey, rowSerializer.getTopKeySerializer())));
                    batch.add(delete);
                } else {
                    ByteBuffer serializedValue = serializedValue(rowSerializer, topKey, value);
                    serializedDataSize.add(serializedValue.remaining());
                    Insert insert =
                            insertInto(keyspace, quote(columnFamily))
                                    .value(primaryKey(keyspace, columnFamily), serializedKey(rowKey, rowSerializer.getRowKeySerializer()))
                                    .value(topKey(keyspace, columnFamily), serializedKey(topKey, rowSerializer.getTopKeySerializer()))
                                    .value(quote("value"), serializedValue);

                    int ttl = ttlValue(ttlProvider, rowKey, topKey);

                    if (ttl > 0) {
                        insert.using(ttl(ttl));
                    }

                    batch.add(insert);
                }
            }
            profile.bytes += serializedDataSize.intValue();
            executeAndProfile(
                    batch,
                    monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Insert " + columnFamily),
                    profile
            );
        }
    }

    public <R, T> void delete(String keyspace, String columnFamily, DataOperationsProfile profile, RowSerializer<R, T> rowSerializer, R rowKey) {
        Statement delete = QueryBuilder
                .delete()
                .from(keyspace, quote(columnFamily))
                .where(eq(primaryKey(keyspace, columnFamily), serializedKey(rowKey, rowSerializer.getRowKeySerializer())));
        delete(delete, columnFamily, profile);
    }

    public <R, T> void delete(String keyspace, String columnFamily,
                              DataOperationsProfile dataOperationsProfile,
                              RowSerializer<R, T> rowSerializer,
                              R rowKey,
                              Iterable<T> topKeys) {
        delete(keyspace, columnFamily, dataOperationsProfile, rowSerializer, Collections.singletonMap(rowKey, topKeys));
    }

    public <R, T> void delete(String keyspace, String columnFamily,
                              DataOperationsProfile dataOperationsProfile,
                              RowSerializer<R, T> rowSerializer,
                              Map<R, Iterable<T>> topKeysInRows) {
        String primaryKeyName = primaryKey(keyspace, columnFamily);
        String topKeyName = topKey(keyspace, columnFamily);

        for (R rowKey : topKeysInRows.keySet()) {
            Statement delete = QueryBuilder
                    .delete()
                    .from(keyspace, quote(columnFamily))
                    .where(eq(primaryKeyName, serializedKey(rowKey, rowSerializer.getRowKeySerializer())))
                    .and(in(topKeyName, serializedKeys(topKeysInRows.get(rowKey), rowSerializer.getTopKeySerializer())));
            delete(delete, columnFamily, dataOperationsProfile);
        }
    }

    private void delete(Statement stmt, String columnFamily, DataOperationsProfile profile) {
        execute(executeAndProfile(
                stmt,
                monitoring.start(HerculesMonitoringGroup.HERCULES_DD, "Delete " + columnFamily),
                profile));
    }

    private <R, T> ByteBuffer serializedValue(RowSerializer<R, T> rowSerializer, T topKey, Object value) {
        return rowSerializer.getValueSerializer(topKey).serialize(value, protocol());
    }

    private void execute(Function<Session, ResultSet> f) {
        execute(f, rs -> {});
    }

    private void execute(Function<Session, ResultSet> f, Consumer<ResultSet> consumer) {
        try (Session session = hercules.getCluster().newSession()) {
            ResultSet rs = f.apply(session);
            consumer.accept(rs);
        } catch (Exception e) {
            logger.error("cannot query and build results", e);
        }
    }

    private void buildQueryResult(ResultSet rows, Integer rowCount, Consumer<Row> consumer) {
        int c = 0;
        for (Row row : rows) {
            prefetch(rows);
            if (c > rowCount) {
                break;
            }
            consumer.accept(row);
        }
    }

    private void prefetch(ResultSet rows) {
        if (rows.getAvailableWithoutFetching() == 100 && !rows.isFullyFetched())
            rows.fetchMoreResults(); // this is asynchronous
    }

    private <R, T> void buildQueryResult(final String primaryKey,
                                         final String topKey,
                                         ResultSet rows,
                                         Integer rowCount,
                                         RowSerializer<R, T> rowSerializer,
                                         HerculesMultiQueryResult<R, T> results,
                                         DataOperationsProfile dataOperationsProfile) {
        LinkedHashMap<R, LinkedHashMap<T, Object>> result = new LinkedHashMap<>();  // row key -> ( column key value -> column value)
        ColumnDefinitions columnDefinitions = rows.getColumnDefinitions();

        R lastKey = null;

        for (Row row : rows) {
            prefetch(rows);
            T columnName = null;
            Object columnValue = null;
            LinkedHashMap<T, Object> valueMap = new LinkedHashMap<>();

            ByteBuffer primaryKeyValue = row.get(primaryKey, codecForColumn(columnDefinitions.getType(primaryKey)));
            lastKey = rowSerializer.getRowKeySerializer().deserialize(primaryKeyValue, protocol());
            T topKeyName = row.get(topKey, codecForColumn(columnDefinitions.getType(topKey)));
            columnName = topKeyName instanceof ByteBuffer ?
                    rowSerializer.getTopKeySerializer().deserialize((ByteBuffer) topKeyName, protocol())
                    : topKeyName;

            ByteBuffer value = row.get("value", TypeCodec.blob());
            if (rowSerializer.hasValueSerializer(columnName)) {
                TypeCodec valueSerializer = rowSerializer.getValueSerializer(columnName);
                if (valueSerializer == null) {
                    throw new NullPointerException("unknown valuer serializer for " + columnName);
                }
                columnValue = deserializeValue(value, valueSerializer, dataOperationsProfile);

                valueMap.put(columnName, columnValue);
            }

            if (result.keySet().size() > rowCount) {
                break;
            }
            result.computeIfAbsent(lastKey, v -> new LinkedHashMap<>()).putAll(valueMap);
        }
        results.setEntries(result);
        results.setLastKey(lastKey);
    }

    private Function<Session, ResultSet> executeAndProfile(Statement select, StopWatch monitor, DataOperationsProfile dataOperationsProfile) {
        return session -> {
            try {
                return session.execute(select);
            } finally {
                long time = monitor.stop();
                if (dataOperationsProfile != null) {
                    dataOperationsProfile.ms += time;
                    dataOperationsProfile.dbQueries++;
                }
            }
        };
    }

    private Object deserializeValue(ByteBuffer value, TypeCodec valueSerializer, DataOperationsProfile profile) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        long bytes = value.remaining();

        Object obj = valueSerializer.deserialize(value, protocol());
        long time = stopWatch.stop();

        if (profile != null) {
            profile.bytes += bytes;
            profile.deserializationMs += time;
        }
        return obj;

    }

    private String topKey(String keyspace, String columnFamily) {
        List<ColumnMetadata> primaryKey = hercules.getCluster().getMetadata()
                .getKeyspace(keyspace)
                .getTable(quote(columnFamily))
                .getClusteringColumns();
        return quote(primaryKey.get(0).getName());
    }

    private String primaryKey(String keyspace,
                              String columnFamily) {
        List<ColumnMetadata> primaryKey = hercules.getCluster().getMetadata()
                .getKeyspace(keyspace)
                .getTable(quote(columnFamily))
                .getPrimaryKey();
        return quote(primaryKey.get(0).getName());
    }

    private <R, T> int ttlValue(TTLProvider<R, T> ttlProvider, R rowKey, T topKey) {
        int ttl = DataDriver.EMPTY_TTL;
        if (ttlProvider != null) {
            ttl = ttlProvider.get(rowKey, topKey);
        }
        return ttl;
    }

    private ProtocolVersion protocol() {
        return hercules.getCluster().getConfiguration().getProtocolOptions().getProtocolVersion();
    }

    private <T> TypeCodec<T> codecForColumn(DataType type) {
        return hercules.getCluster().getConfiguration().getCodecRegistry().codecFor(type);
    }

    private interface TTLProvider<R, T> {
        int get(R row, T top);

    }
}
