package com.appmetr.hercules.profile;

public class DataOperationsProfile {

    public long count;
    public long bytes;
    public long ms;
    public long dbQueries;
    public long serializationMs;
    public long deserializationMs;

    @Override public String toString() {
        return String.format("Load: %1$s count, %2$s bytes, %3$s ms; %4$s Q;  %5$s serializationMs, %6$s deserializationMs\n", count, bytes, ms, dbQueries, serializationMs, deserializationMs);
    }
}
