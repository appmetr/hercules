package com.appmetr.hercules.profile;

public class DataOperationsProfile {

    public long count;
    public long bytes;
    public long ms;
    public long dbQueries;
    public long deserializationMs;

    @Override public String toString() {
        return String.format("Load: %1$s count, %2$s bytes, %3$s ms; %4$s Q, deserialization %5$s ms\n", count, bytes, ms, dbQueries, deserializationMs);
    }
}
