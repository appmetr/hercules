package com.appmetr.hercules.profile;

public class DataOperationsProfile {

    public long count;
    public long bytes;
    public long ms;
    public long dbQueries;

    @Override public String toString() {
        return String.format("Load: %1$s count, %2$s bytes, %3$s ms; %4$s Q\n", count, bytes, ms, dbQueries);
    }
}
