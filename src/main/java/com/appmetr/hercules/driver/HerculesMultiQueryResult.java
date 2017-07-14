package com.appmetr.hercules.driver;

import java.util.LinkedHashMap;

public class HerculesMultiQueryResult<R, T> {
    private LinkedHashMap<R, LinkedHashMap<T, Object>> entries = new LinkedHashMap<>();
    private R lastKey = null;  //Last loaded from DB key. Cause key may by deleted we need to know this last key outside
    boolean hasResult = false;

    public HerculesMultiQueryResult() {
    }

    public HerculesMultiQueryResult(R lastKey) {
        this.lastKey = lastKey;
    }

    public HerculesMultiQueryResult(LinkedHashMap<R, LinkedHashMap<T, Object>> entries, R lastKey) {
        this.entries = entries;
        this.lastKey = lastKey;
        this.hasResult = entries.size() > 0 ;
    }

    public Boolean containsKey(R rowKey) {
        return entries.containsKey(rowKey);
    }

    public LinkedHashMap<R, LinkedHashMap<T, Object>> getEntries() {
        return entries;
    }

    public void setEntries(LinkedHashMap<R, LinkedHashMap<T, Object>> entries) {
        this.entries.putAll(entries);
        hasResult = true;
    }

    public void setLastKey(R lastKey) {
        this.lastKey = lastKey;
    }

    public R getLastKey() {
        return lastKey;
    }

    public boolean hasResult() {
        return hasResult;
    }
}
