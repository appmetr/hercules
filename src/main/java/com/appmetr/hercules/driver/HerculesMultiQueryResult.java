package com.appmetr.hercules.driver;

import java.util.LinkedHashMap;
import java.util.Map;

public class HerculesMultiQueryResult<R, T> {
    private Map<R, Map<T, Object>> entries = new LinkedHashMap<R, Map<T, Object>>();
    private R lastKey = null;  //Last loaded from DB key. Cause key may by deleted we need to know this last key outside
    boolean hasResult = false;

    public HerculesMultiQueryResult() {
    }

    public HerculesMultiQueryResult(R lastKey) {
        this.lastKey = lastKey;
    }

    public HerculesMultiQueryResult(Map<R, Map<T, Object>> entries, R lastKey) {
        this.entries = entries;
        this.lastKey = lastKey;
        this.hasResult = true;
    }

    public Boolean containsKey(R rowKey) {
        return entries.containsKey(rowKey);
    }

    public Map<R, Map<T, Object>> getEntries() {
        return entries;
    }

    public R getLastKey() {
        return lastKey;
    }

    public boolean hasResult() {
        return hasResult;
    }
}
