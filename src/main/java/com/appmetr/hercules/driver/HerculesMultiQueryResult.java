package com.appmetr.hercules.driver;

import java.util.HashMap;
import java.util.Map;

public class HerculesMultiQueryResult<R, T> {
    private Map<R, Map<T, Object>> entries = new HashMap<R, Map<T, Object>>();
    boolean hasResult = false;

    public HerculesMultiQueryResult() {
    }

    public HerculesMultiQueryResult(Map<R, Map<T, Object>> entries) {
        this.entries = entries;
        this.hasResult = true;
    }

    public Boolean containsKey(R rowKey) {
        return entries.containsKey(rowKey);
    }

    public Map<R, Map<T, Object>> getEntries() {
        return entries;
    }

    public boolean hasResult() {
        return hasResult;
    }
}
