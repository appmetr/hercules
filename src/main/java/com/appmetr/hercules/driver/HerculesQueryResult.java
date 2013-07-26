package com.appmetr.hercules.driver;

import java.util.LinkedHashMap;
import java.util.Map;

public class HerculesQueryResult<T> {
    private Map<T, Object> entries = new LinkedHashMap<T, Object>();
    boolean hasResult = false;

    public HerculesQueryResult() {
    }

    public HerculesQueryResult(Map<T, Object> entries) {
        this.entries = entries;
        this.hasResult = true;
    }

    public Map<T, Object> getEntries() {
        return entries;
    }

    public boolean hasResult() {
        return hasResult;
    }
}
