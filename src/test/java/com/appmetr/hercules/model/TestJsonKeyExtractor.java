package com.appmetr.hercules.model;

import com.appmetr.hercules.keys.CollectionKeysExtractor;
import com.datastax.driver.core.TypeCodec;
import org.apache.commons.lang3.StringUtils;


import java.util.ArrayList;
import java.util.List;

public class TestJsonKeyExtractor implements CollectionKeysExtractor<EntityWithCollection, String> {
    @Override public Iterable<String> extractKeys(EntityWithCollection team) {
        String users = StringUtils.isEmpty(team.getJsonCollection()) ? "{}" : team.getJsonCollection();
        users = users.substring(1, users.length() - 1);
        String[] keys = users.split(",");

        List<String> resultKeys = new ArrayList<>(keys.length);
        for (String k : keys) {
            resultKeys.add(k.trim());
        }
        return resultKeys;
    }

    @Override public TypeCodec<String> getKeySerializer() {
        return TypeCodec.varchar();
    }

}