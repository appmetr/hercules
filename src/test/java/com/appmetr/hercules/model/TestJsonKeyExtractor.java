package com.appmetr.hercules.model;

import com.appmetr.hercules.keys.CollectionKeysExtractor;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Serializer;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class TestJsonKeyExtractor implements CollectionKeysExtractor<EntityWithCollection, String> {
    @Override public Iterable<String> extractKeys(EntityWithCollection team) {
        String users = StringUtils.isEmpty(team.getJsonCollection()) ? "{}" : team.getJsonCollection();
        users = users.substring(1, users.length() - 1);
        String[] keys = users.split(",");

        List<String> resultKeys = new ArrayList<String>(keys.length);
        for (String k : keys) {
            resultKeys.add(k.trim());
        }
        return resultKeys;
    }

    @Override public Serializer<String> getKeySerializer() {
        return new StringSerializer();
    }

}