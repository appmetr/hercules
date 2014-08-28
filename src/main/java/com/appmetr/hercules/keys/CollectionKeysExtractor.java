package com.appmetr.hercules.keys;

import me.prettyprint.hector.api.Serializer;

public interface CollectionKeysExtractor<E, K> {

    Iterable<K> extractKeys(E entity);

    Serializer<K> getKeySerializer();

}
