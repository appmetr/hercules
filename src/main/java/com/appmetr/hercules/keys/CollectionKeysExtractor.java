package com.appmetr.hercules.keys;

import com.datastax.driver.core.TypeCodec;

public interface CollectionKeysExtractor<E, K> {

    Iterable<K> extractKeys(E entity);

    TypeCodec<K> getKeySerializer();

}
