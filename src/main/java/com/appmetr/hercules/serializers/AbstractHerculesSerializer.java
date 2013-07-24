package com.appmetr.hercules.serializers;

import me.prettyprint.cassandra.serializers.AbstractSerializer;

public abstract class AbstractHerculesSerializer<T> extends AbstractSerializer<T> {
    private Class<T> instanceClass;

    public Class<T> getInstanceClass() {
        return instanceClass;
    }

    public void setInstanceClass(Class<T> instanceClass) {
        this.instanceClass = instanceClass;
    }
}
