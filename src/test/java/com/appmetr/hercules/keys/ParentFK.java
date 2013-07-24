package com.appmetr.hercules.keys;

import com.appmetr.hercules.annotations.Serializer;
import com.appmetr.hercules.serializers.ParentFKSerializer;

@Serializer(ParentFKSerializer.class)
public class ParentFK implements ForeignKey {
    public String parent;

    public ParentFK() {
    }

    public ParentFK(String parent) {
        this.parent = parent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ParentFK parentFK = (ParentFK) o;

        return !(parent != null ? !parent.equals(parentFK.parent) : parentFK.parent != null);
    }

    @Override
    public int hashCode() {
        return parent != null ? parent.hashCode() : 0;
    }
}
