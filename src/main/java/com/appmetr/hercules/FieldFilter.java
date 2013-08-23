package com.appmetr.hercules;

import java.lang.reflect.Field;

public interface FieldFilter {

    public boolean accept(Field field);
}
