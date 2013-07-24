package com.appmetr.hercules.dao;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.model.ParentEntity;

public class ParentEntityDAO extends AbstractDAO<ParentEntity, String> {
    private Hercules hercules;

    public ParentEntityDAO(Hercules hercules) {
        super(ParentEntity.class);

        this.hercules = hercules;
    }

    @Override public Hercules getHercules() {
        return hercules;
    }
}
