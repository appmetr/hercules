package com.appmetr.hercules;

import com.appmetr.hercules.driver.DataDriver;
import com.appmetr.hercules.driver.ThriftDataDriver;
import com.appmetr.hercules.manager.EntityManager;
import com.appmetr.hercules.manager.IndexManager;
import com.appmetr.hercules.manager.WideEntityManager;
import com.appmetr.monblank.Monitoring;
import com.appmetr.monblank.MonitoringStub;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;

public class HerculesFactory {
    public static Hercules create(final HerculesConfig config) {
        Injector injector = Guice.createInjector(new AbstractModule() {
            @Override protected void configure() {
                bind(HerculesConfig.class).toInstance(config);

                bind(DataDriver.class).to(ThriftDataDriver.class).in(Scopes.SINGLETON);
                bind(EntityManager.class).in(Scopes.SINGLETON);
                bind(WideEntityManager.class).in(Scopes.SINGLETON);
                bind(IndexManager.class).in(Scopes.SINGLETON);

                bind(Monitoring.class).to(MonitoringStub.class).in(Scopes.SINGLETON);
            }
        });

        return injector.getInstance(Hercules.class);
    }
}
