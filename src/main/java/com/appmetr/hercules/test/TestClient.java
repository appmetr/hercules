package com.appmetr.hercules.test;

import com.appmetr.hercules.Hercules;
import com.appmetr.hercules.HerculesConfig;
import com.appmetr.hercules.annotations.Entity;
import com.appmetr.hercules.driver.CqlDataDriver;
import com.appmetr.hercules.driver.DataDriver;
import com.appmetr.hercules.manager.EntityManager;
import com.appmetr.hercules.manager.IndexManager;
import com.appmetr.hercules.manager.WideEntityManager;
import com.appmetr.monblank.Monitoring;
import com.appmetr.monblank.MonitoringStub;
import com.datastax.driver.core.AbstractTableMetadata;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.TypeCodec;
import com.google.inject.*;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableLong;

import java.io.IOException;
import java.util.*;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

@Slf4j
public class TestClient {
    public static void main(String[] args) {
        try {
            Injector injector = Guice.createInjector(
                    new AbstractModule() {
                        @Override protected void configure() {
                            Properties properties = new Properties();
                            try {
                                properties.load(this.getClass().getClassLoader().getResourceAsStream("test.properties"));
                            } catch (IOException e) {
                                log.info("cannot load properties", e);
                            }
                            bind(Properties.class).toInstance(properties);

                            // binding all properties as String
                            Names.bindProperties(binder(), properties);

                            bind(Monitoring.class).to(MonitoringStub.class).in(Scopes.SINGLETON);
                        }
                    },
                    new AbstractModule() {
                        @Override protected void configure() {
                            Multibinder.newSetBinder(binder(), Class.class, Entity.class).addBinding().toInstance(AdvertiseIdUser.class);
                            Multibinder.newSetBinder(binder(), Class.class, Entity.class).addBinding().toInstance(AppUser.class);


                            ;
                            Multibinder.newSetBinder(binder(), TypeCodec.class).addBinding()
                                    .toInstance(new CompositeKeyStringSerializer<>(AppUserPK.class));
                            Multibinder.newSetBinder(binder(), TypeCodec.class).addBinding().to(JsonListStringSerializer.class);

                        }
                    },
                    new AbstractModule() {
                        @Override protected void configure() {
                            bind(HerculesConfig.class).to(Config.class).in(Scopes.SINGLETON);

                            bind(DataDriver.class).to(CqlDataDriver.class).in(Scopes.SINGLETON);
                            bind(EntityManager.class).in(Scopes.SINGLETON);
                            bind(WideEntityManager.class).in(Scopes.SINGLETON);
                            bind(IndexManager.class).in(Scopes.SINGLETON);
                        }
                    });
            Hercules instance = injector.getInstance(Hercules.class);
            instance.init();


            //dumpTables(instance);

            //extractAdvertiseId(injector);
           extractUser(injector);

            instance.shutdown();
        } catch (Exception e) {
            log.info("cannot init app", e);
        }

    }

    private static void dumpTables(Hercules instance) {
        Map<List<ColumnMetadata>, String> map = instance.getCluster().getMetadata().getKeyspace("\"PixAPI\"").getTables()
                .stream()
                .collect(Collectors.toMap(
                        AbstractTableMetadata::getColumns,
                        AbstractTableMetadata::getName,
                        (s, s2) -> s + "," + s2));

        for (Map.Entry<List<ColumnMetadata>, String> entry : map.entrySet()) {
            log.info("{} {}", entry.getKey(), entry.getValue());
        }

        /*Set<String> uniqueColumns = .stream()
                .map(AbstractTableMetadata::getColumns).map(m -> m.toString()).collect(Collectors.toSet());*/
        /*for (String uniqueColumn : uniqueColumns) {
            log.info("{}", uniqueColumn);
        }*/
    }

    private static void extractUser(Injector injector) {
        AppUserDao userDao = injector.getInstance(AppUserDao.class);
        AppUser user2 = userDao.get(
                new AppUserPK("070e247a-2040-4d88-b14c-50ec9e58c1fd",
                        "d51105e56e2e0991"));

        log.info("user {}", user2);
    }

    private static void saveUser(Injector injector) {
        AppUserDao userDao = injector.getInstance(AppUserDao.class);
        AppUser user = new AppUser("070e247a-2040-4d88-b14c-50ec9e58c1fd","blablalb");
        user.setGender("M");
        user.setCountry("anotherCountry");
        user.setLocale("RU");
        AppUserPK pk = new AppUserPK("070e247a-2040-4d88-b14c-50ec9e58c1fd", "blablalb");
        userDao.save(pk, user);
    }

    private static void extractAllAdvId(Injector injector) {
        MutableLong c = new MutableLong(0);
        injector.getInstance(AdvetriseIdDao.class).processAll(1_000, batch -> {
            log.debug("processed {}", batch.size());
            c.add(batch.size());
        });
        log.info("processed {}", c.longValue());
    }

    private static void extractAdvertiseId(Injector injector) {
        AdvetriseIdDao dao = injector.getInstance(AdvetriseIdDao.class);

        //AdvertiseIdUser user = dao.get("fc63290ff85f495e");
        AdvertiseIdUser user = dao.get("d51105e56e2e0991");

        log.info("user {}", user);
    }
}

class Config extends HerculesConfig {
    @Inject @Named("cassandra.cluster.name")
    private String clusterName;
    @Inject @Named("cassandra.keyspace.name")
    private String keyspaceName;
    @Inject @Named("cassandra.host")
    private String cassandraHost;
    @Inject @Named("cassandra.schemaModificationEnabled")
    private Boolean schemaModificationEnabled;

    @Inject @Entity
    private Set<Class> entityClasses = new HashSet<>();

    @Inject
    private Set<TypeCodec> codecs = new HashSet<>();

    @Override public Boolean isSchemaModificationEnabled() {
        return schemaModificationEnabled;
    }

    @Override public String getClusterName() {
        return clusterName;
    }

    @Override public String getCassandraHost() {
        return cassandraHost;
    }

    @Override public String getKeyspaceName() {
        return keyspaceName;

    }

    @Override public Set<TypeCodec> getCodecs() {
        return this.codecs;
    }

    @Override public Set<Class> getEntityClasses() {
        return entityClasses;
    }
}
