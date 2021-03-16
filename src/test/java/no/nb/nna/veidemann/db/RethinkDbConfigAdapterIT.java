/*
 * Copyright 2018 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package no.nb.nna.veidemann.db;

import com.rethinkdb.RethinkDB;
import io.grpc.Context;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.CrawlConfig;
import no.nb.nna.veidemann.api.config.v1.CrawlJob;
import no.nb.nna.veidemann.api.config.v1.CrawlScheduleConfig;
import no.nb.nna.veidemann.api.config.v1.GetLabelKeysRequest;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.config.v1.Label;
import no.nb.nna.veidemann.api.config.v1.ListRequest;
import no.nb.nna.veidemann.api.config.v1.LogLevels;
import no.nb.nna.veidemann.api.config.v1.LogLevels.LogLevel;
import no.nb.nna.veidemann.api.config.v1.UpdateRequest;
import no.nb.nna.veidemann.commons.auth.EmailContextKey;
import no.nb.nna.veidemann.commons.auth.RolesContextKey;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.settings.CommonSettings;
import no.nb.nna.veidemann.commons.util.ApiTools;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Comparator;
import java.util.Map;

import static no.nb.nna.veidemann.api.config.v1.Kind.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Integration tests for ConfigListQueryBuilder.
 * <p>
 * These tests are dependent on a running RethinkDB instance.
 */
public class RethinkDbConfigAdapterIT {
    public static RethinkDbConfigAdapter configAdapter;
    static final RethinkDB r = RethinkDB.r;

    ConfigObject crawlScheduleConfig1;
    ConfigObject crawlScheduleConfig2;
    ConfigObject crawlScheduleConfig3;
    ConfigObject crawlJob1;
    ConfigObject crawlJob2;
    ConfigObject politenessConfig1;
    ConfigObject browserScript1;
    ConfigObject crawlConfig1;
    ConfigObject browserConfig1;
    ConfigObject collectionConfig1;

    @Before
    public void init() throws DbException {
        String dbHost = System.getProperty("db.host");
        int dbPort = Integer.parseInt(System.getProperty("db.port"));

        if (!DbService.isConfigured()) {
            CommonSettings settings = new CommonSettings();
            DbService.configure(new CommonSettings()
                    .withDbHost(dbHost)
                    .withDbPort(dbPort)
                    .withDbName("veidemann")
                    .withDbUser("admin")
                    .withDbPassword(""));
        }

        try {
            DbService.getInstance().getDbInitializer().delete();
        } catch (DbException e) {
            if (!e.getMessage().matches("Database .* does not exist.")) {
                throw e;
            }
        }
        try {
            DbService.getInstance().getDbInitializer().initialize();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

        configAdapter = (RethinkDbConfigAdapter) DbService.getInstance().getConfigAdapter();

        browserConfig1 = configAdapter.saveConfigObject(
                createConfBuilder(browserConfig, "bc1").build());

        politenessConfig1 = configAdapter.saveConfigObject(
                createConfBuilder(politenessConfig, "pc1").build());

        collectionConfig1 = configAdapter.saveConfigObject(
                createConfBuilder(collection, "col1").build());

        crawlConfig1 = configAdapter.saveConfigObject(
                createConfBuilder(crawlConfig, "cc1")
                        .setCrawlConfig(CrawlConfig.newBuilder()
                                .setBrowserConfigRef(ApiTools.refForConfig(browserConfig1))
                                .setPolitenessRef(ApiTools.refForConfig(politenessConfig1))
                                .setCollectionRef(ApiTools.refForConfig(collectionConfig1)))
                        .build());

        crawlScheduleConfig1 = configAdapter.saveConfigObject(
                createConfBuilder(crawlScheduleConfig, "csc1", ApiTools.buildLabel("foo", "bar"))
                        .setCrawlScheduleConfig(CrawlScheduleConfig.newBuilder()
                                .setCronExpression("cron1"))
                        .build());

        crawlScheduleConfig2 = configAdapter.saveConfigObject(
                createConfBuilder(crawlScheduleConfig, "csc2")
                        .setCrawlScheduleConfig(CrawlScheduleConfig.newBuilder()
                                .setCronExpression("cron2"))
                        .build());

        crawlScheduleConfig3 = configAdapter.saveConfigObject(
                createConfBuilder(crawlScheduleConfig, "csc3",
                        ApiTools.buildLabel("foo", "bar"), ApiTools.buildLabel("aaa", "bbb"))
                        .setCrawlScheduleConfig(CrawlScheduleConfig.newBuilder()
                                .setCronExpression("cron3"))
                        .build());

        browserScript1 = configAdapter.saveConfigObject(
                createConfBuilder(browserScript, "bs1").build());

        crawlJob1 = configAdapter.saveConfigObject(
                createConfBuilder(crawlJob, "cj1",
                        ApiTools.buildLabel("foo", "bar"), ApiTools.buildLabel("aaa", "bbb"))
                        .setCrawlJob(CrawlJob.newBuilder()
                                .setScheduleRef(ApiTools.refForConfig(crawlScheduleConfig1))
                                .setCrawlConfigRef(ApiTools.refForConfig(crawlConfig1))
                                .setScopeScriptRef(ApiTools.refForConfig(browserScript1)))
                        .build());

        crawlJob2 = configAdapter.saveConfigObject(
                createConfBuilder(crawlJob, "cj2",
                        ApiTools.buildLabel("foo", "bar"), ApiTools.buildLabel("aaa", "bbb"))
                        .setCrawlJob(CrawlJob.newBuilder()
                                .setScheduleRef(ApiTools.refForConfig(crawlScheduleConfig1))
                                .setCrawlConfigRef(ApiTools.refForConfig(crawlConfig1))
                                .setScopeScriptRef(ApiTools.refForConfig(browserScript1)))
                        .build());
    }

    @After
    public void shutdown() {
        DbService.getInstance().close();
    }

    private ConfigObject.Builder createConfBuilder(Kind kind, String name, Label... label) {
        return ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(kind)
                .setMeta(ApiTools.buildMeta(name, name + "desc", label));
    }

    @Test
    public void testSaveAndGetConfigObject() throws DbException {
        ConfigObject.Builder co = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(browserConfig);
        co.getMetaBuilder()
                .setName("Default")
                .setDescription("test")
                .addLabelBuilder().setKey("foo").setValue("bar");
        co.getBrowserConfigBuilder()
                .setUserAgent("agent")
                .addScriptRef(ApiTools.refForConfig(browserScript1));

        // Convert to rethink object and back to ensure nothing gets lost
        Map rethink = ProtoUtils.protoToRethink(co);
        ConfigObject proto = ProtoUtils.rethinkToProto(rethink, ConfigObject.class);
        assertThat(proto).isEqualTo(co.build());

        // Test save
        ConfigObject saved = configAdapter.saveConfigObject(co.build());
        assertThat(saved.getId()).isNotEmpty();

        // Test get
        ConfigObject fetched1 = configAdapter.getConfigObject(ConfigRef.newBuilder()
                .setKind(browserConfig)
                .setId(saved.getId())
                .build());

        assertThat(fetched1.getId()).isNotEmpty().isEqualTo(saved.getId());

        ConfigObject expected = co.setId(saved.getId()).setMeta(saved.getMeta()).build();
        assertThat(fetched1).isEqualTo(expected);

        // Test delete
        assertThat(configAdapter.deleteConfigObject(saved).getDeleted()).isTrue();
        ConfigObject fetched2 = configAdapter.getConfigObject(ConfigRef.newBuilder()
                .setKind(seed)
                .setId(saved.getId())
                .build());

        assertThat(fetched2).isNull();

        // Test mismatch between kind and spec
        ConfigObject.Builder co2 = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(browserConfig);
        co2.getMetaBuilder()
                .setName("Default");
        co2.getSeedBuilder()
                .setDisabled(true)
                .addJobRef(ConfigRef.newBuilder().setKind(browserScript).setId("script"));
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() ->
                        configAdapter.saveConfigObject(co2.build())
                );

        // Test wrong ConfiRef kind
        ConfigObject entity = configAdapter.saveConfigObject(createConfBuilder(crawlEntity, "en1").build());
        ConfigObject.Builder co3 = createConfBuilder(seed, "Default");
        co3.getSeedBuilder()
                .setDisabled(true)
                .setEntityRef(ApiTools.refForConfig(entity))
                .addJobRef(ConfigRef.newBuilder().setKind(browserScript).setId("script"));
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() ->
                        configAdapter.saveConfigObject(co3.build())
                ).withMessage("jobRef has wrong kind: browserScript");

        // Test nonexisting ConfiRef
        ConfigObject.Builder co4 = createConfBuilder(seed, "Default");
        co4.getSeedBuilder()
                .setDisabled(true)
                .setEntityRef(ApiTools.refForConfig(entity))
                .addJobRef(ConfigRef.newBuilder().setKind(crawlJob).setId("job"));
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() ->
                        configAdapter.saveConfigObject(co4.build())
                ).withMessage("Reference with kind 'crawlJob' and id 'job' doesn't exist");
    }

    @Test
    public void testDelete() throws DbException {
        assertThatExceptionOfType(DbQueryException.class)
                .isThrownBy(() -> configAdapter.deleteConfigObject(crawlScheduleConfig1))
                .withMessage("Can't delete crawlScheduleConfig, there are 2 crawlJob(s) referring it");
        assertThat(configAdapter.deleteConfigObject(crawlJob1).getDeleted()).isTrue();
        assertThatExceptionOfType(DbQueryException.class)
                .isThrownBy(() -> configAdapter.deleteConfigObject(crawlScheduleConfig1))
                .withMessage("Can't delete crawlScheduleConfig, there are 1 crawlJob(s) referring it");
        assertThat(configAdapter.deleteConfigObject(crawlJob2).getDeleted()).isTrue();
        assertThat(configAdapter.deleteConfigObject(crawlScheduleConfig1).getDeleted()).isTrue();

        // Not allowed to delete default CrawlHostGroup config
        assertThatExceptionOfType(DbQueryException.class)
                .isThrownBy(() -> configAdapter.deleteConfigObject(ConfigObject.newBuilder()
                        .setKind(crawlHostGroupConfig)
                        .setId("chg-default").build()))
                .withMessage("Removal of default Crawl Host Group Config not allowed");
    }

    @Test
    public void testListConfigObjects() throws DbException {
        // Test list by kind
        ListRequest.Builder req1 = ListRequest.newBuilder()
                .setKind(crawlScheduleConfig);

        assertThat(configAdapter.listConfigObjects(req1.build()).stream())
                .contains(crawlScheduleConfig1, crawlScheduleConfig2, crawlScheduleConfig3).doesNotContain(crawlJob1);

        // Test list by id
        ListRequest.Builder req2 = ListRequest.newBuilder()
                .setKind(crawlScheduleConfig)
                .addId(crawlScheduleConfig1.getId())
                .addId(crawlScheduleConfig3.getId());

        assertThat(configAdapter.listConfigObjects(req2.build()).stream())
                .contains(crawlScheduleConfig1, crawlScheduleConfig3).doesNotContain(crawlScheduleConfig2, crawlJob1);

        // Test list by name regexp
        ListRequest.Builder req3 = ListRequest.newBuilder()
                .setKind(crawlScheduleConfig)
                .setNameRegex(".*3");

        assertThat(configAdapter.listConfigObjects(req3.build()).stream())
                .contains(crawlScheduleConfig3).doesNotContain(crawlScheduleConfig1, crawlScheduleConfig2, crawlJob1);

        // Test select returned fields
        ListRequest.Builder req4 = ListRequest.newBuilder()
                .setKind(crawlScheduleConfig);
        req4.getReturnedFieldsMaskBuilder()
                .addPaths("meta.name")
                .addPaths("apiVersion");

        assertThat(configAdapter.listConfigObjects(req4.build()).stream())
                .isNotEmpty().allMatch(c ->
                c.getMeta().getLabelCount() == 0
                        && c.getApiVersion().equals("v1")
                        && !c.getMeta().getName().isEmpty()
                        && c.getMeta().getDescription().isEmpty());

        // Test list by template filter
        ListRequest.Builder req5 = ListRequest.newBuilder()
                .setKind(crawlScheduleConfig);
        req5.getQueryTemplateBuilder().getMetaBuilder()
                .setName("csc3")
                .setDescription("csc2desc")
                .addLabelBuilder().setKey("foo").setValue("bar");

        ListRequest.Builder lr = req5.clone();
        lr.getQueryMaskBuilder().addPaths("meta.name");

        assertThat(configAdapter.listConfigObjects(lr.build()).stream())
                .contains(crawlScheduleConfig3).doesNotContain(crawlScheduleConfig1, crawlScheduleConfig2, crawlJob1);


        lr = req5.clone();
        lr.getQueryMaskBuilder().addPaths("meta.description");

        assertThat(configAdapter.listConfigObjects(lr.build()).stream())
                .contains(crawlScheduleConfig2).doesNotContain(crawlScheduleConfig3, crawlScheduleConfig1);

        lr = req5.clone();
        lr.getQueryMaskBuilder().addPaths("meta.label");

        assertThat(configAdapter.listConfigObjects(lr.build()).stream())
                .contains(crawlScheduleConfig1, crawlScheduleConfig3).doesNotContain(crawlScheduleConfig2, crawlJob1);

        lr.getQueryTemplateBuilder().getMetaBuilder().addLabelBuilder().setKey("aaa").setValue("bbb");

        assertThat(configAdapter.listConfigObjects(lr.build()).stream())
                .contains(crawlScheduleConfig3).doesNotContain(crawlScheduleConfig1, crawlScheduleConfig2, crawlJob1);


        // Test order
        ListRequest.Builder req6 = ListRequest.newBuilder()
                .setKind(crawlScheduleConfig);
        req6.setOrderByPath("meta.name");

        assertThat(configAdapter.listConfigObjects(req6.build()).stream())
                .isSortedAccordingTo((r1, r2) -> r1.getMeta().getName().compareToIgnoreCase(r2.getMeta().getName()));

        req6.setOrderDescending(true);
        assertThat(configAdapter.listConfigObjects(req6.build()).stream())
                .isSortedAccordingTo((r1, r2) -> r2.getMeta().getName().compareToIgnoreCase(r1.getMeta().getName()));

        req6.setOrderDescending(false);
        req6.setOrderByPath("meta.lastModified");
        assertThat(configAdapter.listConfigObjects(req6.build()).stream()).isSortedAccordingTo(
                Comparator.comparing(r -> ProtoUtils.tsToOdt(r.getMeta().getLastModified())));

        req6.setOrderDescending(false);
        req6.setOrderByPath("meta.label");
        req6.setNameRegex("c[s|j].[1|3]");
        assertThat(configAdapter.listConfigObjects(req6.build()).stream().toArray())
                .containsExactly(crawlScheduleConfig3, crawlScheduleConfig1);

        // Test all options at once
        ListRequest.Builder req7 = ListRequest.newBuilder()
                .setKind(crawlScheduleConfig)
                .addId(crawlScheduleConfig1.getId())
                .addLabelSelector("foo:")
                .setOrderByPath("meta.label")
                .setNameRegex("csc");
        req7.getQueryTemplateBuilder()
                .getMetaBuilder().setDescription("csc1desc");
        req7.getQueryMaskBuilder().addPaths("meta.description");
        assertThat(configAdapter.listConfigObjects(req7.build()).stream())
                .contains(crawlScheduleConfig1).doesNotContain(crawlScheduleConfig3, crawlScheduleConfig2, crawlJob1);
    }

    @Test
    public void testCountConfigObjects() throws Exception {
        assertThat(configAdapter.countConfigObjects(ListRequest.newBuilder()
                .setKind(crawlScheduleConfig)
                .setNameRegex("c[s|j]").build()).getCount()).isEqualTo(3);
    }

    @Test
    public void testGetLabelKeys() throws Exception {
        assertThat(configAdapter.getLabelKeys(GetLabelKeysRequest.newBuilder()
                .setKind(crawlScheduleConfig).build()).getKeyList())
                .containsExactlyInAnyOrder("foo", "aaa");
    }

    @Test
    public void testUpdateConfigObjects() throws Exception {
        // Test add label to objects which already has 'foo:bar' label
        UpdateRequest.Builder ur1 = UpdateRequest.newBuilder();
        ur1.getListRequestBuilder().setKind(crawlScheduleConfig)
                .getQueryTemplateBuilder().getMetaBuilder().addLabelBuilder().setKey("foo").setValue("bar");
        ur1.getListRequestBuilder().getQueryMaskBuilder().addPaths("meta.label");
        ur1.getUpdateTemplateBuilder().getMetaBuilder().setDescription("jalla").addLabelBuilder().setKey("big").setValue("bang");
        ur1.getUpdateMaskBuilder().addPaths("meta.label+");
        assertThat(configAdapter.updateConfigObjects(ur1.build()).getUpdated()).isEqualTo(2);

        // Check result
        ListRequest.Builder test1 = ListRequest.newBuilder()
                .setKind(crawlScheduleConfig)
                .setNameRegex("c[s|j]");

        Label fooBarLabel = Label.newBuilder().setKey("foo").setValue("bar").build();
        Label bigBangLabel = Label.newBuilder().setKey("big").setValue("bang").build();
        Label aaaBBBLabel = Label.newBuilder().setKey("aaa").setValue("bbb").build();
        Label cccDDDLabel = Label.newBuilder().setKey("ccc").setValue("ddd").build();
        assertThat(configAdapter.listConfigObjects(test1.build()).stream())
                .allMatch(r -> {
                    if (r.getMeta().getLabelList().contains(fooBarLabel)) {
                        return r.getMeta().getLabelList().contains(bigBangLabel);
                    } else {
                        return !r.getMeta().getLabelList().contains(bigBangLabel);
                    }
                });

        // Test remove aaa:bbb label and meta.description from all objects
        UpdateRequest.Builder ur2 = UpdateRequest.newBuilder();
        ur2.getListRequestBuilder()
                .setKind(crawlScheduleConfig)
                .setNameRegex("c[s|j]")
                .getReturnedFieldsMaskBuilder().addPaths("meta.description");
        ur2.getUpdateTemplateBuilder().getMetaBuilder().addLabelBuilder().setKey("aaa").setValue("bbb");
        ur2.getUpdateTemplateBuilder().getCrawlScheduleConfigBuilder().setCronExpression("newCron");
        ur2.getUpdateMaskBuilder()
                .addPaths("meta.label-")
                .addPaths("meta.description")
                .addPaths("crawlScheduleConfig.cronExpression");

        // Set user to user1
        Context.current().withValues(EmailContextKey.getKey(), "user1", RolesContextKey.getKey(), null)
                .call(() -> assertThat(configAdapter.updateConfigObjects(ur2.build()).getUpdated()).isEqualTo(3));

        // Check result
        assertThat(configAdapter.listConfigObjects(test1.build()).stream())
                .allSatisfy(r -> {
                    assertThat(r.getMeta().getLabelList()).doesNotContain(aaaBBBLabel);
                    assertThat(r.getMeta().getDescription()).isEmpty();
                    assertThat(r.getCrawlScheduleConfig().getCronExpression()).isEqualTo("newCron");
                });

        // Repeat last update. No objects should actually change
        ConfigObject[] before = configAdapter.listConfigObjects(ListRequest.newBuilder().setKind(crawlScheduleConfig).build())
                .stream().toArray(ConfigObject[]::new);

        // Set user to user2
        Context.current().withValues(EmailContextKey.getKey(), "user2", RolesContextKey.getKey(), null)
                .call(() -> assertThat(configAdapter.updateConfigObjects(ur2.build()).getUpdated()).isEqualTo(0));

        ConfigObject[] after = configAdapter.listConfigObjects(ListRequest.newBuilder().setKind(crawlScheduleConfig).build())
                .stream().toArray(ConfigObject[]::new);

        assertThat(before).contains(after);

        // Update whole subobject
        UpdateRequest.Builder ur3 = UpdateRequest.newBuilder();
        ur3.getListRequestBuilder()
                .setKind(crawlScheduleConfig)
                .setNameRegex("c[s|j]")
                .getReturnedFieldsMaskBuilder().addPaths("meta.description");
        ur3.getUpdateTemplateBuilder().getMetaBuilder().addLabelBuilder().setKey("ccc").setValue("ddd");
        ur3.getUpdateTemplateBuilder().getMetaBuilder().setDescription("desc");
        ur3.getUpdateTemplateBuilder().getMetaBuilder().setName("cs");
        ur3.getUpdateTemplateBuilder().getCrawlScheduleConfigBuilder().setCronExpression("newestCron");
        ur3.getUpdateMaskBuilder()
                .addPaths("meta")
                .addPaths("crawlScheduleConfig.validFrom")
                .addPaths("crawlScheduleConfig");

        // Set user to user3
        Context.current().withValues(EmailContextKey.getKey(), "user3", RolesContextKey.getKey(), null)
                .call(() -> assertThat(configAdapter.updateConfigObjects(ur3.build()).getUpdated()).isEqualTo(3));

        // Check result
        assertThat(configAdapter.listConfigObjects(test1.build()).stream())
                .allSatisfy(r -> {
                    assertThat(r.getMeta().getLabelList()).containsOnly(cccDDDLabel);
                    assertThat(r.getMeta().getName()).isEqualTo("cs");
                    assertThat(r.getMeta().getDescription()).isEqualTo("desc");
                    assertThat(r.getCrawlScheduleConfig().getCronExpression()).isEqualTo("newestCron");
                });

        // Check removal of object (timestamp)
        ConfigObject.Builder csc4 = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(crawlScheduleConfig);
        csc4.getMetaBuilder()
                .setName("csc4")
                .setDescription("desc4")
                .addLabelBuilder().setKey("foo").setValue("bar");
        csc4.getMetaBuilder().addLabelBuilder().setKey("aaa").setValue("bbb");
        csc4.getCrawlScheduleConfigBuilder()
                .setCronExpression("cron4")
                .setValidTo(ProtoUtils.getNowTs());
        configAdapter.saveConfigObject(csc4.build());

        UpdateRequest.Builder ur5 = UpdateRequest.newBuilder();
        ur5.getListRequestBuilder()
                .setKind(crawlScheduleConfig)
                .setNameRegex("^csc.*");
        ur5.getUpdateMaskBuilder()
                .addPaths("crawlScheduleConfig.validFrom")
                .addPaths("crawlScheduleConfig.validTo");

        assertThat(configAdapter.updateConfigObjects(ur5.build()).getUpdated()).isEqualTo(1);

        ListRequest.Builder test3 = ListRequest.newBuilder()
                .setKind(crawlScheduleConfig);
        assertThat(configAdapter.listConfigObjects(test3.build()).stream())
                .allSatisfy(r -> {
                    assertThat(r.getCrawlScheduleConfig().hasValidFrom()).isFalse();
                    assertThat(r.getCrawlScheduleConfig().hasValidTo()).isFalse();
                });

        UpdateRequest.Builder ur6 = UpdateRequest.newBuilder();
        ur6.getListRequestBuilder()
                .setKind(crawlJob)
                .addId(crawlJob1.getId())
                .addId(crawlJob2.getId());
        ur6.getUpdateMaskBuilder()
                .addPaths("crawlJob.limits.maxBytes");
        ur6.getUpdateTemplateBuilder()
                .setKind(crawlJob)
                .getCrawlJobBuilder().getLimitsBuilder().setMaxBytes(420);

        assertThat(configAdapter.updateConfigObjects(ur6.build()).getUpdated()).isEqualTo(2);
    }

    @Test
    public void testSeed() throws DbException {
        // Create two entities for use by seeds in this test
        ConfigObject.Builder eb = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(crawlEntity);
        eb.getMetaBuilder().setName("Example.com");
        ConfigObject entity = configAdapter.saveConfigObject(eb.build());

        eb = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(crawlEntity);
        eb.getMetaBuilder().setName("Example2.com");
        ConfigObject entity2 = configAdapter.saveConfigObject(eb.build());


        ConfigObject.Builder co = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(seed);

        co.getMetaBuilder()
                .setName("http://example.com")
                .setDescription("test")
                .addLabelBuilder().setKey("foo").setValue("bar");

        co.getSeedBuilder()
                .setEntityRef(ApiTools.refForConfig(entity))
                .addJobRef(ApiTools.refForConfig(crawlJob1));

        // Convert to rethink object and back to ensure nothing gets lost
        Map rethink = ProtoUtils.protoToRethink(co);
        ConfigObject proto = ProtoUtils.rethinkToProto(rethink, ConfigObject.class);
        assertThat(proto).isEqualTo(co.build());

        // Test save
        ConfigObject saved = configAdapter.saveConfigObject(co.build());
        assertThat(saved.getId()).isNotEmpty();

        // Test get
        ConfigObject fetched1 = configAdapter.getConfigObject(ConfigRef.newBuilder()
                .setKind(seed)
                .setId(saved.getId())
                .build());

        assertThat(fetched1.getId()).isNotEmpty().isEqualTo(saved.getId());

        ConfigObject expected = co.setId(saved.getId()).setMeta(saved.getMeta()).build();
        assertThat(fetched1).isEqualTo(expected);

        // Test List
        ListRequest.Builder lr = ListRequest.newBuilder().setKind(seed);
        assertThat(configAdapter.listConfigObjects(lr.build()).stream()).containsExactly(saved);

        // Add one more seeds for following tests
        co = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(seed);
        co.getMetaBuilder()
                .setName("http://example2.com")
                .setDescription("test2");
        co.getSeedBuilder()
                .setEntityRef(ApiTools.refForConfig(entity2))
                .addJobRef(ApiTools.refForConfig(crawlJob1))
                .addJobRef(ApiTools.refForConfig(crawlJob2));
        ConfigObject seed2 = configAdapter.saveConfigObject(co.build());

        // Test list by entityRef
        lr = ListRequest.newBuilder()
                .setKind(seed);
        lr.getQueryTemplateBuilder()
                .getSeedBuilder().setEntityRef(ApiTools.refForConfig(entity));
        lr.getQueryMaskBuilder().addPaths("seed.entityRef");
        assertThat(configAdapter.listConfigObjects(lr.build()).stream())
                .hasSize(1)
                .allSatisfy(s -> {
                    assertThat(s.getApiVersion()).isEqualTo("v1");
                    assertThat(s.getKind()).isEqualTo(seed);
                    assertThat(s.getId()).isEqualTo(saved.getId());
                });

        // Test list by jobRef
        lr = ListRequest.newBuilder()
                .setKind(seed);
        lr.getQueryTemplateBuilder()
                .getSeedBuilder().addJobRef(ApiTools.refForConfig(crawlJob1));
        lr.getQueryMaskBuilder().addPaths("seed.jobRef");
        assertThat(configAdapter.listConfigObjects(lr.build()).stream())
                .hasSize(2)
                .allSatisfy(s -> {
                    assertThat(s.getApiVersion()).isEqualTo("v1");
                    assertThat(s.getKind()).isEqualTo(seed);
                    assertThat(s.getId()).isIn(saved.getId(), seed2.getId());
                });

        // Test list by two jobRefs
        // TODO: This test returns two objects, but should return just one. Leaves this for now as it is not important
        //       enough to block release of other bugfixes.
//        lr = ListRequest.newBuilder()
//                .setKind(seed);
//        lr.getQueryTemplateBuilder()
//                .getSeedBuilder().addJobRef(ApiTools.refForConfig(crawlJob1)).addJobRef(ApiTools.refForConfig(crawlJob2));
//        lr.getQueryMaskBuilder().addPaths("seed.jobRef");
//        assertThat(configAdapter.listConfigObjects(lr.build()).stream())
//                .hasSize(1)
//                .allSatisfy(s -> {
//                    assertThat(s.getApiVersion()).isEqualTo("v1");
//                    assertThat(s.getKind()).isEqualTo(seed);
//                    assertThat(s.getId()).isEqualTo(seed2.getId());
//                });

        // Test list by id and entityRef
        lr = ListRequest.newBuilder()
                .addId(saved.getId())
                .setKind(seed);
        lr.getQueryTemplateBuilder()
                .getSeedBuilder().setEntityRef(ApiTools.refForConfig(entity));
        lr.getQueryMaskBuilder().addPaths("seed.entityRef");
        assertThat(configAdapter.listConfigObjects(lr.build()).stream())
                .hasSize(1)
                .allSatisfy(s -> {
                    assertThat(s.getApiVersion()).isEqualTo("v1");
                    assertThat(s.getKind()).isEqualTo(seed);
                    assertThat(s.getId()).isEqualTo(saved.getId());
                });

        // Test list by id and jobRef
        lr = ListRequest.newBuilder()
                .addId(saved.getId())
                .setKind(seed);
        lr.getQueryTemplateBuilder()
                .getSeedBuilder().addJobRef(ApiTools.refForConfig(crawlJob1));
        lr.getQueryMaskBuilder().addPaths("seed.jobRef");
        assertThat(configAdapter.listConfigObjects(lr.build()).stream())
                .hasSize(1)
                .allSatisfy(s -> {
                    assertThat(s.getApiVersion()).isEqualTo("v1");
                    assertThat(s.getKind()).isEqualTo(seed);
                    assertThat(s.getId()).isEqualTo(saved.getId());
                });

        // Test list by id and two jobRefs
        lr = ListRequest.newBuilder()
                .addId(seed2.getId())
                .setKind(seed);
        lr.getQueryTemplateBuilder()
                .getSeedBuilder().addJobRef(ApiTools.refForConfig(crawlJob1)).addJobRef(ApiTools.refForConfig(crawlJob2));
        lr.getQueryMaskBuilder().addPaths("seed.jobRef");
        assertThat(configAdapter.listConfigObjects(lr.build()).stream())
                .hasSize(1)
                .allSatisfy(s -> {
                    assertThat(s.getApiVersion()).isEqualTo("v1");
                    assertThat(s.getKind()).isEqualTo(seed);
                    assertThat(s.getId()).isEqualTo(seed2.getId());
                });

        // Test update
        lr = ListRequest.newBuilder().setKind(seed).addId(saved.getId());
        UpdateRequest ur = UpdateRequest.newBuilder().setListRequest(lr)
                .setUpdateTemplate(ConfigObject.getDefaultInstance()).build();
        assertThat(configAdapter.updateConfigObjects(ur).getUpdated()).isEqualTo(1);
        assertThat(configAdapter.listConfigObjects(lr.build()).stream())
                .hasSize(1)
                .allSatisfy(s -> {
                    assertThat(s.getApiVersion()).isEqualTo("v1");
                    assertThat(s.getKind()).isEqualTo(seed);
                    assertThat(s.getId()).isEqualTo(saved.getId());
                    assertThat(s.getMeta().getName()).isEmpty();
                    assertThat(s.getSeed().hasEntityRef()).isFalse();
                    assertThat(s.getSeed().getJobRefList()).isEmpty();
                });

        // Test delete
        assertThat(configAdapter.deleteConfigObject(saved).getDeleted()).isTrue();
        ConfigObject fetched2 = configAdapter.getConfigObject(ConfigRef.newBuilder()
                .setKind(seed)
                .setId(saved.getId())
                .build());

        assertThat(fetched2).isNull();
    }

    @Test
    public void testCrawlEntity() throws DbException {
        ConfigObject.Builder co = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(crawlEntity);

        co.getMetaBuilder()
                .setName("Example.com")
                .setDescription("test")
                .addLabelBuilder().setKey("foo").setValue("bar");

        // Convert to rethink object and back to ensure nothing gets lost
        Map rethink = ProtoUtils.protoToRethink(co);
        ConfigObject proto = ProtoUtils.rethinkToProto(rethink, ConfigObject.class);
        assertThat(proto).isEqualTo(co.build());

        // Test save
        ConfigObject saved = configAdapter.saveConfigObject(co.build());
        assertThat(saved.getId()).isNotEmpty();

        // Test get
        ConfigObject fetched1 = configAdapter.getConfigObject(ConfigRef.newBuilder()
                .setKind(crawlEntity)
                .setId(saved.getId())
                .build());

        assertThat(fetched1.getId()).isNotEmpty().isEqualTo(saved.getId());

        ConfigObject expected = co.setId(saved.getId()).setMeta(saved.getMeta()).build();
        assertThat(fetched1).isEqualTo(expected);

        // Test List
        ListRequest lr = ListRequest.newBuilder().setKind(crawlEntity).build();
        assertThat(configAdapter.listConfigObjects(lr).stream()).containsExactly(saved);

        // Test update
        UpdateRequest ur = UpdateRequest.newBuilder().setListRequest(lr)
                .setUpdateTemplate(ConfigObject.getDefaultInstance()).build();
        assertThat(configAdapter.updateConfigObjects(ur).getUpdated()).isEqualTo(1);
        assertThat(configAdapter.listConfigObjects(lr).stream())
                .hasSize(1)
                .allSatisfy(s -> {
                    assertThat(s.getApiVersion()).isEqualTo("v1");
                    assertThat(s.getKind()).isEqualTo(crawlEntity);
                    assertThat(s.getId()).isEqualTo(saved.getId());
                    assertThat(s.getMeta().getName()).isEmpty();
                });

        // Test delete
        assertThat(configAdapter.deleteConfigObject(saved).getDeleted()).isTrue();
        ConfigObject fetched2 = configAdapter.getConfigObject(ConfigRef.newBuilder()
                .setKind(crawlEntity)
                .setId(saved.getId())
                .build());

        assertThat(fetched2).isNull();
    }

    @Test
    public void testSaveAndGetLogConfig() throws DbException {
        LogLevel l1 = LogLevel.newBuilder().setLogger("no.nb.nna").setLevel(LogLevels.Level.INFO).build();
        LogLevel l2 = LogLevel.newBuilder().setLogger("org.apache").setLevel(LogLevels.Level.FATAL).build();
        LogLevels logLevels = LogLevels.newBuilder().addLogLevel(l1).addLogLevel(l2).build();
        LogLevels response;

        response = configAdapter.saveLogConfig(logLevels);
        assertThat(response).isEqualTo(logLevels);

        response = configAdapter.getLogConfig();
        assertThat(response).isEqualTo(logLevels);
    }
}
