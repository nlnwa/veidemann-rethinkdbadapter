/*
 * Copyright 2019 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
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
import no.nb.nna.veidemann.api.eventhandler.v1.Activity.ChangeType;
import no.nb.nna.veidemann.api.eventhandler.v1.Data;
import no.nb.nna.veidemann.api.eventhandler.v1.EventObject;
import no.nb.nna.veidemann.api.eventhandler.v1.EventObject.Severity;
import no.nb.nna.veidemann.api.eventhandler.v1.EventObject.State;
import no.nb.nna.veidemann.api.eventhandler.v1.EventRef;
import no.nb.nna.veidemann.api.eventhandler.v1.ListLabelRequest;
import no.nb.nna.veidemann.api.eventhandler.v1.ListRequest;
import no.nb.nna.veidemann.api.eventhandler.v1.SaveRequest;
import no.nb.nna.veidemann.api.eventhandler.v1.UpdateRequest;
import no.nb.nna.veidemann.commons.auth.EmailContextKey;
import no.nb.nna.veidemann.commons.auth.RolesContextKey;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.settings.CommonSettings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Integration tests for ConfigListQueryBuilder.
 * <p>
 * These tests are dependent on a running RethinkDB instance.
 */
public class RethinkDbEventAdapterIT {
    public static RethinkDbEventAdapter eventAdapter;
    static final RethinkDB r = RethinkDB.r;

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

        eventAdapter = (RethinkDbEventAdapter) DbService.getInstance().getEventAdapter();
    }

    @After
    public void shutdown() {
        DbService.getInstance().close();
    }

    @Test
    public void testSaveAndGet() throws Exception {
        EventObject.Builder eo = EventObject.newBuilder()
                .setSeverity(Severity.INFO);

        // Test save with missing parameters
        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() ->
                eventAdapter.saveEventObject(saveRequest(eo, "My first comment")))
                .withMessage("Missing type for event object");

        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() ->
                eventAdapter.saveEventObject(saveRequest(eo.setType("type1"), "My first comment")))
                .withMessage("Missing source for event object");

        eo.setSource("system");

        // Test save
        EventObject saved = eventAdapter.saveEventObject(saveRequest(eo, "My first comment"));
        assertThat(saved.getId()).isNotEmpty();

        // Test get
        EventObject fetched1 = eventAdapter.getEventObject(EventRef.newBuilder()
                .setId(saved.getId())
                .build());

        assertThat(fetched1.getId()).isNotEmpty().isEqualTo(saved.getId());

        EventObject.Builder expected = eo.setId(saved.getId()).setState(State.NEW);
        expected.addActivityBuilder()
                .setModifiedBy("anonymous")
                .setModifiedTime(saved.getActivity(0).getModifiedTime())
                .setComment("My first comment")
                .addDescriptionBuilder().setType(ChangeType.CREATED);

        assertThat(fetched1).isEqualTo(expected.build());

        // Update sevrity
        EventObject.Builder eo2 = saved.toBuilder().setSeverity(Severity.WARN);

        // Set user to user1
        EventObject eo3 = Context.current().withValues(EmailContextKey.getKey(), "user1", RolesContextKey.getKey(), null)
                .call(() -> {
                    EventObject tmp = eventAdapter.saveEventObject(saveRequest(eo2, "Changed severity"));
                    assertThat(tmp).hasFieldOrPropertyWithValue("severity", Severity.WARN);
                    assertThat(tmp.getActivityList()).hasSize(2).element(0)
                            .hasFieldOrPropertyWithValue("modifiedBy", "user1")
                            .hasFieldOrPropertyWithValue("comment", "Changed severity");
                    assertThat(tmp.getActivity(0).getDescription(0))
                            .hasFieldOrPropertyWithValue("type", ChangeType.VALUE_CHANGED)
                            .hasFieldOrPropertyWithValue("field", "severity")
                            .hasFieldOrPropertyWithValue("newVal", Severity.WARN.name())
                            .hasFieldOrPropertyWithValue("oldVal", "");
                    return tmp;
                });

        // Test that no changes are logged in activity when there is no changes made to object
        saved = eventAdapter.saveEventObject(saveRequest(saved.toBuilder().setSeverity(Severity.WARN), "Changed severity to same"));
        assertThat(saved).isEqualTo(eo3);

        // Test adding to array
        saved = eventAdapter.saveEventObject(saveRequest(saved.toBuilder().addLabel("label1"), "Added label"));
        assertThat(saved).hasFieldOrPropertyWithValue("severity", Severity.WARN);
        assertThat(saved.getActivityList()).hasSize(3).element(0)
                .hasFieldOrPropertyWithValue("modifiedBy", "anonymous")
                .hasFieldOrPropertyWithValue("comment", "Added label");
        assertThat(saved.getActivity(0).getDescription(0))
                .hasFieldOrPropertyWithValue("type", ChangeType.ARRAY_ADD)
                .hasFieldOrPropertyWithValue("field", "label")
                .hasFieldOrPropertyWithValue("newVal", "label1")
                .hasFieldOrPropertyWithValue("oldVal", "");

        // Test that updates to source and type are ignored
        EventObject saved2 = eventAdapter.saveEventObject(saveRequest(saved.toBuilder().setSource("source1").setType("type2"), "Updated source and type"));
        assertThat(saved2).isEqualTo(saved);

        // Test several changes at once and that the whole history is kept
        Data d1 = Data.newBuilder().setKey("text").setValue("Jadda").build();
        saved = eventAdapter.saveEventObject(SaveRequest.newBuilder().setComment("Did a lot")
                .setObject(saved.toBuilder()
                        .clearLabel()
                        .addLabel("label2")
                        .addLabel("LABEL3")
                        .setSeverity(Severity.ERROR)
                        .setAssignee("me")
                        .addData(d1))
                .build());
        assertThat(saved).hasFieldOrPropertyWithValue("severity", Severity.ERROR)
                .hasFieldOrPropertyWithValue("assignee", "me")
                .hasFieldOrPropertyWithValue("source", "system")
                .hasFieldOrPropertyWithValue("type", "type1");
        assertThat(saved.getDataList()).hasSize(1).element(0)
                .hasFieldOrPropertyWithValue("key", "text")
                .hasFieldOrPropertyWithValue("value", "Jadda");
        assertThat(saved.getLabelList()).hasSize(2).containsExactly("label2", "label3");
        assertThat(saved.getActivityList()).hasSize(4);

        assertThat(saved.getActivity(0))
                .hasFieldOrPropertyWithValue("modifiedBy", "anonymous")
                .hasFieldOrPropertyWithValue("comment", "Did a lot");
        assertThat(saved.getActivity(0).getDescriptionCount()).isEqualTo(6);
        assertThat(saved.getActivity(0).getDescription(0))
                .hasFieldOrPropertyWithValue("type", ChangeType.VALUE_CHANGED)
                .hasFieldOrPropertyWithValue("field", "assignee")
                .hasFieldOrPropertyWithValue("newVal", "me")
                .hasFieldOrPropertyWithValue("oldVal", "");
        assertThat(saved.getActivity(0).getDescription(1))
                .hasFieldOrPropertyWithValue("type", ChangeType.ARRAY_ADD)
                .hasFieldOrPropertyWithValue("field", "data")
                .hasFieldOrPropertyWithValue("newVal", "{\"key\":\"text\",\"value\":\"Jadda\"}")
                .hasFieldOrPropertyWithValue("oldVal", "");
        assertThat(saved.getActivity(0).getDescription(2))
                .hasFieldOrPropertyWithValue("type", ChangeType.VALUE_CHANGED)
                .hasFieldOrPropertyWithValue("field", "severity")
                .hasFieldOrPropertyWithValue("newVal", Severity.ERROR.name())
                .hasFieldOrPropertyWithValue("oldVal", Severity.WARN.name());
        assertThat(saved.getActivity(0).getDescription(3))
                .hasFieldOrPropertyWithValue("type", ChangeType.ARRAY_ADD)
                .hasFieldOrPropertyWithValue("field", "label")
                .hasFieldOrPropertyWithValue("newVal", "label2")
                .hasFieldOrPropertyWithValue("oldVal", "");
        assertThat(saved.getActivity(0).getDescription(4))
                .hasFieldOrPropertyWithValue("type", ChangeType.ARRAY_ADD)
                .hasFieldOrPropertyWithValue("field", "label")
                .hasFieldOrPropertyWithValue("newVal", "label3")
                .hasFieldOrPropertyWithValue("oldVal", "");
        assertThat(saved.getActivity(0).getDescription(5))
                .hasFieldOrPropertyWithValue("type", ChangeType.ARRAY_DEL)
                .hasFieldOrPropertyWithValue("field", "label")
                .hasFieldOrPropertyWithValue("newVal", "")
                .hasFieldOrPropertyWithValue("oldVal", "label1");

        assertThat(saved.getActivity(1))
                .hasFieldOrPropertyWithValue("modifiedBy", "anonymous")
                .hasFieldOrPropertyWithValue("comment", "Added label");
        assertThat(saved.getActivity(1).getDescriptionCount()).isEqualTo(1);
        assertThat(saved.getActivity(1).getDescription(0))
                .hasFieldOrPropertyWithValue("type", ChangeType.ARRAY_ADD)
                .hasFieldOrPropertyWithValue("field", "label")
                .hasFieldOrPropertyWithValue("newVal", "label1")
                .hasFieldOrPropertyWithValue("oldVal", "");

        assertThat(saved.getActivity(2))
                .hasFieldOrPropertyWithValue("modifiedBy", "user1")
                .hasFieldOrPropertyWithValue("comment", "Changed severity");
        assertThat(saved.getActivity(2).getDescriptionCount()).isEqualTo(1);
        assertThat(saved.getActivity(2).getDescription(0))
                .hasFieldOrPropertyWithValue("type", ChangeType.VALUE_CHANGED)
                .hasFieldOrPropertyWithValue("field", "severity")
                .hasFieldOrPropertyWithValue("newVal", Severity.WARN.name())
                .hasFieldOrPropertyWithValue("oldVal", "");

        assertThat(saved.getActivity(3))
                .hasFieldOrPropertyWithValue("modifiedBy", "anonymous")
                .hasFieldOrPropertyWithValue("comment", "My first comment");
        assertThat(saved.getActivity(3).getDescriptionCount()).isEqualTo(1);
        assertThat(saved.getActivity(3).getDescription(0))
                .hasFieldOrPropertyWithValue("type", ChangeType.CREATED)
                .hasFieldOrPropertyWithValue("field", "")
                .hasFieldOrPropertyWithValue("newVal", "")
                .hasFieldOrPropertyWithValue("oldVal", "");
    }

    private SaveRequest saveRequest(EventObject.Builder obj, String comment) {
        return SaveRequest.newBuilder().setObject(obj).setComment(comment).build();
    }

    @Test
    public void testListEventObjects() throws DbException {
        EventObject eo1 = eventAdapter.saveEventObject(saveRequest(EventObject.newBuilder().setType("type1").setSource("system"), "c1-1"));
        EventObject eo2 = eventAdapter.saveEventObject(saveRequest(EventObject.newBuilder().setType("type1").setSource("system"), "c2-1"));
        EventObject eo3 = eventAdapter.saveEventObject(saveRequest(EventObject.newBuilder().setType("type1").setSource("source1").setState(State.OPEN), "c3-1"));
        EventObject eo4 = eventAdapter.saveEventObject(saveRequest(EventObject.newBuilder().setType("type1").setSource("system"), "c4-1"));
        EventObject eo5 = eventAdapter.saveEventObject(saveRequest(EventObject.newBuilder().setType("type1").setSource("source1"), "c5-1"));

        // Test list by kind
        ListRequest.Builder req1 = ListRequest.newBuilder();

        assertThat(eventAdapter.listEventObjects(req1.build()).stream())
                .containsExactly(eo5, eo4, eo3, eo2, eo1);

        // Test list by id
        ListRequest.Builder req2 = ListRequest.newBuilder()
                .addId(eo1.getId())
                .addId(eo3.getId());

        assertThat(eventAdapter.listEventObjects(req2.build()).stream())
                .containsExactlyInAnyOrder(eo1, eo3);

        // Test select returned fields
        ListRequest.Builder req4 = ListRequest.newBuilder();
        req4.getReturnedFieldsMaskBuilder()
                .addPaths("source")
                .addPaths("state");

        assertThat(eventAdapter.listEventObjects(req4.build()).stream())
                .isNotEmpty()
                .allMatch(c -> {
                    assertThat(c.getLabelCount()).isEqualTo(0);
                    assertThat(c.getDataCount()).isEqualTo(0);
                    assertThat(c.getActivityCount()).isEqualTo(0);
                    assertThat(c.getSource()).isNotEmpty();
                    assertThat(c.getState()).isEqualTo(State.NEW);
                    assertThat(c.getType()).isEmpty();
                    assertThat(c.getSeverity()).isEqualTo(Severity.INFO);
                    return true;
                });

        // Test list by template filter
        ListRequest.Builder req5 = ListRequest.newBuilder();
        req5.getQueryTemplateBuilder()
                .setSource("source1")
                .addLabel("label1")
                .setState(State.OPEN);

        ListRequest.Builder lr = req5.clone();
        lr.getQueryMaskBuilder().addPaths("source");

        assertThat(eventAdapter.listEventObjects(lr.build()).stream())
                .containsExactly(eo5, eo3);


        lr = req5.clone();
        lr.getQueryMaskBuilder().addPaths("label");

        assertThat(eventAdapter.listEventObjects(lr.build()).stream()).isEmpty();

        eo5 = eventAdapter.saveEventObject(saveRequest(eo5.toBuilder().addLabel("label1").addLabel("label2"), "c5-2"));
        EventObject eo6 = eventAdapter.saveEventObject(saveRequest(EventObject.newBuilder().setType("type1").setSource("system").addLabel("label1"), "c6-1"));

        assertThat(eventAdapter.listEventObjects(lr.build()).stream())
                .containsExactly(eo6, eo5);

        lr = req5.clone();
        lr.getQueryMaskBuilder().addPaths("state");

        assertThat(eventAdapter.listEventObjects(lr.build()).stream()).isEmpty();

        eo3 = eventAdapter.saveEventObject(saveRequest(eo3.toBuilder().setState(State.OPEN), "c3-2"));
        eo1 = eventAdapter.saveEventObject(saveRequest(eo1.toBuilder().setState(State.OPEN), "c1-2"));

        assertThat(eventAdapter.listEventObjects(lr.build()).stream())
                .containsExactly(eo1, eo3);

        // Test all options at once
        ListRequest.Builder req7 = ListRequest.newBuilder()
                .addId(eo5.getId());
        req7.getQueryTemplateBuilder()
                .addLabel("label2");
        req7.getQueryMaskBuilder().addPaths("label");
        assertThat(eventAdapter.listEventObjects(req7.build()).stream())
                .containsExactly(eo5);
    }

    @Test
    public void testCountEventObjects() throws Exception {
        eventAdapter.saveEventObject(saveRequest(EventObject.newBuilder().setType("type1").setSource("system"), ""));
        eventAdapter.saveEventObject(saveRequest(EventObject.newBuilder().setType("type1").setSource("system"), ""));
        eventAdapter.saveEventObject(saveRequest(EventObject.newBuilder().setType("type2").setSource("system"), ""));
        eventAdapter.saveEventObject(saveRequest(EventObject.newBuilder().setType("type1").setSource("system"), ""));

        assertThat(eventAdapter.countEventObjects(ListRequest.newBuilder()
                .build()).getCount()).isEqualTo(4);

        ListRequest.Builder r = ListRequest.newBuilder();
        r.getQueryMaskBuilder().addPaths("type");
        r.getQueryTemplateBuilder().setType("type1");
        assertThat(eventAdapter.countEventObjects(r.build()).getCount()).isEqualTo(3);
    }

    @Test
    public void testDelete() throws DbException {
        EventObject eo1 = eventAdapter.saveEventObject(saveRequest(EventObject.newBuilder().setType("type1").setSource("system"), ""));
        EventObject eo2 = eventAdapter.saveEventObject(saveRequest(EventObject.newBuilder().setType("type1").setSource("system"), ""));
        EventObject eo3 = eventAdapter.saveEventObject(saveRequest(EventObject.newBuilder().setType("type1").setSource("system"), ""));
        EventObject eo4 = eventAdapter.saveEventObject(saveRequest(EventObject.newBuilder().setType("type1").setSource("system"), ""));

        assertThat(eventAdapter.deleteEventObject(eo1).getDeleted()).isTrue();
        assertThat(eventAdapter.deleteEventObject(eo2).getDeleted()).isTrue();
        assertThat(eventAdapter.deleteEventObject(eo2).getDeleted()).isFalse();

        assertThat(eventAdapter.listEventObjects(ListRequest.newBuilder().build()).stream()).containsExactlyInAnyOrder(eo3, eo4);
    }

    @Test
    public void testUpdateEventObjects() throws Exception {
        String fooLabel = "foo";
        String bigLabel = "big";
        String aaaLabel = "aaa";

        EventObject eo1 = eventAdapter.saveEventObject(saveRequest(EventObject.newBuilder().setType("type1").setSource("system").addLabel(fooLabel), ""));
        eo1 = eventAdapter.saveEventObject(saveRequest(eo1.toBuilder().setState(State.OPEN), ""));
        EventObject eo2 = eventAdapter.saveEventObject(saveRequest(EventObject.newBuilder().setType("type1").setSource("system"), ""));
        EventObject eo3 = eventAdapter.saveEventObject(saveRequest(EventObject.newBuilder().setType("type1").setSource("system").addLabel(fooLabel).addLabel(aaaLabel), ""));
        EventObject eo4 = eventAdapter.saveEventObject(saveRequest(EventObject.newBuilder().setType("type1").setSource("system").setAssignee("user1"), ""));

        // Test add label to objects which already has 'foo' label
        UpdateRequest.Builder ur1 = UpdateRequest.newBuilder().setComment("c1");
        ur1.getListRequestBuilder().getQueryTemplateBuilder().addLabel(fooLabel);
        ur1.getListRequestBuilder().getQueryMaskBuilder().addPaths("label");
        ur1.getUpdateTemplateBuilder().addLabel(bigLabel);
        ur1.getUpdateMaskBuilder().addPaths("label+");
        try {
            assertThat(eventAdapter.updateEventObject(ur1.build()).getUpdated()).isEqualTo(2);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Check result
        ListRequest test1 = ListRequest.getDefaultInstance();

        assertThat(eventAdapter.listEventObjects(test1).stream())
                .allMatch(r -> {
                    if (r.getLabelList().contains(fooLabel)) {
                        return r.getLabelList().contains(bigLabel);
                    } else {
                        return !r.getLabelList().contains(bigLabel);
                    }
                });

        // Test remove aaa label and assignee from all objects + set state to open
        UpdateRequest.Builder ur2 = UpdateRequest.newBuilder().setComment("c2");
        ur2.getListRequestBuilder();
        ur2.getUpdateTemplateBuilder().addLabel(aaaLabel).setState(State.OPEN);
        ur2.getUpdateMaskBuilder()
                .addPaths("label-")
                .addPaths("assignee")
                .addPaths("state");

        // Set user to user1
        Context.current().withValues(EmailContextKey.getKey(), "user1", RolesContextKey.getKey(), null)
                .call(() -> assertThat(eventAdapter.updateEventObject(ur2.build()).getUpdated()).isEqualTo(3));

        // Check result
        assertThat(eventAdapter.listEventObjects(test1).stream())
                .allSatisfy(r -> {
                    assertThat(r.getLabelList()).doesNotContain(aaaLabel);
                    assertThat(r.getAssignee()).isEmpty();
                    assertThat(r.getState()).isEqualTo(State.OPEN);
                });

        // Repeat last update. No objects should actually change
        EventObject[] before = eventAdapter.listEventObjects(test1)
                .stream().toArray(EventObject[]::new);

        // Set user to user2
        Context.current().withValues(EmailContextKey.getKey(), "user2", RolesContextKey.getKey(), null)
                .call(() -> assertThat(eventAdapter.updateEventObject(ur2.build()).getUpdated()).isEqualTo(0));

        EventObject[] after = eventAdapter.listEventObjects(test1)
                .stream().toArray(EventObject[]::new);

        assertThat(before).contains(after);

        // Check activity for one event
        EventObject saved = eventAdapter.getEventObject(EventRef.newBuilder().setId(eo3.getId()).build());
        assertThat(saved).hasFieldOrPropertyWithValue("severity", Severity.INFO)
                .hasFieldOrPropertyWithValue("assignee", "")
                .hasFieldOrPropertyWithValue("source", "system")
                .hasFieldOrPropertyWithValue("type", "type1");
        assertThat(saved.getDataList()).hasSize(0);
        assertThat(saved.getLabelList()).hasSize(2).containsExactly(fooLabel, bigLabel);
        assertThat(saved.getActivityList()).hasSize(3);

        assertThat(saved.getActivity(0))
                .hasFieldOrPropertyWithValue("modifiedBy", "user1")
                .hasFieldOrPropertyWithValue("comment", "c2");
        assertThat(saved.getActivity(0).getDescriptionCount()).isEqualTo(2);
        assertThat(saved.getActivity(0).getDescription(0))
                .hasFieldOrPropertyWithValue("type", ChangeType.VALUE_CHANGED)
                .hasFieldOrPropertyWithValue("field", "state")
                .hasFieldOrPropertyWithValue("newVal", State.OPEN.name())
                .hasFieldOrPropertyWithValue("oldVal", "");
        assertThat(saved.getActivity(0).getDescription(1))
                .hasFieldOrPropertyWithValue("type", ChangeType.ARRAY_DEL)
                .hasFieldOrPropertyWithValue("field", "label")
                .hasFieldOrPropertyWithValue("newVal", "")
                .hasFieldOrPropertyWithValue("oldVal", aaaLabel);

        assertThat(saved.getActivity(1))
                .hasFieldOrPropertyWithValue("modifiedBy", "anonymous")
                .hasFieldOrPropertyWithValue("comment", "c1");
        assertThat(saved.getActivity(1).getDescriptionCount()).isEqualTo(1);
        assertThat(saved.getActivity(1).getDescription(0))
                .hasFieldOrPropertyWithValue("type", ChangeType.ARRAY_ADD)
                .hasFieldOrPropertyWithValue("field", "label")
                .hasFieldOrPropertyWithValue("newVal", bigLabel)
                .hasFieldOrPropertyWithValue("oldVal", "");

        assertThat(saved.getActivity(2))
                .hasFieldOrPropertyWithValue("modifiedBy", "anonymous")
                .hasFieldOrPropertyWithValue("comment", "");
        assertThat(saved.getActivity(2).getDescriptionCount()).isEqualTo(1);
        assertThat(saved.getActivity(2).getDescription(0))
                .hasFieldOrPropertyWithValue("type", ChangeType.CREATED)
                .hasFieldOrPropertyWithValue("field", "")
                .hasFieldOrPropertyWithValue("newVal", "")
                .hasFieldOrPropertyWithValue("oldVal", "");
    }

    @Test
    public void testGetLabelKeys() throws Exception {
        eventAdapter.saveEventObject(saveRequest(EventObject.newBuilder().setType("type1").setSource("system")
                .addLabel("abc").addLabel("foo"), ""));
        eventAdapter.saveEventObject(saveRequest(EventObject.newBuilder().setType("type1").setSource("system")
                .addLabel("FOO").addLabel("fooabc").addLabel("abcfoo"), ""));
        eventAdapter.saveEventObject(saveRequest(EventObject.newBuilder().setType("type1").setSource("system")
                .addLabel("12").addLabel("lkjss"), ""));
        eventAdapter.saveEventObject(saveRequest(EventObject.newBuilder().setType("type1").setSource("system")
                .addLabel("FOO").addLabel("pinkfooabc").addLabel("abcfo"), ""));

        assertThat(eventAdapter.listLabels(ListLabelRequest.newBuilder()
                .setText("foo").build()).getLabelList())
                .containsExactlyInAnyOrder("foo", "fooabc", "abcfoo", "pinkfooabc");
    }
}