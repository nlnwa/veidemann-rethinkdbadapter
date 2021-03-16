/*
 * Copyright 2017 National Library of Norway.
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

import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.CrawlHostGroupConfig;
import no.nb.nna.veidemann.api.config.v1.CrawlHostGroupConfig.IpRange;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.config.v1.Label;
import no.nb.nna.veidemann.api.config.v1.Meta;
import no.nb.nna.veidemann.api.config.v1.Role;
import no.nb.nna.veidemann.api.config.v1.RoleMapping;
import org.junit.Test;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.util.Map;

import static com.rethinkdb.RethinkDB.r;
import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 */
public class ProtoUtilsTest {

    /**
     * Test of protoToRethink method, of class ProtobufUtils.
     */
    @Test
    public void testProtoToRethink() {
        ConfigObject msg = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(Kind.crawlHostGroupConfig)
                .setId("UUID")
                .setMeta(Meta.newBuilder()
                        .setName("Nasjonalbiblioteket")
                        .addLabel(Label.newBuilder()
                                .setKey("frequency")
                                .setValue("Daily"))
                        .addLabel(Label.newBuilder()
                                .setKey("orgType")
                                .setValue("Government"))
                        .setCreated(ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))))
                .setCrawlHostGroupConfig(CrawlHostGroupConfig.newBuilder()
                        .addIpRange(IpRange.newBuilder().setIpFrom("127.0.0.1").setIpTo("127.0.0.255"))
                        .setDelayFactor(.1f)
                        .setMaxRetries(5))
                .build();

        Map crawlHostGroupConfig = r.hashMap("id", "UUID")
                .with("apiVersion", "v1")
                .with("kind", "crawlHostGroupConfig")
                .with("meta", r.hashMap()
                        .with("name", "Nasjonalbiblioteket")
                        .with("created", OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))
                        .with("label", r.array(
                                r.hashMap("key", "frequency").with("value", "Daily"),
                                r.hashMap("key", "orgType").with("value", "Government")))
                )
                .with("crawlHostGroupConfig", r.hashMap()
                        .with("ipRange", r.array(r.hashMap("ipFrom", "127.0.0.1").with("ipTo", "127.0.0.255")))
                        .with("delayFactor", .1f)
                        .with("maxRetries", 5)
                );

        Map<String, Object> result = ProtoUtils.protoToRethink(msg);

        assertThat(result).isEqualTo(crawlHostGroupConfig);

        // Check conversion of object with list of enums
        Map roleMappingRethink = r.hashMap("email", "admin@example.com")
                .with("role", r.array(Role.ADMIN.name(), Role.CURATOR.name()));
        RoleMapping roleMappingProto = RoleMapping.newBuilder().setEmail("admin@example.com").addRole(Role.ADMIN).addRole(Role.CURATOR).build();
        assertThat(ProtoUtils.protoToRethink(roleMappingProto)).isEqualTo(roleMappingRethink);
    }

    /**
     * Test of rethinkToProto method, of class ProtobufUtils.
     */
    @Test
    public void testRethinkToProto_Map_Class() {
        ConfigObject expResult = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(Kind.crawlHostGroupConfig)
                .setId("UUID")
                .setMeta(Meta.newBuilder()
                        .setName("Nasjonalbiblioteket")
                        .addLabel(Label.newBuilder()
                                .setKey("frequency")
                                .setValue("Daily"))
                        .addLabel(Label.newBuilder()
                                .setKey("orgType")
                                .setValue("Government"))
                        .setCreated(ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))))
                .setCrawlHostGroupConfig(CrawlHostGroupConfig.newBuilder()
                        .addIpRange(IpRange.newBuilder().setIpFrom("127.0.0.1").setIpTo("127.0.0.255"))
                        .setDelayFactor(.1f)
                        .setMaxRetries(5))
                .build();

        Map crawlHostGroupConfig = r.hashMap("id", "UUID")
                .with("apiVersion", "v1")
                .with("kind", "crawlHostGroupConfig")
                .with("meta", r.hashMap()
                        .with("name", "Nasjonalbiblioteket")
                        .with("created", OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))
                        .with("label", r.array(
                                r.hashMap("key", "frequency").with("value", "Daily"),
                                r.hashMap("key", "orgType").with("value", "Government")))
                )
                .with("crawlHostGroupConfig", r.hashMap()
                        .with("ipRange", r.array(r.hashMap("ipFrom", "127.0.0.1").with("ipTo", "127.0.0.255")))
                        .with("delayFactor", .1f)
                        .with("maxRetries", 5)
                );

        ConfigObject result = ProtoUtils
                .rethinkToProto(crawlHostGroupConfig, ConfigObject.class);

        assertThat(result).isEqualTo(expResult);

        // Check conversion of object with list of enums
        Map roleMappingRethink = r.hashMap("email", "admin@example.com")
                .with("role", r.array(Role.ADMIN.name(), Role.CURATOR.name()));
        RoleMapping roleMappingProto = RoleMapping.newBuilder().setEmail("admin@example.com").addRole(Role.ADMIN).addRole(Role.CURATOR).build();
        assertThat(ProtoUtils.rethinkToProto(roleMappingRethink, RoleMapping.class)).isEqualTo(roleMappingProto);
    }

    /**
     * Test of rethinkToProto method, of class ProtobufUtils.
     */
    @Test
    public void testRethinkToProto_Map_MessageBuilder() {
        ConfigObject expResult = ConfigObject.newBuilder()
                .setApiVersion("v1")
                .setKind(Kind.crawlHostGroupConfig)
                .setId("UUID")
                .setMeta(Meta.newBuilder()
                        .setName("Nasjonalbiblioteket")
                        .addLabel(Label.newBuilder()
                                .setKey("frequency")
                                .setValue("Daily"))
                        .addLabel(Label.newBuilder()
                                .setKey("orgType")
                                .setValue("Government"))
                        .setCreated(ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))))
                .setCrawlHostGroupConfig(CrawlHostGroupConfig.newBuilder()
                        .addIpRange(IpRange.newBuilder().setIpFrom("127.0.0.1").setIpTo("127.0.0.255"))
                        .setDelayFactor(.1f)
                        .setMaxRetries(5))
                .build();

        Map crawlHostGroupConfig = r.hashMap("id", "UUID")
                .with("apiVersion", "v1")
                .with("kind", "crawlHostGroupConfig")
                .with("meta", r.hashMap()
                        .with("name", "Nasjonalbiblioteket")
                        .with("created", OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))
                        .with("label", r.array(
                                r.hashMap("key", "frequency").with("value", "Daily"),
                                r.hashMap("key", "orgType").with("value", "Government")))
                )
                .with("crawlHostGroupConfig", r.hashMap()
                        .with("ipRange", r.array(r.hashMap("ipFrom", "127.0.0.1").with("ipTo", "127.0.0.255")))
                        .with("delayFactor", .1f)
                        .with("maxRetries", 5)
                );

        Message result = ProtoUtils.rethinkToProto(crawlHostGroupConfig, ConfigObject.newBuilder());

        assertThat(result).isEqualTo(expResult);
    }

    /**
     * Test of timeStampToOffsetDateTime method, of class ProtobufUtils.
     */
    @Test
    public void testTsToOdt() {
        Instant now = Instant.now();
        now = now.with(ChronoField.NANO_OF_SECOND, 0);
        Timestamp timestamp = Timestamps.fromMillis(now.toEpochMilli());
        OffsetDateTime expResult = OffsetDateTime.ofInstant(now, ZoneOffset.UTC);

        OffsetDateTime result = ProtoUtils.tsToOdt(timestamp);
        assertThat(result).isEqualTo(expResult);
    }

    /**
     * Test of offsetDateTimeToTimeStamp method, of class ProtobufUtils.
     */
    @Test
    public void testOdtToTs() {
        Instant now = Instant.now();
        OffsetDateTime timestamp = OffsetDateTime.ofInstant(now, ZoneOffset.UTC);
        Timestamp expResult = Timestamps.fromMillis(now.toEpochMilli());

        Timestamp result = ProtoUtils.odtToTs(timestamp);
        assertThat(result).isEqualTo(expResult);
    }

}
