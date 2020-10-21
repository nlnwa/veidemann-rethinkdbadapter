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

package no.nb.nna.veidemann.db.fieldmask;

import no.nb.nna.veidemann.api.commons.v1.FieldMask;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigObjectOrBuilder;
import no.nb.nna.veidemann.api.config.v1.Kind;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.junit.Test;

import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class ObjectPathAccessorTest {

    @Test
    public void createForFieldMaskProto() {
        FieldMask m = FieldMask.newBuilder()
                .addPaths("meta.name")
                .addPaths("meta.foo.bar")
                .addPaths("test").build();

        ObjectPathAccessor fmd = new ObjectPathAccessor(ConfigObject.class);
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> fmd.createForFieldMaskProto(m))
                .withMessage("Illegal fieldmask path: meta.foo.bar");
    }

    @Test
    public void getValue() {
        ConfigObject.Builder co = ConfigObject.newBuilder().setApiVersion("v1").setKind(Kind.browserConfig);
        co.getBrowserConfigBuilder().setUserAgent("agent");

        ObjectPathAccessor fmd = new ObjectPathAccessor(ConfigObject.class);

        assertThat(fmd.getValue("apiVersion", co.build())).isEqualTo("v1");
        assertThat(fmd.getValue("meta.name", co.build())).isEqualTo("");
        assertThat(fmd.getValue("browserConfig.userAgent", co.build())).isEqualTo("agent");
        assertThat(fmd.getPathDef("apiVersion").getValue(co.build())).isEqualTo("v1");
        assertThat(fmd.getPathDef("meta.name").getValue(co.build())).isEqualTo("");
        assertThat(fmd.getPathDef("browserConfig.userAgent").getValue(co.build())).isEqualTo("agent");

        FieldMask m = FieldMask.newBuilder()
                .addPaths("meta.name")
                .addPaths("apiVersion")
                .addPaths("browserConfig.windowHeight")
                .build();

        MaskedObject fm = fmd.createForFieldMaskProto(m);

        assertThat(fm.getPathDef("apiVersion").getValue(co.build())).isEqualTo("v1");
        assertThat(fm.getPathDef("meta.name").getValue(co.build())).isEqualTo("");
        assertThat(fm.getPathDef("browserConfig.userAgent")).isNull();
        assertThat(fm.getPathDef("browserConfig.windowHeight").getValue(co.build())).isEqualTo(0);
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> fm.getPathDef("browserConfig.foo").getValue(co.build()))
                .withMessage("Illegal fieldmask path: browserConfig.foo");
    }

    @Test
    public void setValue() {
        ConfigObject.Builder co = ConfigObject.newBuilder().setApiVersion("v1").setKind(Kind.browserConfig);

        FieldMask m = FieldMask.newBuilder()
                .addPaths("meta.name")
                .addPaths("apiVersion")
                .build();

        ObjectPathAccessor<ConfigObjectOrBuilder> fmd = new ObjectPathAccessor(ConfigObject.class);

        Object o = fmd.setValue("meta.name", co, "foo");
        assertThat(o).isExactlyInstanceOf(ConfigObject.Builder.class)
                .hasFieldOrPropertyWithValue("apiVersion", "v1")
                .hasFieldOrPropertyWithValue("kind", Kind.browserConfig)
                .hasFieldOrPropertyWithValue("meta.name", "foo")
                .extracting(c -> ((ConfigObjectOrBuilder) c).hasBrowserConfig(), as(InstanceOfAssertFactories.BOOLEAN)).isFalse();

        o = fmd.setValue("meta.name", co.build(), "foo");
        assertThat(o).isExactlyInstanceOf(ConfigObject.class)
                .hasFieldOrPropertyWithValue("apiVersion", "v1")
                .hasFieldOrPropertyWithValue("kind", Kind.browserConfig)
                .hasFieldOrPropertyWithValue("meta.name", "foo")
                .extracting(c -> ((ConfigObjectOrBuilder) c).hasBrowserConfig(), as(InstanceOfAssertFactories.BOOLEAN)).isFalse();

        ConfigObjectOrBuilder cob = (ConfigObjectOrBuilder) o;

        cob = fmd.setValue("browserConfig.userAgent", cob, "agent");
        assertThat(cob).isExactlyInstanceOf(ConfigObject.class)
                .hasFieldOrPropertyWithValue("apiVersion", "v1")
                .hasFieldOrPropertyWithValue("kind", Kind.browserConfig)
                .hasFieldOrPropertyWithValue("meta.name", "foo")
                .hasFieldOrPropertyWithValue("browserConfig.userAgent", "agent")
                .extracting(c -> ((ConfigObjectOrBuilder) c).hasBrowserConfig(), as(InstanceOfAssertFactories.BOOLEAN)).isTrue();

        cob = fmd.setValue("browserConfig.windowWidth", cob, 100);
        assertThat(cob).isExactlyInstanceOf(ConfigObject.class)
                .hasFieldOrPropertyWithValue("apiVersion", "v1")
                .hasFieldOrPropertyWithValue("kind", Kind.browserConfig)
                .hasFieldOrPropertyWithValue("meta.name", "foo")
                .hasFieldOrPropertyWithValue("browserConfig.userAgent", "agent")
                .hasFieldOrPropertyWithValue("browserConfig.windowWidth", 100)
                .extracting(c -> ((ConfigObjectOrBuilder) c).hasBrowserConfig(), as(InstanceOfAssertFactories.BOOLEAN)).isTrue();

        cob = fmd.setValue("browserConfig.windowWidth", cob, 100);
        assertThat(cob).isExactlyInstanceOf(ConfigObject.class)
                .hasFieldOrPropertyWithValue("apiVersion", "v1")
                .hasFieldOrPropertyWithValue("kind", Kind.browserConfig)
                .hasFieldOrPropertyWithValue("meta.name", "foo")
                .hasFieldOrPropertyWithValue("browserConfig.userAgent", "agent")
                .hasFieldOrPropertyWithValue("browserConfig.windowWidth", 100)
                .extracting(c -> ((ConfigObjectOrBuilder) c).hasBrowserConfig(), as(InstanceOfAssertFactories.BOOLEAN)).isTrue();
    }

}