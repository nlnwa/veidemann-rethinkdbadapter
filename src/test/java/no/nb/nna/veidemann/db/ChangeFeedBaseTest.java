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
import com.rethinkdb.net.Cursor;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ChangeFeedBaseTest {
    static final RethinkDB r = RethinkDB.r;

    @Test
    public void stream() throws TimeoutException {
        Cursor<Map<String, Object>> cursorMock = mock(Cursor.class);
        when(cursorMock.hasNext())
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(false);
        when(cursorMock.next(anyLong()))
                .thenReturn(r.hashMap("id", "id1"))
                .thenReturn(r.hashMap("id", "id2").with("seed", r.hashMap("disabled", "100")));

        ChangeFeed<ConfigObject> cf = new ChangeFeedBase<ConfigObject>(cursorMock) {
            @Override
            protected Function<Map<String, Object>, ConfigObject> mapper() {
                return co -> {
                    ConfigObject res = ProtoUtils.rethinkToProto(co, ConfigObject.class);
                    return res;
                };
            }
        };

        // Expecting only one beacause one should fail
        assertThat(cf.stream()).hasSize(1);
    }
}