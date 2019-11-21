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

import com.rethinkdb.net.Cursor;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 *
 */
public abstract class ChangeFeedBase<T> implements ChangeFeed<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ChangeFeedBase.class);

    final CursorSpliterator<Map<String, Object>> it;
    final Stream<T> stream;

    public ChangeFeedBase(Cursor<Map<String, Object>> cursor) {
        this.it = new CursorSpliterator<>(cursor);
        this.stream = StreamSupport
                .stream(it, false)
                .onClose(() -> {
                    it.close();
                })
                .map(o -> {
                    try {
                        return mapper().apply(o);
                    } catch (Throwable e) {
                        LOG.error("Error mapping database object", e);
                        return null;
                    }
                })
                .filter(o -> o != null);
    }

    protected abstract Function<Map<String, Object>, T> mapper();

    @Override
    public Stream<T> stream() {
        return stream;
    }

    @Override
    public void close() {
        it.close();
    }

    private static class CursorSpliterator<T extends Map<String, Object>> implements Spliterator<T>, Closeable {
        private final Cursor<T> cursor;
        private boolean closed;

        public CursorSpliterator(Cursor<T> cursor) {
            this.cursor = cursor;
        }

        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            if (action == null) throw new NullPointerException();
            while (!closed && cursor.hasNext()) {
                try {
                    action.accept(cursor.next(2000));
                } catch (TimeoutException e) {
                    continue;
                }
                return true;
            }
            return false;
        }

        @Override
        public Spliterator<T> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return Long.MAX_VALUE;
        }

        @Override
        public int characteristics() {
            return 0;
        }

        @Override
        public void close() {
            this.closed = true;
            this.cursor.close();
        }
    }
}
