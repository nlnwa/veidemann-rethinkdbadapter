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
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    final Stream<T> stream;

    /**
     * Creates a change feed based on a query result from RethinkDB.
     *
     * Since RethinkDB returns Cursor or List depending on query, this constructor takes an object which can be either
     * Cursor<Map<String, Object>> or List<Map<String, Object>>. Everything else will throw a ClassCastException.
     *
     * @param cursor the result from RethinkDB to embed.
     */
    public ChangeFeedBase(Object cursor) {
        if(cursor instanceof Cursor) {
            stream = init((Cursor<Map<String, Object>>) cursor);
        } else {
            stream = init((List<Map<String, Object>>) cursor);
        }
    }

    private Stream<T> init(Cursor<Map<String, Object>> cursor) {
        CursorSpliterator<Map<String, Object>> it = new CursorSpliterator<>(cursor);
        return StreamSupport
                .stream(it, false)
                .onClose(it::close)
                .map(o -> {
                    try {
                        return mapper().apply(o);
                    } catch (Throwable e) {
                        LOG.error("Error mapping database object", e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .distinct();
    }

    private Stream<T> init(List<Map<String, Object>> cursor) {
        Spliterator<Map<String, Object>> it = cursor.spliterator();
        return StreamSupport
                .stream(it, false)
                .map(o -> {
                    try {
                        return mapper().apply(o);
                    } catch (Throwable e) {
                        LOG.error("Error mapping database object", e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .distinct();
    }

    protected abstract Function<Map<String, Object>, T> mapper();

    @Override
    public Stream<T> stream() {
        return stream;
    }

    @Override
    public void close() {
        stream.close();
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
