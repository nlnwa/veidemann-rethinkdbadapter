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

import com.google.protobuf.MessageOrBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class Indexes<T extends MessageOrBuilder> {
    Index primary;
    Map<String, List<Index>> indexes = new HashMap<>();

    protected void addIndex(String indexName, String... path) {
        for (String p : path) {
            indexes.computeIfAbsent(p, v -> new ArrayList<>()).add(new Index(indexName, false, path));
        }
    }

    protected void addIgnoreCaseIndex(String indexName, String... path) {
        for (String p : path) {
            indexes.computeIfAbsent(p, v -> new ArrayList<>()).add(new Index(indexName, false, 0, true, path));
        }
    }

    protected void addPrimaryIndex(String indexName, String path) {
        primary = new Index(indexName, true, path);
        indexes.computeIfAbsent(path, v -> new ArrayList<>()).add(primary);
    }

    protected Index getPrimary() {
        return primary;
    }

    public Index getBestIndex(ObjectOrMask<T> maskedObject) {
        Index candidate = null;
        for (PathElem<T> pe : maskedObject.getPaths()) {
            List<Index> idxList = indexes.get(pe.fullName);
            if (idxList == null) {
                continue;
            }
            int matchedElements = 0;
            for (Index idx : idxList) {
                if (matchedElements < idx.path.length) {
                    int m = 0;
                    for (String p : idx.path) {
                        if (maskedObject.getPathDef(p) != null) {
                            m++;
                        }
                    }
                    if (m > matchedElements) {
                        matchedElements = m;
                        candidate = idx;
                    }
                }
            }
            if (candidate != null) {
                break;
            }
        }
        return candidate;
    }

    public List<Index> getBestIndexes(ObjectOrMask<T> maskedObject) {
        String[] paths = maskedObject.getPaths().stream().map(p -> p.fullName).toArray(String[]::new);
        return getBestIndexes(paths);
    }

    public List<Index> getBestIndexes(String... path) {
        Set<Index> bestIndexes = new HashSet<>();
        List<Index> idxList = new ArrayList<>();
        for (String pe : path) {
            List<Index> l = indexes.get(pe);
            if (l == null) {
                continue;
            }
            idxList.addAll(l);
        }
        for (Index index : idxList) {
            if (index.isPrimary()) {
                idxList.clear();
                idxList.add(index);
                path = new String[]{index.path[0]};
                break;
            }
        }
        for (Index idx : idxList) {
            int m = 0;
            for (int i = 0; i < idx.path.length; i++) {
                String p = idx.path[i];
                if (path.length >= (i + 1) && p.equals(path[i])) {
                    m++;
                } else {
                    break;
                }
            }
            if (m > 0) {
                int priority = path.length + idx.path.length - 2 * m;
                bestIndexes.add(idx.withPriority(priority));
            }
        }
        return bestIndexes.stream().sorted(Comparator.comparingInt(o -> o.priority)).collect(Collectors.toList());
    }

    public static class Index {
        public final String indexName;
        public final String[] path;
        final boolean primary;
        int priority;
        final boolean ignoreCase;

        public Index(String indexName, boolean primary, String... path) {
            this(indexName, primary, 0, false, path);
        }

        public Index(String indexName, boolean primary, int priority, String... path) {
            this(indexName, primary, priority, false, path);
        }

        public Index(String indexName, boolean primary, int priority, boolean ignoreCase, String... path) {
            this.indexName = indexName;
            this.path = path;
            this.primary = primary;
            this.priority = priority;
            this.ignoreCase = ignoreCase;
        }

        public Index withPriority(int priority) {
            return new Index(this.indexName, this.primary, priority, ignoreCase, this.path);
        }

        public boolean isPrimary() {
            return primary;
        }

        public boolean isIgnoreCase() {
            return ignoreCase;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Index that = (Index) o;
            return priority == that.priority && indexName.equals(that.indexName) && Arrays.deepEquals(path, that.path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(priority, indexName, Arrays.hashCode(path));
        }

        @Override
        public String toString() {
            return "Index{" +
                    "indexName='" + indexName + '\'' +
                    ", path=" + Arrays.toString(path) +
                    ", priority=" + priority +
                    ", primary=" + primary +
                    ", ignoreCase=" + ignoreCase +
                    '}';
        }
    }
}
