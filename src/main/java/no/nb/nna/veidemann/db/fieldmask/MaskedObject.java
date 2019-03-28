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
import no.nb.nna.veidemann.api.commons.v1.FieldMask;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class MaskedObject<T extends MessageOrBuilder> implements ObjectOrMask<T> {
    private final ObjectPathAccessor<T> objectDef;
    private final PathElem<T> masks;
    private Map<String, PathElem<T>> paths = new HashMap<>();

    private MaskedObject(ObjectPathAccessor<T> objectDef) {
        this.objectDef = objectDef;
        masks = new PathElem(objectDef, "", "");
    }

    MaskedObject(ObjectPathAccessor<T> objectDef, FieldMask mask) {
        this(objectDef);

        // Reduce paths to only contain the shortest unique paths
        Set<String> nonOverlappingMasks = new HashSet<>();
        for (String p : mask.getPathsList()) {
            boolean shouldAdd = true;
            for (Iterator<String> it = nonOverlappingMasks.iterator(); it.hasNext(); ) {
                String em = it.next();
                if (p.startsWith(em)) {
                    shouldAdd = false;
                    break;
                }
                if (em.startsWith(p)) {
                    it.remove();
                }
            }
            if (shouldAdd) {
                nonOverlappingMasks.add(p);
            }
        }

        for (String p : nonOverlappingMasks) {
            parseMask(p);
        }
    }

    MaskedObject(ObjectPathAccessor<T> objectDef, String mask) {
        this(objectDef);
        parseMask(mask);
    }

    private void parseMask(String mask) {
        UpdateType updateType = UpdateType.REPLACE;
        if (mask.endsWith("+")) {
            mask = mask.substring(0, mask.length() - 1);
            updateType = UpdateType.APPEND;
        } else if (mask.endsWith("-")) {
            mask = mask.substring(0, mask.length() - 1);
            updateType = UpdateType.DELETE;
        }

        if (!objectDef.referencePaths.containsKey(mask)) {
            throw new IllegalArgumentException("Illegal fieldmask path: " + mask);
        }

        String tokens[] = mask.split("\\.");
        String fullName = "";
        PathElem e = masks;
        for (String token : tokens) {
            fullName += token;
            e = e.getOrCreateChild(fullName, token);
            e.descriptor = objectDef.getDescriptorForPath(fullName);
            fullName += ".";
        }
        if (e != null) {
            if (e.descriptor.isRepeated()) {
                e.updateType = updateType;
            }
            e.children.addAll(objectDef.getPathDef(e.fullName).children);
            paths.put(e.fullName, e);
        }
    }

    public ObjectPathAccessor getObjectDef() {
        return objectDef;
    }

    public PathElem<T> getMasks() {
        return masks;
    }

    public PathElem<T> getPathDef(String path) {
        PathElem p = paths.get(path);
        if (p != null) {
            return p;
        }

        if (!objectDef.referencePaths.containsKey(path)) {
            throw new IllegalArgumentException("Illegal fieldmask path: " + path);
        } else {
            return null;
        }
    }

    public Collection<PathElem<T>> getPaths() {
        return paths.values();
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("FieldMasks{\n");
        sb.append("  masks:\n");
        masks.children.forEach(e -> sb.append("    ").append(e.regex()).append("\n"));
        sb.append("  paths:\n");
        paths.forEach((k, v) -> sb.append("    ").append(k).append("\n"));
        sb.append('}');
        return sb.toString();
    }

    enum UpdateType {
        REPLACE,
        APPEND,
        DELETE
    }

}
