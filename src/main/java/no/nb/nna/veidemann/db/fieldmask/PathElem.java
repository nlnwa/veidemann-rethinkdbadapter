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

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.MessageOrBuilder;
import no.nb.nna.veidemann.db.fieldmask.MaskedObject.UpdateType;
import org.apache.logging.log4j.util.Strings;

import java.util.ArrayList;
import java.util.List;

public class PathElem<T extends MessageOrBuilder> {
    private final ObjectPathAccessor<T> objectDef;
    String name;
    String fullName;
    boolean isRepeated;
    PathElem<T> parent;
    final List<PathElem<T>> children = new ArrayList<>();
    FieldDescriptor descriptor;
    UpdateType updateType;

    PathElem(ObjectPathAccessor<T> objectDef, String fullName, String name) {
        this.objectDef = objectDef;
        this.fullName = fullName;
        this.name = name;
    }

    public FieldDescriptor getDescriptor() {
        return descriptor;
    }

    public PathElem<T> getParent() {
        return parent;
    }

    public String getName() {
        return name;
    }

    public String getFullName() {
        return fullName;
    }

    private void addChild(PathElem<T> e) {
        e.parent = this;
        children.add(e);
    }

    PathElem<T> getOrCreateChild(String fullName, String name) {
        PathElem<T> e = null;
        for (PathElem<T> p : children) {
            if (p.name.equals(name)) {
                e = p;
                break;
            }
        }
        if (e == null) {
            e = new PathElem<>(objectDef, fullName, name);
            addChild(e);
        }
        return e;
    }

    public Object getValue(MessageOrBuilder object) {
        if (objectDef == null) {
            return null;
        }
        return objectDef.getValue(this, object);
    }

    public void setValue(T object, Object value) {
        if (objectDef == null) {
            return;
        }
        objectDef.setValue(this, object, value);
    }

    @Override
    public String toString() {
        return name;
    }

    public String regex() {
        final StringBuilder sb = new StringBuilder(name);
        if (isRepeated) {
            sb.append("+");
        }
        if (!children.isEmpty()) {
            sb.append(".(");
            sb.append(Strings.join(children, '|'));
            sb.append(")");
        }
        return sb.toString();
    }
}
