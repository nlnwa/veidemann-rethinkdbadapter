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
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Timestamp;
import no.nb.nna.veidemann.api.commons.v1.FieldMask;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ObjectPathAccessor<T extends MessageOrBuilder> implements ObjectOrMask<T> {
    Map<String, PathElem<T>> referencePaths = new HashMap<>();
    private PathElem<T> masks = new PathElem(this, "", "");

    public ObjectPathAccessor(Class<? extends T> type) {
        try {
            Method n = type.getMethod("getDefaultInstance");
            T referenceObject = (T) n.invoke(null);
            createPaths(referenceObject);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public MaskedObject createForFieldMaskProto(FieldMask mask) {
        return new MaskedObject(this, mask);
    }

    public MaskedObject createForSingleMask(String mask) {
        return new MaskedObject(this, mask);
    }

    void createPaths(MessageOrBuilder object) {
        for (FieldDescriptor d : object.getDescriptorForType().getFields()) {
            createFieldPath(d, masks, "");
        }
    }

    void createFieldPath(FieldDescriptor field, PathElem parent, String prefix) {
        if (field.getType() == Type.MESSAGE && field.getMessageType().getFields().isEmpty()) {
            return;
        }

        String fullName = parent.fullName;
        if (!fullName.isEmpty()) {
            fullName += ".";
        }
        fullName += field.getJsonName();

        PathElem e = parent.getOrCreateChild(fullName, field.getJsonName());
        e.descriptor = field;

        referencePaths.put(e.fullName, e);
        if (field.isRepeated()) {
            e.isRepeated = true;
        } else if (field.getType() == Type.MESSAGE) {
            if (field.getMessageType().getFields().isEmpty()) {
                return;
            }
            if (field.getMessageType() == Timestamp.getDescriptor()) {
                return;
            }
            for (FieldDescriptor m : field.getMessageType().getFields()) {
                createFieldPath(m, e, m.getJsonName());
            }
        }
    }

    public FieldDescriptor getDescriptorForPath(String path) {
        return getPathDef(path).descriptor;
    }

    public PathElem<T> getPathDef(String path) {
        PathElem p = referencePaths.get(path);
        if (p == null) {
            throw new IllegalArgumentException("Illegal path: " + path);
        }
        return p;
    }

    public Collection<PathElem<T>> getPaths() {
        return referencePaths.values();
    }

    public PathElem getMasks() {
        return masks;
    }

    public Object getValue(String path, MessageOrBuilder object) {
        return getValue(getPathDef(path), object);
    }

    public Object getValue(PathElem p, MessageOrBuilder object) {
        if (!p.parent.name.isEmpty()) {
            object = (MessageOrBuilder) getValue(p.parent, object);
        }
        return object.getField(p.descriptor);
    }

    public T setValue(String path, T object, Object value) {
        return setValue(getPathDef(path), object, value);
    }

    public T setValue(PathElem<T> path, T object, Object value) {
        if (object instanceof Message.Builder) {
            innerSetValue(path, path, (Message.Builder) object, value);
            return object;
        } else {
            Message.Builder b = ((Message) object).toBuilder();
            innerSetValue(path, path, b, value);
            return (T) b.build();
        }
    }

    private MessageOrBuilder innerSetValue(PathElem destination, PathElem p, Message.Builder object, Object value) {
        if (!p.parent.name.isEmpty()) {
            object = (Message.Builder) innerSetValue(destination, p.parent, object, value);
        }

        if (p == destination) {
            if (p.descriptor.isRepeated()) {
                return object.addRepeatedField(p.descriptor, value);
            } else {
                return object.setField(p.descriptor, value);
            }
        } else {
            return object.getFieldBuilder(p.descriptor);
        }
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("FieldMasks{\n");
        sb.append("  masks:\n");
        masks.children.forEach(e -> sb.append("    ").append(e.regex()).append("\n"));
        sb.append("  paths:\n");
        referencePaths.forEach((k, v) -> sb.append("    ").append(k).append("\n"));
        sb.append('}');
        return sb.toString();
    }
}
