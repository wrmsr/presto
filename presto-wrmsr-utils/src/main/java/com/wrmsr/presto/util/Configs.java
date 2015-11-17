/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wrmsr.presto.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.*;
import com.google.common.collect.Lists;
import com.wrmsr.presto.util.collect.ImmutableCollectors;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationMap;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.configuration.tree.ConfigurationNode;
import org.apache.commons.configuration.tree.DefaultExpressionEngine;
import org.apache.commons.configuration.tree.ExpressionEngine;
import org.apache.commons.configuration.tree.NodeAddData;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.wrmsr.presto.util.Primitives.toBool;

/*
___________________________________________________
@@@@@@@@@@@@@@@@@@@@@**^^""~~~"^@@^*@*@@**@@@@@@@@@
@@@@@@@@@@@@@*^^'"~   , - ' '; ,@@b. '  -e@@@@@@@@@
@@@@@@@@*^"~      . '     . ' ,@@@@(  e@*@@@@@@@@@@
@@@@@^~         .       .   ' @@@@@@, ~^@@@@@@@@@@@
@@@~ ,e**@@*e,  ,e**e, .    ' '@@@@@@e,  "*@@@@@'^@
@',e@@@@@@@@@@ e@@@@@@       ' '*@@@@@@    @@@'   0
@@@@@@@@@@@@@@@@@@@@@',e,     ;  ~^*^'    ;^~   ' 0
@@@@@@@@@@@@@@@^""^@@e@@@   .'           ,'   .'  @
@@@@@@@@@@@@@@'    '@@@@@ '         ,  ,e'  .    ;@
@@@@@@@@@@@@@' ,&&,  ^@*'     ,  .  i^"@e, ,e@e  @@
@@@@@@@@@@@@' ,@@@@,          ;  ,& !,,@@@e@@@@ e@@
@@@@@,~*@@*' ,@@@@@@e,   ',   e^~^@,   ~'@@@@@@,@@@
@@@@@@, ~" ,e@@@@@@@@@*e*@*  ,@e  @@""@e,,@@@@@@@@@
@@@@@@@@ee@@@@@@@@@@@@@@@" ,e@' ,e@' e@@@@@@@@@@@@@
@@@@@@@@@@@@@@@@@@@@@@@@" ,@" ,e@@e,,@@@@@@@@@@@@@@
@@@@@@@@@@@@@@@@@@@@@@@~ ,@@@,,0@@@@@@@@@@@@@@@@@@@
@@@@@@@@@@@@@@@@@@@@@@@@,,@@@@@@@@@@@@@@@@@@@@@@@@@
"""""""""""""""""""""""""""""""""""""""""""""""""""

overhaul this fucker
*/
public class Configs
{
    private Configs()
    {
    }

    protected static HierarchicalConfiguration toHierarchical(Map<String, String> properties)
    {
        return toHierarchical(new MapConfiguration(properties));
    }

    public static final String IS_LIST_ATTR = "__is_list__";

    protected static class ListAnnotatingHierarchicalConfiguration extends HierarchicalConfiguration
    {
        public ListAnnotatingHierarchicalConfiguration()
        {
            super.setExpressionEngine(new ListPreservingDefaultExpressionEngine());
        }

        public ListAnnotatingHierarchicalConfiguration(HierarchicalConfiguration c)
        {
            super(c);
            super.setExpressionEngine(new ListPreservingDefaultExpressionEngine());
        }

        @Override
        public void setExpressionEngine(ExpressionEngine expressionEngine)
        {
            throw new IllegalStateException();
        }

        public ListPreservingDefaultExpressionEngine getExpressionEngine()
        {
            return (ListPreservingDefaultExpressionEngine) Preconditions.checkNotNull(super.getExpressionEngine());
        }

        @Override
        protected void addPropertyDirect(String key, Object obj)
        {
            ListPreservingDefaultExpressionEngine.NodeAddData data = getExpressionEngine().prepareAdd(getRootNode(), key);
            ConfigurationNode node = processNodeAddData(data);
            node.setValue(obj);
            for (String k : data.getListAttributes()) {
                String p = k + new ListPreservingDefaultConfigurationKey(getExpressionEngine()).constructAttributeKey(IS_LIST_ATTR);
                if (!containsKey(p)) {
                    addProperty(p, true);
                }
            }
        }

        public void addNodes(String key, Collection<? extends ConfigurationNode> nodes)
        {
            throw new IllegalStateException();

            /*
            if (nodes == null || nodes.isEmpty())
            {
                return;
            }

            fireEvent(EVENT_ADD_NODES, key, nodes, true);
            ConfigurationNode parent;
            List<ConfigurationNode> target = fetchNodeList(key);
            if (target.size() == 1)
            {
                // existing unique key
                parent = target.get(0);
            }
            else
            {
                // otherwise perform an add operation
                parent = processNodeAddData(getExpressionEngine().prepareAdd(
                        getRootNode(), key));
            }

            if (parent.isAttribute())
            {
                throw new IllegalArgumentException(
                        "Cannot add nodes to an attribute node!");
            }

            for (ConfigurationNode child : nodes)
            {
                if (child.isAttribute())
                {
                    parent.addAttribute(child);
                }
                else
                {
                    parent.addChild(child);
                }
                clearReferences(child);
            }
            fireEvent(EVENT_ADD_NODES, key, nodes, false);
            */
        }


        private ConfigurationNode processNodeAddData(NodeAddData data)
        {
            ConfigurationNode node = data.getParent();

            // Create missing nodes on the path
            for (String name : data.getPathNodes()) {
                ConfigurationNode child = createNode(name);
                node.addChild(child);
                node = child;
            }

            // Add new target node
            ConfigurationNode child = createNode(data.getNewNodeName());
            if (data.isAttribute()) {
                node.addAttribute(child);
            }
            else {
                node.addChild(child);
            }
            return child;
        }

    }

    protected static HierarchicalConfiguration toHierarchical(Configuration conf)
    {
        if (conf == null) {
            return null;
        }

        if (conf instanceof HierarchicalConfiguration) {
            HierarchicalConfiguration hc = (HierarchicalConfiguration) conf;
            checkArgument(hc.getExpressionEngine() instanceof DefaultExpressionEngine);
            return hc;
        }
        else {
            HierarchicalConfiguration hc = new ListAnnotatingHierarchicalConfiguration();

            // Workaround for problem with copy()
            boolean delimiterParsingStatus = hc.isDelimiterParsingDisabled();
            hc.setDelimiterParsingDisabled(true);
            hc.append(conf);
            hc.setDelimiterParsingDisabled(delimiterParsingStatus);
            return hc;
        }
    }

    @SuppressWarnings({"unchecked"})
    protected static Map<String, Object> unpackHierarchical(HierarchicalConfiguration config)
    {
        if (!config.getRoot().hasChildren()) {
            return ImmutableMap.of();
        }
        return (Map<String, Object>) unpackNode(config.getRootNode());
    }

    protected static Object unpackNode(ConfigurationNode node)
    {
        List<ConfigurationNode> children = node.getChildren();
        if (!children.isEmpty()) {
            Map<String, List<ConfigurationNode>> namedChildren = new HashMap<>();
            for (ConfigurationNode child : children) {
                if (namedChildren.containsKey(child.getName())) {
                    namedChildren.get(child.getName()).add(child);
                }
                else {
                    namedChildren.put(child.getName(), com.google.common.collect.Lists.newArrayList(child));
                }
            }
            Map ret = new HashMap<>();
            for (Map.Entry<String, List<ConfigurationNode>> e : namedChildren.entrySet()) {
                List<ConfigurationNode> l = e.getValue();
                checkState(!l.isEmpty());
                boolean hasListAtt = l.stream().flatMap(n -> n.getAttributes(IS_LIST_ATTR).stream()).map(o -> toBool(o)).findFirst().isPresent();
                Object no;
                if (l.size() > 1 || hasListAtt) {
                    no = l.stream().map(n -> unpackNode(n)).filter(o -> o != null).collect(ImmutableCollectors.toImmutableList());
                }
                else {
                    no = unpackNode(l.get(0));
                }
                ret.put(e.getKey(), no);
            }
            if (ret.size() == 1 && ret.containsKey("")) {
                return ret.get("");
            }
            else {
                return ret;
            }
        }
        return node.getValue();
    }

    public static Map<String, String> stripSubconfig(Map<String, String> properties, String prefix)
    {
        HierarchicalConfiguration hierarchicalProperties = toHierarchical(properties);
        Configuration subconfig;
        try {
            subconfig = hierarchicalProperties.configurationAt(prefix);
        }
        catch (IllegalArgumentException e) {
            return ImmutableMap.of();
        }

        Map<String, String> subproperties = new ConfigurationMap(subconfig).entrySet().stream()
                .collect(ImmutableCollectors.toImmutableMap(e -> checkNotNull(e.getKey()).toString(), e -> checkNotNull(e.getValue()).toString()));

        hierarchicalProperties.clearTree(prefix);
        for (String key : Sets.newHashSet(properties.keySet())) {
            if (!hierarchicalProperties.containsKey(key)) {
                properties.remove(key);
            }
        }

        return subproperties;
    }

    public static List<String> getAllStrings(HierarchicalConfiguration hc, String key)
    {
        List<String> values = hc.getList(key).stream().filter(o -> o != null).map(Object::toString).collect(Collectors.toList());
        try {
            HierarchicalConfiguration subhc = hc.configurationAt(key);
            for (String subkey : Lists.newArrayList(subhc.getKeys())) {
                if (!isNullOrEmpty(subkey)) {
                    String subvalue = subhc.getString(subkey);
                    if (subvalue != null) {
                        values.add(subvalue);
                    }
                }
            }
        }
        catch (IllegalArgumentException e) {
            // pass
        }
        return values;
    }

    public static final int LIST_BASE = 0;
    public static final String LIST_START = "(";
    public static final String LIST_END = ")";
    public static final String FIELD_SEPERATOR = ".";

    protected static abstract class Sigil
    {
        public String render()
        {
            StringBuilder sb = new StringBuilder();
            render(sb);
            return sb.toString();
        }

        public abstract void render(StringBuilder sb);
    }

    protected static final class ListItemSigil extends Sigil
    {
        private final int index;
        private final Sigil next;

        public ListItemSigil(int index, Sigil next)
        {
            this.index = index;
            this.next = next;
        }

        @Override
        public void render(StringBuilder sb)
        {
            sb.append(LIST_START);
            sb.append(index);
            sb.append(LIST_END);
            if (!(next instanceof TerminalSigil)) {
                sb.append(FIELD_SEPERATOR);
            }
            next.render(sb);
        }
    }

    protected static final class MapEntrySigil extends Sigil
    {
        private final String key;
        private final Sigil next;

        public MapEntrySigil(String key, Sigil next)
        {
            this.key = key;
            this.next = next;
        }

        @Override
        public void render(StringBuilder sb)
        {
            sb.append(key);
            if (next instanceof MapEntrySigil) {
                sb.append(FIELD_SEPERATOR);
            }
            next.render(sb);
        }
    }

    protected static final class TerminalSigil extends Sigil
    {
        private TerminalSigil()
        {
        }

        public static final TerminalSigil INSTANCE = new TerminalSigil();

        @Override
        public void render(StringBuilder sb)
        {
        }
    }

    protected static Map<Sigil, String> flattenList(List<?> list)
    {
        return IntStream.range(0, list.size()).boxed()
                .flatMap(i -> flattenValues(list.get(i)).entrySet().stream()
                            .map(e -> ImmutablePair.of(new ListItemSigil(i, e.getKey()), e.getValue())))
                .collect(ImmutableCollectors.toImmutableMap(e -> e.getKey(), e -> e.getValue()));
    }

    protected static Map<Sigil, String> flattenMap(Map<?, ?> map)
    {
        return map.entrySet().stream()
                .flatMap(e -> {
                    String key = Objects.toString(e.getKey());
                    return flattenValues(e.getValue()).entrySet().stream()
                            .map(e2 -> ImmutablePair.of(new MapEntrySigil(key, e2.getKey()), e2.getValue()));
                })
                .collect(ImmutableCollectors.toImmutableMap(e -> e.getKey(), e -> e.getValue()));
    }

    protected static Map<Sigil, String> flattenValues(Object object)
    {
        if (object instanceof List) {
            return flattenList((List) object);
        }
        else if (object instanceof Map) {
            return flattenMap((Map) object);
        }
        else {
            return ImmutableMap.of(TerminalSigil.INSTANCE, Objects.toString(object));
        }
    }

    protected static Map<String, String> flatten(Object object)
    {
        return flattenValues(object).entrySet().stream()
                .collect(ImmutableCollectors.toImmutableMap(e -> e.getKey().render(), e -> e.getValue()));
    }

//    @SuppressWarnings({"unchecked"})
//    public static Map<String, String> flattenValues(Object value)
//    {
//        /*
//        TODO:
//        !!binary	byte[], String
//        !!timestamp	java.util.Date, java.sql.Date, java.sql.Timestamp
//        !!omap, !!pairs	List of Object[]
//        */
//        Map<String, String> map = newHashMap();
//        if (value instanceof Map) {
//            Map<String, Object> mapValue = (Map<String, Object>) value;
//            return mapValue.entrySet().stream()
//                    .flatMap(e -> flattenValues((key != null ? key + FIELD_SEPERATOR : "") + e.getKey(), e.getValue()).entrySet().stream())
//                    .filter(e -> e.getValue() != null)
//                    .collect(ImmutableCollectors.toImmutableMap(e -> e.getKey(), e -> e.getValue()));
//        }
//        else if (value instanceof List || value instanceof Set) {
//            List<Object> listValue = ImmutableList.copyOf((Iterable<Object>) value);
//            Map<String, String> flattened = IntStream.range(0, listValue.size()).boxed()
//                .flatMap(i -> {
//                    String subkey = LIST_START + Integer.toString(i + LIST_BASE) + LIST_END; // FIXME expressionengine
//                    return flattenValues(subkey, listValue.get(i)).entrySet().stream();
//                })
//                .collect(ImmutableCollectors.toImmutableMap(e -> e.getKey(), e -> e.getValue()));
//            return flattenValues(key, flattened).entrySet().stream()
//                    .map(e -> {
//                        checkState(e.getKey().startsWith(FIELD_SEPERATOR));
//                        return ImmutablePair.of(e.getKey().substring(1), e.getValue());
//                    })
//                    .collect(ImmutableCollectors.toImmutableMap(e -> e.getKey(), e -> e.getValue()));
//        }
//        else {
//            return ImmutableMap.of(key, value.toString());
//        }
//    }
//
//    public static Map<String, String> flattenValues(Object value)
//    {
//        return flattenValues(null, value);
//    }

    // FIXME rm
    public static Map<String, String> loadByExtension(byte[] data, String extension)
    {
        if (extension == "properties") {
            Properties properties = new Properties();
            try {
                properties.load(new StringReader(new String(data)));
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
            return properties.entrySet().stream()
                    .filter(e -> e.getValue() != null)
                    .collect(ImmutableCollectors.toImmutableMap(e -> e.getKey().toString(), e -> e.getValue().toString()));
        }
        else if (Serialization.OBJECT_MAPPERS_BY_EXTENSION.containsKey(extension)) {
            ObjectMapper objectMapper = Serialization.OBJECT_MAPPERS_BY_EXTENSION.get(extension).get();
            Object value;
            try {
                value = objectMapper.readValue(data, Object.class);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
            return flatten(value);
        }
        else {
            throw new IllegalArgumentException(String.format("Unhandled config extension: %s", extension));
        }
    }

    // FIXME invert?
    public static final Codecs.Codec<HierarchicalConfiguration, Map<String, String>> CONFIG_PROPERTIES_CODEC = Codecs.Codec.of(
            hc -> flatten(Configs.unpackHierarchical(hc)),
            m -> toHierarchical(m));

    public static final class ObjectConfigCodec implements Codecs.Codec<Object, HierarchicalConfiguration>
    {
        @Override
        public HierarchicalConfiguration encode(Object data)
        {
            return toHierarchical(flatten(data));
        }

        @Override
        public Object decode(HierarchicalConfiguration data)
        {
            return unpackHierarchical(data);
        }

        public <T> T decode(HierarchicalConfiguration data, Class<T> clazz)
        {
            ObjectMapper mapper = Serialization.OBJECT_MAPPER.get();
            try {
                byte[] bytes = mapper.writeValueAsBytes(decode(data));
                return mapper.readValue(bytes, clazz);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    public static final ObjectConfigCodec OBJECT_CONFIG_CODEC = new ObjectConfigCodec();

    protected static class ListPreservingDefaultConfigurationKey
    {
        private static final int INITIAL_SIZE = 32;
        private ListPreservingDefaultExpressionEngine expressionEngine;
        private StringBuilder keyBuffer;

        public ListPreservingDefaultConfigurationKey(ListPreservingDefaultExpressionEngine engine)
        {
            keyBuffer = new StringBuilder(INITIAL_SIZE);
            setExpressionEngine(engine);
        }

        public ListPreservingDefaultConfigurationKey(ListPreservingDefaultExpressionEngine engine, String key)
        {
            setExpressionEngine(engine);
            keyBuffer = new StringBuilder(trim(key));
        }

        public ListPreservingDefaultExpressionEngine getExpressionEngine()
        {
            return expressionEngine;
        }

        public void setExpressionEngine(ListPreservingDefaultExpressionEngine expressionEngine)
        {
            if (expressionEngine == null) {
                throw new IllegalArgumentException("Expression engine must not be null!");
            }
            this.expressionEngine = expressionEngine;
        }

        public ListPreservingDefaultConfigurationKey append(String property, boolean escape)
        {
            String key;
            if (escape && property != null) {
                key = escapeDelimiters(property);
            }
            else {
                key = property;
            }
            key = trim(key);

            if (keyBuffer.length() > 0 && !isAttributeKey(property) && key.length() > 0) {
                keyBuffer.append(getExpressionEngine().getPropertyDelimiter());
            }

            keyBuffer.append(key);
            return this;
        }

        public ListPreservingDefaultConfigurationKey append(String property)
        {
            return append(property, false);
        }

        public ListPreservingDefaultConfigurationKey appendIndex(int index)
        {
            keyBuffer.append(getExpressionEngine().getIndexStart());
            keyBuffer.append(index);
            keyBuffer.append(getExpressionEngine().getIndexEnd());
            return this;
        }

        public ListPreservingDefaultConfigurationKey appendAttribute(String attr)
        {
            keyBuffer.append(constructAttributeKey(attr));
            return this;
        }

        public int length()
        {
            return keyBuffer.length();
        }

        public void setLength(int len)
        {
            keyBuffer.setLength(len);
        }

        @Override
        public boolean equals(Object c)
        {
            if (c == null) {
                return false;
            }

            return keyBuffer.toString().equals(c.toString());
        }

        @Override
        public int hashCode()
        {
            return String.valueOf(keyBuffer).hashCode();
        }

        @Override
        public String toString()
        {
            return keyBuffer.toString();
        }

        public boolean isAttributeKey(String key)
        {
            if (key == null) {
                return false;
            }
            return key.startsWith(getExpressionEngine().getAttributeStart()) && (getExpressionEngine().getAttributeEnd() == null || key.endsWith(getExpressionEngine().getAttributeEnd()));
        }

        public String constructAttributeKey(String key)
        {
            if (key == null) {
                return StringUtils.EMPTY;
            }
            if (isAttributeKey(key)) {
                return key;
            }
            else {
                StringBuilder buf = new StringBuilder();
                buf.append(getExpressionEngine().getAttributeStart()).append(key);
                if (getExpressionEngine().getAttributeEnd() != null) {
                    buf.append(getExpressionEngine().getAttributeEnd());
                }
                return buf.toString();
            }
        }

        public String attributeName(String key)
        {
            return isAttributeKey(key) ? removeAttributeMarkers(key) : key;
        }

        public String trimLeft(String key)
        {
            if (key == null) {
                return StringUtils.EMPTY;
            }
            else {
                String result = key;
                while (hasLeadingDelimiter(result)) {
                    result = result.substring(getExpressionEngine().getPropertyDelimiter().length());
                }
                return result;
            }
        }

        public String trimRight(String key)
        {
            if (key == null) {
                return StringUtils.EMPTY;
            }
            else {
                String result = key;
                while (hasTrailingDelimiter(result)) {
                    result = result.substring(0, result.length() - getExpressionEngine().getPropertyDelimiter().length());
                }
                return result;
            }
        }

        public String trim(String key)
        {
            return trimRight(trimLeft(key));
        }

        public KeyIterator iterator()
        {
            return new KeyIterator();
        }

        private boolean hasTrailingDelimiter(String key)
        {
            return key.endsWith(getExpressionEngine().getPropertyDelimiter()) && (getExpressionEngine().getEscapedDelimiter() == null || !key.endsWith(getExpressionEngine().getEscapedDelimiter()));
        }

        private boolean hasLeadingDelimiter(String key)
        {
            return key.startsWith(getExpressionEngine().getPropertyDelimiter()) && (getExpressionEngine().getEscapedDelimiter() == null || !key.startsWith(getExpressionEngine().getEscapedDelimiter()));
        }

        private String removeAttributeMarkers(String key)
        {
            return key.substring(getExpressionEngine().getAttributeStart().length(), key.length() - ((getExpressionEngine().getAttributeEnd() != null) ? getExpressionEngine().getAttributeEnd().length() : 0));
        }

        private String unescapeDelimiters(String key)
        {
            return (getExpressionEngine().getEscapedDelimiter() == null) ? key : StringUtils.replace(key, getExpressionEngine().getEscapedDelimiter(), getExpressionEngine().getPropertyDelimiter());
        }

        private String escapeDelimiters(String key)
        {
            return (getExpressionEngine().getEscapedDelimiter() == null || key .indexOf(getExpressionEngine().getPropertyDelimiter()) < 0) ? key : StringUtils.replace(key, getExpressionEngine().getPropertyDelimiter(), getExpressionEngine().getEscapedDelimiter());
        }

        public class KeyIterator implements Iterator<Object>, Cloneable
        {
            private String current;
            private int startIndex;
            private int endIndex;
            private int indexValue;
            private boolean hasIndex;
            private boolean attribute;

            public String nextKey()
            {
                return nextKey(false);
            }

            public String nextKey(boolean decorated)
            {
                if (!hasNext()) {
                    throw new NoSuchElementException("No more key parts!");
                }

                hasIndex = false;
                indexValue = -1;
                String key = findNextIndices();

                current = key;
                hasIndex = checkIndex(key);
                attribute = checkAttribute(current);

                return currentKey(decorated);
            }

            public boolean hasNext()
            {
                return endIndex < keyBuffer.length();
            }

            public Object next()
            {
                return nextKey();
            }

            public void remove()
            {
                throw new UnsupportedOperationException("Remove not supported!");
            }

            public String currentKey()
            {
                return currentKey(false);
            }

            public String currentPrefix()
            {
                return keyBuffer.substring(0, endIndex);
            }

            public String currentKey(boolean decorated)
            {
                return (decorated && !isPropertyKey()) ? constructAttributeKey(current) : current;
            }

            public boolean isAttribute()
            {
                // if attribute emulation mode is active, the last part of a key is
                // always an attribute key, too
                return attribute || (isAttributeEmulatingMode() && !hasNext());
            }

            public boolean isPropertyKey()
            {
                return !attribute;
            }

            public int getIndex()
            {
                return indexValue;
            }

            public boolean hasIndex()
            {
                return hasIndex;
            }

            @Override
            public KeyIterator clone()
            {
                try {
                    return (KeyIterator) super.clone();
                }
                catch (CloneNotSupportedException cex) {
                    // should not happen
                    return null;
                }
            }

            private String findNextIndices()
            {
                startIndex = endIndex;
                // skip empty names
                if (startIndex < length() && hasLeadingDelimiter(keyBuffer.substring(startIndex))) {
                    startIndex += getExpressionEngine().getPropertyDelimiter().length();
                }

                // Key ends with a delimiter?
                if (startIndex >= length()) {
                    endIndex = length();
                    startIndex = endIndex - 1;
                    return keyBuffer.substring(startIndex, endIndex);
                }
                else {
                    return nextKeyPart();
                }
            }

            private String nextKeyPart()
            {
                int attrIdx = keyBuffer.toString().indexOf(getExpressionEngine().getAttributeStart(), startIndex);
                if (attrIdx < 0 || attrIdx == startIndex) {
                    attrIdx = length();
                }

                int delIdx = nextDelimiterPos(keyBuffer.toString(), startIndex + 0, attrIdx);
                if (delIdx < 0) {
                    delIdx = attrIdx;
                }

                endIndex = Math.min(attrIdx, delIdx);
                return unescapeDelimiters(keyBuffer.substring(startIndex, endIndex));
            }

            private int nextDelimiterPos(String key, int pos, int endPos)
            {
                int delimiterPos = pos;
                boolean found = false;

                do {
                    delimiterPos = key.indexOf(getExpressionEngine().getPropertyDelimiter(), delimiterPos);
                    if (delimiterPos < 0 || delimiterPos >= endPos) {
                        return -1;
                    }
                    int escapePos = escapedPosition(key, delimiterPos);
                    if (escapePos < 0) {
                        found = true;
                    }
                    else {
                        delimiterPos = escapePos;
                    }
                }
                while (!found);

                return delimiterPos;
            }

            private int escapedPosition(String key, int pos)
            {
                if (getExpressionEngine().getEscapedDelimiter() == null) {
                    // nothing to escape
                    return -1;
                }
                int escapeOffset = escapeOffset();
                if (escapeOffset < 0 || escapeOffset > pos) {
                    // No escaping possible at this position
                    return -1;
                }

                int escapePos = key.indexOf(getExpressionEngine().getEscapedDelimiter(), pos - escapeOffset);
                if (escapePos <= pos && escapePos >= 0) {
                    // The found delimiter is escaped. Next valid search position
                    // is behind the escaped delimiter.
                    return escapePos + getExpressionEngine().getEscapedDelimiter().length();
                }
                else {
                    return -1;
                }
            }

            private int escapeOffset()
            {
                return getExpressionEngine().getEscapedDelimiter().indexOf(getExpressionEngine().getPropertyDelimiter());
            }

            private boolean checkAttribute(String key)
            {
                if (isAttributeKey(key)) {
                    current = removeAttributeMarkers(key);
                    return true;
                }
                else {
                    return false;
                }
            }

            private boolean checkIndex(String key)
            {
                boolean result = false;

                try {
                    int idx = key.lastIndexOf(getExpressionEngine().getIndexStart());
                    if (idx >= 0) {
                        int endidx = key.indexOf(getExpressionEngine().getIndexEnd(),idx);

                        if (endidx > idx + 1) {
                            indexValue = Integer.parseInt(key.substring(idx + 1, endidx));
                            current = key.substring(0, idx);
                            result = true;
                        }
                    }
                }
                catch (NumberFormatException nfe) {
                    result = false;
                }

                return result;
            }

            private boolean isAttributeEmulatingMode()
            {
                return getExpressionEngine().getAttributeEnd() == null && StringUtils.equals(getExpressionEngine().getPropertyDelimiter(), getExpressionEngine().getAttributeStart());
            }
        }
    }

    protected static class ListPreservingDefaultExpressionEngine implements ExpressionEngine
    {
        /*
        public static class Key
        {
            private final String name;
            private final Optional<Integer> index;
            private final Optional<String> attribute;

            public Key(String name, Optional<Integer> index, Optional<String> attribute)
            {
                this.name = name;
                this.index = index;
                this.attribute = attribute;
            }
        }
        */

        public static final String DEFAULT_PROPERTY_DELIMITER = ".";
        public static final String DEFAULT_ESCAPED_DELIMITER = DEFAULT_PROPERTY_DELIMITER + DEFAULT_PROPERTY_DELIMITER;
        public static final String DEFAULT_ATTRIBUTE_START = "[@";
        public static final String DEFAULT_ATTRIBUTE_END = "]";
        public static final String DEFAULT_INDEX_START = "(";
        public static final String DEFAULT_INDEX_END = ")";
        private String propertyDelimiter = DEFAULT_PROPERTY_DELIMITER;
        private String escapedDelimiter = DEFAULT_ESCAPED_DELIMITER;
        private String attributeStart = DEFAULT_ATTRIBUTE_START;
        private String attributeEnd = DEFAULT_ATTRIBUTE_END;
        private String indexStart = DEFAULT_INDEX_START;
        private String indexEnd = DEFAULT_INDEX_END;

        public String getAttributeEnd()
        {
            return attributeEnd;
        }

        public void setAttributeEnd(String attributeEnd)
        {
            this.attributeEnd = attributeEnd;
        }

        public String getAttributeStart()
        {
            return attributeStart;
        }

        public void setAttributeStart(String attributeStart)
        {
            this.attributeStart = attributeStart;
        }

        public String getEscapedDelimiter()
        {
            return escapedDelimiter;
        }

        public void setEscapedDelimiter(String escapedDelimiter)
        {
            this.escapedDelimiter = escapedDelimiter;
        }

        public String getIndexEnd()
        {
            return indexEnd;
        }

        public void setIndexEnd(String indexEnd)
        {
            this.indexEnd = indexEnd;
        }

        public String getIndexStart()
        {
            return indexStart;
        }

        public void setIndexStart(String indexStart)
        {
            this.indexStart = indexStart;
        }

        public String getPropertyDelimiter()
        {
            return propertyDelimiter;
        }

        public void setPropertyDelimiter(String propertyDelimiter)
        {
            this.propertyDelimiter = propertyDelimiter;
        }

        @Override
        public List<ConfigurationNode> query(ConfigurationNode root, String key)
        {
            List<ConfigurationNode> nodes = new LinkedList<ConfigurationNode>();
            findNodesForKey(new ListPreservingDefaultConfigurationKey(this, key).iterator(), root, nodes);
            return nodes;
        }

        @Override
        public String nodeKey(ConfigurationNode node, String parentKey)
        {
            if (parentKey == null) {
                // this is the root node
                return StringUtils.EMPTY;
            }
            else {
                ListPreservingDefaultConfigurationKey key = new ListPreservingDefaultConfigurationKey(this, parentKey);
                if (node.isAttribute()) {
                    key.appendAttribute(node.getName());
                }
                else {
                    key.append(node.getName(), true);
                }
                return key.toString();
            }
        }

        public static class NodeAddData extends org.apache.commons.configuration.tree.NodeAddData
        {
            private List<String> listAttributes;

            public NodeAddData()
            {
            }

            public NodeAddData(ConfigurationNode parent, String nodeName)
            {
                super(parent, nodeName);
            }

            public List<String> getListAttributes()
            {
                if (listAttributes != null) {
                    return Collections.unmodifiableList(listAttributes);
                }
                else {
                    return Collections.emptyList();
                }
            }

            public void addListAttribute(String listAttribute)
            {
                if (listAttributes == null) {
                    listAttributes = new LinkedList<>();
                }
                listAttributes.add(listAttribute);
            }
        }

        @Override
        public NodeAddData prepareAdd(ConfigurationNode root, String key)
        {
            ListPreservingDefaultConfigurationKey.KeyIterator it = new ListPreservingDefaultConfigurationKey(this, key).iterator();
            if (!it.hasNext()) {
                throw new IllegalArgumentException("Key for add operation must be defined!");
            }
            NodeAddData result = new NodeAddData();
            result.setParent(findLastPathNode(it, root));
            while (it.hasNext()) {
                if (!it.isPropertyKey()) {
                    throw new IllegalArgumentException("Invalid key for add operation: " + key + " (Attribute key in the middle.)");
                }
                result.addPathNode(it.currentKey());
                it.next();
            }
            result.setNewNodeName(it.currentKey());
            result.setAttribute(!it.isPropertyKey());

            it = new ListPreservingDefaultConfigurationKey(this, key).iterator();
            checkState(it.hasNext());
            while (it.hasNext()) {
                if (!it.isPropertyKey()) {
                    break;
                }
                String keyPart = it.nextKey(false);
                if (it.hasIndex()) {
                    result.addListAttribute(it.currentPrefix());
                }
            }

            /*
            String keyPart = it.nextKey(false);

            if (it.hasNext()) {
                if (!keyIt.isPropertyKey()) {
                    // Attribute keys can only appear as last elements of the path
                    throw new IllegalArgumentException("Invalid path for add operation: Attribute key in the middle!");
                }
                int idx = keyIt.hasIndex() ? keyIt.getIndex() : node.getChildrenCount(keyPart) - 1;
                if (idx < 0 || idx >= node.getChildrenCount(keyPart)) {
                    return node;
                }
                else {
                    return findLastPathNode(keyIt, node.getChildren(keyPart).get(idx));
                }
            }
            else {
                return node;
            }

            checkState(it.hasNext());
            for (int i = 0; i < parts.size(); ++i) {
                ListPreservingDefaultConfigurationKey.KeyIterator part = parts.get(i);
                // ITS HERE OMG
                if (part.hasIndex()) {
                    Configs.Sigil sigil = Configs.TerminalSigil.INSTANCE;
                    for (int j = i; j >= 0; --j) {
                        ListPreservingDefaultConfigurationKey.KeyIterator p = parts.get(j);
                        if (p.hasIndex()) {
                            sigil = new Configs.ListItemSigil(p.getIndex(), sigil);
                        }
                        sigil = new Configs.MapEntrySigil(p.currentKey(), sigil);
                    }
                    result.addListAttribute(sigil.render());
                }
            }
            */

            return result;
        }

        protected void findNodesForKey(ListPreservingDefaultConfigurationKey.KeyIterator keyPart, ConfigurationNode node, Collection<ConfigurationNode> nodes)
        {
            if (!keyPart.hasNext()) {
                nodes.add(node);
            }
            else {
                String key = keyPart.nextKey(false);
                if (keyPart.isPropertyKey()) {
                    processSubNodes(keyPart, node.getChildren(key), nodes);
                }
                if (keyPart.isAttribute()) {
                    processSubNodes(keyPart, node.getAttributes(key), nodes);
                }
            }
        }

        protected ConfigurationNode findLastPathNode(ListPreservingDefaultConfigurationKey.KeyIterator keyIt, ConfigurationNode node)
        {
            String keyPart = keyIt.nextKey(false);

            if (keyIt.hasNext()) {
                if (!keyIt.isPropertyKey()) {
                    // Attribute keys can only appear as last elements of the path
                    throw new IllegalArgumentException("Invalid path for add operation: Attribute key in the middle!");
                }
                int idx = keyIt.hasIndex() ? keyIt.getIndex() : node.getChildrenCount(keyPart) - 1;
                if (idx < 0 || idx >= node.getChildrenCount(keyPart)) {
                    return node;
                }
                else {
                    return findLastPathNode(keyIt, node.getChildren(keyPart).get(idx));
                }
            }
            else {
                return node;
            }
        }

        private void processSubNodes(ListPreservingDefaultConfigurationKey.KeyIterator keyPart, List<ConfigurationNode> subNodes, Collection<ConfigurationNode> nodes)
        {
            if (keyPart.hasIndex()) {
                int idx = keyPart.getIndex();
                if (idx >= 0 && idx < subNodes.size()) {
                    findNodesForKey((ListPreservingDefaultConfigurationKey.KeyIterator) keyPart.clone(), subNodes.get(idx), nodes);
                }
            }
            else {
                for (ConfigurationNode node : subNodes) {
                    findNodesForKey((ListPreservingDefaultConfigurationKey.KeyIterator) keyPart.clone(), node, nodes);
                }
            }
        }
    }
}
