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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ListPreservingDefaultConfigurationKey
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
        private List<Integer> indexValue;
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
            indexValue = ImmutableList.of();
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

        public List<Integer> getIndex()
        {
            return indexValue;
        }

        public boolean hasIndex()
        {
            return hasIndex;
        }

        @Override
        public Object clone()
        {
            try {
                return super.clone();
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
            while (startIndex < length() && hasLeadingDelimiter(keyBuffer.substring(startIndex))) {
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

            int delIdx = nextDelimiterPos(keyBuffer.toString(), startIndex, attrIdx);
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
                if (idx > 0) {
                    int endidx = key.indexOf(getExpressionEngine().getIndexEnd(),idx);

                    if (endidx > idx + 1) {
                        indexValue = ImmutableList.copyOf(Iterables.transform(
                                Splitter.on(",").split(key.substring(idx + 1, endidx)), Integer::parseInt));
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

