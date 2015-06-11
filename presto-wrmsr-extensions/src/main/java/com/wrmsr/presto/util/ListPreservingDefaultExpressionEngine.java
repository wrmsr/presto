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

import org.apache.commons.configuration.tree.ConfigurationNode;
import org.apache.commons.configuration.tree.ExpressionEngine;
import org.apache.commons.configuration.tree.NodeAddData;
import org.apache.commons.lang.StringUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class ListPreservingDefaultExpressionEngine implements ExpressionEngine
{
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
            if (it.hasIndex()) {
                result.addListAttribute(it.currentPrefix());
            }
            it.next();
        }

        if (it.hasIndex()) {
            String p = it.currentPrefix();
            result.addListAttribute(p.substring(0, p.length() - 3)); // FIXME
        }

        result.setNewNodeName(it.currentKey());
        result.setAttribute(!it.isPropertyKey());
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
