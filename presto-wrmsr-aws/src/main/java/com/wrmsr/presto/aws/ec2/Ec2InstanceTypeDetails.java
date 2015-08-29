/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wrmsr.presto.aws.ec2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

public class Ec2InstanceTypeDetails
{
    public final String family;
    public final boolean enhancedNetworking;
    public final int vCpu;
    public final String generation;
    public final int ebsIops;
    public final String networkPerformance;
    public final int ebsThroughput;

    @JsonDeserialize(using = RegionPricing.Deserializer.class)
    public static final class RegionPricing
    {
        public static class Deserializer extends JsonDeserializer<RegionPricing>
        {
            @Override
            public RegionPricing deserialize(JsonParser jp, DeserializationContext ctxt)
                    throws IOException, JsonProcessingException
            {
                ObjectMapper mapper = (ObjectMapper) jp.getCodec();
                ObjectNode node = jp.getCodec().readTree(jp);
                Iterator<Map.Entry<String, JsonNode>> it = node.fields();
                Map<String, PlatformPricing> m = newHashMap();
                while (it.hasNext()) {
                    Map.Entry<String, JsonNode> e = it.next();
                    JsonParser contentParser = mapper.treeAsTokens(e.getValue());
                    PlatformPricing p = mapper.readValue(contentParser, PlatformPricing.class);
                    m.put(e.getKey(), p);
                }
                return new RegionPricing(m);
            }
        }

        public final Map<String, PlatformPricing> platformPricing;

        @JsonCreator
        public RegionPricing(
                @JsonProperty("platform") Map<String, PlatformPricing> platformPricing)
        {
            this.platformPricing = platformPricing;
        }
    }

    public static final class PlatformPricing
    {
        public final Map<String, String> reserved;
        public final String ondemand;

        @JsonCreator
        public PlatformPricing(
                @JsonProperty("reserved") Map<String, String> reserved,
                @JsonProperty("ondemand") String ondemand)
        {
            this.reserved = reserved;
            this.ondemand = ondemand;
        }
    }

    public final Map<String, RegionPricing> regionPricing;

    public static final class Vpc
    {
        public final int ipsPerEni;
        public final int maxEnis;

        @JsonCreator
        public Vpc(
                @JsonProperty("ips_per_eni") int ipsPerEni,
                @JsonProperty("max_enis") int maxEnis)
        {
            this.ipsPerEni = ipsPerEni;
            this.maxEnis = maxEnis;
        }
    }

    public final Vpc vpc;

    public final List<String> arch;

    public final List<String> linuxVirtualizationTypes;
    public final boolean ebsOptimized;

    public static final class Storage
    {
        public final boolean ssd;
        public final int devices;
        public final int size;

        @JsonCreator
        public Storage(
                @JsonProperty("ssd") boolean ssd,
                @JsonProperty("devices") int devices,
                @JsonProperty("size") int size)
        {
            this.ssd = ssd;
            this.devices = devices;
            this.size = size;
        }
    }

    public final Storage storage;

    public final int maxBandwidth;
    public final String instanceType;
    public final BigDecimal ecu;
    public final BigDecimal memory;

    @JsonCreator
    public Ec2InstanceTypeDetails(
            @JsonProperty("family") String family,
            @JsonProperty("enhanced_networking") boolean enhancedNetworking,
            @JsonProperty("vCPU") int vCpu,
            @JsonProperty("generation") String generation,
            @JsonProperty("ebs_iops") int ebsIops,
            @JsonProperty("network_performance") String networkPerformance,
            @JsonProperty("ebs_throughput") int ebsThroughput,
            @JsonProperty("pricing") Map<String,RegionPricing> regionPricing,
            @JsonProperty("vpc") Vpc vpc,
            @JsonProperty("arch") List<String> arch,
            @JsonProperty("linux_virtualization_types") List<String> linuxVirtualizationTypes,
            @JsonProperty("ebs_optimized") boolean ebsOptimized,
            @JsonProperty("storage") Storage storage,
            @JsonProperty("max_bandwidth") int maxBandwidth,
            @JsonProperty("instance_type") String instanceType,
            @JsonProperty("ECU") BigDecimal ecu,
            @JsonProperty("memory") BigDecimal memory)
    {
        this.family = family;
        this.enhancedNetworking = enhancedNetworking;
        this.vCpu = vCpu;
        this.generation = generation;
        this.ebsIops = ebsIops;
        this.networkPerformance = networkPerformance;
        this.ebsThroughput = ebsThroughput;
        this.regionPricing = regionPricing;
        this.vpc = vpc;
        this.arch = arch;
        this.linuxVirtualizationTypes = linuxVirtualizationTypes;
        this.ebsOptimized = ebsOptimized;
        this.storage = storage;
        this.maxBandwidth = maxBandwidth;
        this.instanceType = instanceType;
        this.ecu = ecu;
        this.memory = memory;
    }
}
