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
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wrmsr.presto.util.Serialization;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static com.wrmsr.presto.util.ImmutableCollectors.toImmutableMap;

public class Ec2InstanceTypeDetails
{
    public static final String URL = "https://github.com/toelen/spymemcached-jcache";
    public static final String RESOURCE = "com/wrmsr/presto/aws/ec2/instance-types.json";

    public static Map<String, Ec2InstanceTypeDetails> read()
            throws IOException
    {
        List<Ec2InstanceTypeDetails> lst;
        try (InputStream in = Ec2InstanceTypeDetails.class.getClassLoader().getResourceAsStream(RESOURCE)) {
            lst = Serialization.JSON_OBJECT_MAPPER.get().readValue(in, new TypeReference<List<Ec2InstanceTypeDetails>>(){});
        }
        return lst.stream().map(i -> new ImmutablePair<>(i.instanceType, i)).collect(toImmutableMap());
    }

    @JsonSerialize(using = RegionPricing.Serializer.class)
    @JsonDeserialize(using = RegionPricing.Deserializer.class)
    public static final class RegionPricing
    {
        public static class Serializer extends JsonSerializer<RegionPricing>
        {
            @Override
            public void serialize(RegionPricing value, JsonGenerator jgen, SerializerProvider provider)
                    throws IOException, JsonProcessingException
            {
                if (value != null) {
                    jgen.writeObject(value.getPlatformPricing());
                }
                else {
                    jgen.writeNull();
                }
            }
        }

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

        private final Map<String, PlatformPricing> platformPricing;

        public RegionPricing(Map<String, PlatformPricing> platformPricing)
        {
            this.platformPricing = platformPricing;
        }

        public Map<String, PlatformPricing> getPlatformPricing()
        {
            return platformPricing;
        }

        @Override
        public String toString()
        {
            return Serialization.toJsonString(this);
        }
    }

    public static final class PlatformPricing
    {
        private final Map<String, String> reserved;
        private final String ondemand;

        @JsonCreator
        public PlatformPricing(
                @JsonProperty("reserved") Map<String, String> reserved,
                @JsonProperty("ondemand") String ondemand)
        {
            this.reserved = reserved;
            this.ondemand = ondemand;
        }

        @JsonProperty("reserved")
        public Map<String, String> getReserved()
        {
            return reserved;
        }

        @JsonProperty("ondemand")
        public String getOndemand()
        {
            return ondemand;
        }

        @Override
        public String toString()
        {
            return Serialization.toJsonString(this);
        }
    }

    public static final class Vpc
    {
        private final int ipsPerEni;
        private final int maxEnis;

        @JsonCreator
        public Vpc(
                @JsonProperty("ips_per_eni") int ipsPerEni,
                @JsonProperty("max_enis") int maxEnis)
        {
            this.ipsPerEni = ipsPerEni;
            this.maxEnis = maxEnis;
        }

        @JsonProperty("ips_per_eni")
        public int getIpsPerEni()
        {
            return ipsPerEni;
        }

        @JsonProperty("max_enis")
        public int getMaxEnis()
        {
            return maxEnis;
        }

        @Override
        public String toString()
        {
            return Serialization.toJsonString(this);
        }
    }

    public static final class Storage
    {
        private final boolean ssd;
        private final int devices;
        private final int size;

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

        @JsonProperty("ssd")
        public boolean isSsd()
        {
            return ssd;
        }

        @JsonProperty("devices")
        public int getDevices()
        {
            return devices;
        }

        @JsonProperty("size")
        public int getSize()
        {
            return size;
        }

        @Override
        public String toString()
        {
            return Serialization.toJsonString(this);
        }
    }

    private final String family;
    private final boolean enhancedNetworking;
    private final int vCpu;
    private final String generation;
    private final int ebsIops;
    private final String networkPerformance;
    private final int ebsThroughput;
    private final Map<String, RegionPricing> regionPricing;
    private final Vpc vpc;
    private final List<String> arch;
    private final List<String> linuxVirtualizationTypes;
    private final boolean ebsOptimized;
    private final Storage storage;
    private final int maxBandwidth;
    private final String instanceType;
    private final BigDecimal ecu;
    private final BigDecimal memory;

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

    @JsonProperty("family")
    public String getFamily()
    {
        return family;
    }

    @JsonProperty("enhanced_networking")
    public boolean isEnhancedNetworking()
    {
        return enhancedNetworking;
    }

    @JsonProperty("vCPU")
    public int getvCpu()
    {
        return vCpu;
    }

    @JsonProperty("generation")
    public String getGeneration()
    {
        return generation;
    }

    @JsonProperty("ebs_iops")
    public int getEbsIops()
    {
        return ebsIops;
    }

    @JsonProperty("network_performance")
    public String getNetworkPerformance()
    {
        return networkPerformance;
    }

    @JsonProperty("ebs_throughput")
    public int getEbsThroughput()
    {
        return ebsThroughput;
    }

    @JsonProperty("pricing")
    public Map<String, RegionPricing> getRegionPricing()
    {
        return regionPricing;
    }

    @JsonProperty("vpc")
    public Vpc getVpc()
    {
        return vpc;
    }

    @JsonProperty("arch")
    public List<String> getArch()
    {
        return arch;
    }

    @JsonProperty("linux_virtualization_types")
    public List<String> getLinuxVirtualizationTypes()
    {
        return linuxVirtualizationTypes;
    }

    @JsonProperty("ebs_optimized")
    public boolean isEbsOptimized()
    {
        return ebsOptimized;
    }

    @JsonProperty("storage")
    public Storage getStorage()
    {
        return storage;
    }

    @JsonProperty("max_bandwidth")
    public int getMaxBandwidth()
    {
        return maxBandwidth;
    }

    @JsonProperty("instance_type")
    public String getInstanceType()
    {
        return instanceType;
    }

    @JsonProperty("ECU")
    public BigDecimal getEcu()
    {
        return ecu;
    }

    @JsonProperty("memory")
    public BigDecimal getMemory()
    {
        return memory;
    }

    @Override
    public String toString()
    {
        return Serialization.toJsonString(this);
    }
}
