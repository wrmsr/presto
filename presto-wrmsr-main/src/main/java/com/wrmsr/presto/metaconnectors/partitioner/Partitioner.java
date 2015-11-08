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
package com.wrmsr.presto.metaconnectors.partitioner;

import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.SortedRangeSet;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.BigintType;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.util.ColumnDomain;
import com.wrmsr.presto.util.ImmutableCollectors;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static com.google.common.base.Preconditions.checkArgument;

@FunctionalInterface
public interface Partitioner
{
    class Partition
    {
        private final String prtitionId;
        private final TupleDomain<String> tupleDomain;

        public Partition(String prtitionId, TupleDomain<String> tupleDomain)
        {
            this.prtitionId = prtitionId;
            this.tupleDomain = tupleDomain;
        }

        public String getPrtitionId()
        {
            return prtitionId;
        }

        public TupleDomain<String> getTupleDomain()
        {
            return tupleDomain;
        }
    }

    List<Partition> getPartitionsConnector(SchemaTableName table, TupleDomain<String> tupleDomain);

    static List<Partition> generateDensePartitions(LinkedHashMap<String, ColumnDomain> columnDomains, int numPartitions)
    {
        checkArgument(columnDomains.size() == 1); // FIXME
        Map.Entry<String, ColumnDomain> e = columnDomains.entrySet().stream().findFirst().get();
        String column = e.getKey();
        ColumnDomain columnDomain = e.getValue();
        int base = (Integer) columnDomain.getMin();
        int step = ((Integer) columnDomain.getMax() - base) / numPartitions;
        // FIXME: alignment / remainder
        return LongStream.range(0, numPartitions).boxed()
                .map((i) -> new Partition(
                        i.toString(),
                        TupleDomain.withColumnDomains(
                                ImmutableMap.of(
                                        column,
                                        Domain.create(
                                                ValueSet.ofRanges(Range.range(BigintType.BIGINT, i * step + base, true, (i + 1) * step + base, false)),
                                                false)))))
                .collect(ImmutableCollectors.toImmutableList());
     }
}
