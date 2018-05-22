/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class KTableAggregateTest {

    private final Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
    private final Serialized<String, String> stringSerialized = Serialized.with(Serdes.String(), Serdes.String());
    private final MockProcessorSupplier<String, Object> supplier = new MockProcessorSupplier<>();
    private final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());
    final Properties props = StreamsTestUtils.topologyTestConfig(Serdes.String(), Serdes.String());

    @Rule
    public final KStreamTestDriver kstreamDriver = new KStreamTestDriver();

    @Test
    public void testAggBasic() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";


        KTable<String, String> table1 = builder.table(topic1, consumed);
        KTable<String, String> table2 = table1.groupBy(MockMapper.<String, String>noOpKeyValueMapper(),
                stringSerialized
        ).aggregate(MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                MockAggregator.TOSTRING_REMOVER,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("topic1-Canonized").withValueSerde(Serdes.String()));

        table2.toStream().process(supplier);

        kstreamDriver.setUp(builder, TestUtils.tempDirectory(), Serdes.String(), Serdes.String());

        kstreamDriver.process(topic1, "A", "1");
        kstreamDriver.flushState();
        kstreamDriver.process(topic1, "B", "2");
        kstreamDriver.flushState();
        kstreamDriver.process(topic1, "A", "3");
        kstreamDriver.flushState();
        kstreamDriver.process(topic1, "B", "4");
        kstreamDriver.flushState();
        kstreamDriver.process(topic1, "C", "5");
        kstreamDriver.flushState();
        kstreamDriver.process(topic1, "D", "6");
        kstreamDriver.flushState();
        kstreamDriver.process(topic1, "B", "7");
        kstreamDriver.flushState();
        kstreamDriver.process(topic1, "C", "8");
        kstreamDriver.flushState();


        assertEquals(Utils.mkList(
                "A:0+1",
                "B:0+2",
                "A:0+1-1+3",
                "B:0+2-2+4",
                "C:0+5",
                "D:0+6",
                "B:0+2-2+4-4+7",
                "C:0+5-5+8"), supplier.theCapturedProcessor().processed);
    }


    @Test
    public void testAggCoalesced() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        KTable<String, String> table1 = builder.table(topic1, consumed);
        KTable<String, String> table2 = table1.groupBy(MockMapper.<String, String>noOpKeyValueMapper(),
                stringSerialized
        ).aggregate(MockInitializer.STRING_INIT,
            MockAggregator.TOSTRING_ADDER,
            MockAggregator.TOSTRING_REMOVER,
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("topic1-Canonized").withValueSerde(Serdes.String()));

        table2.toStream().process(supplier);

        kstreamDriver.setUp(builder, TestUtils.tempDirectory());

        kstreamDriver.process(topic1, "A", "1");
        kstreamDriver.process(topic1, "A", "3");
        kstreamDriver.process(topic1, "A", "4");
        kstreamDriver.flushState();
        assertEquals(Utils.mkList(
            "A:0+4"), supplier.theCapturedProcessor().processed);
    }


    @Test
    public void testAggRepartition() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        KTable<String, String> table1 = builder.table(topic1, consumed);
        KTable<String, String> table2 = table1.groupBy(new KeyValueMapper<String, String, KeyValue<String, String>>() {
            @Override
                public KeyValue<String, String> apply(String key, String value) {
                switch (key) {
                    case "null":
                        return KeyValue.pair(null, value);
                    case "NULL":
                        return null;
                    default:
                        return KeyValue.pair(value, value);
                }
                }
            },
                stringSerialized
        )
                .aggregate(MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                MockAggregator.TOSTRING_REMOVER,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("topic1-Canonized").withValueSerde(Serdes.String()));

        table2.toStream().process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props, 0L)) {
            driver.pipeInput(recordFactory.create(topic1, "A", "1"));
            driver.pipeInput(recordFactory.create(topic1, "A", null));
            driver.pipeInput(recordFactory.create(topic1, "A", "1"));
            driver.pipeInput(recordFactory.create(topic1, "B", "2"));
            driver.pipeInput(recordFactory.create(topic1, "null", "3"));
            driver.pipeInput(recordFactory.create(topic1, "B", "4"));
            driver.pipeInput(recordFactory.create(topic1, "NULL", "5"));
            driver.pipeInput(recordFactory.create(topic1, "B", "7"));
        }

        assertEquals(Utils.mkList(
                "1:0+1",
                "1:0+1-1",
                "1:0+1-1+1",
                "2:0+2", 
                  //noop
                "2:0+2-2", "4:0+4",
                  //noop
                "4:0+4-4", "7:0+7"
                ), supplier.theCapturedProcessor().processed);
    }

    private void testCountHelper(final StreamsBuilder builder, final String input, final MockProcessorSupplier<String, Object> supplier) {

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props, 0L)) {
            driver.pipeInput(recordFactory.create(input, "A", "green"));
            driver.pipeInput(recordFactory.create(input, "B", "green"));
            driver.pipeInput(recordFactory.create(input, "A", "blue"));
            driver.pipeInput(recordFactory.create(input, "C", "yellow"));
            driver.pipeInput(recordFactory.create(input, "D", "green"));
        }


        assertEquals(Utils.mkList(
            "green:1",
            "green:2",
            "green:1", "blue:1",
            "yellow:1",
            "green:2"
        ), supplier.theCapturedProcessor().processed);
    }

    @Test
    public void testCount() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String input = "count-test-input";

        builder.table(input, consumed)
                .groupBy(MockMapper.<String, String>selectValueKeyValueMapper(), stringSerialized)
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("count"))
                .toStream()
                .process(supplier);

        testCountHelper(builder, input, supplier);
    }

    @Test
    public void testCountWithInternalStore() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String input = "count-test-input";

        builder.table(input, consumed)
            .groupBy(MockMapper.<String, String>selectValueKeyValueMapper(), stringSerialized)
            .count()
            .toStream()
            .process(supplier);

        testCountHelper(builder, input, supplier);
    }

    @Test
    public void testCountCoalesced() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String input = "count-test-input";
        final MockProcessorSupplier<String, Long> supplier = new MockProcessorSupplier<>();

        builder.table(input, consumed)
            .groupBy(MockMapper.<String, String>selectValueKeyValueMapper(), stringSerialized)
            .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("count"))
            .toStream()
            .process(supplier);

        kstreamDriver.setUp(builder, TestUtils.tempDirectory());

        final MockProcessor<String, Long> proc = supplier.theCapturedProcessor();

        kstreamDriver.process(input, "A", "green");
        kstreamDriver.process(input, "B", "green");
        kstreamDriver.process(input, "A", "blue");
        kstreamDriver.process(input, "C", "yellow");
        kstreamDriver.process(input, "D", "green");
        kstreamDriver.flushState();


        assertEquals(Utils.mkList(
            "blue:1",
            "yellow:1",
            "green:2"
            ), proc.processed);
    }
    
    @Test
    public void testRemoveOldBeforeAddNew() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String input = "count-test-input";
        final MockProcessorSupplier<String, String> supplier = new MockProcessorSupplier<>();

        builder.table(input, consumed)
                .groupBy(new KeyValueMapper<String, String, KeyValue<String, String>>() {

                    @Override
                    public KeyValue<String, String> apply(String key, String value) {
                        return KeyValue.pair(String.valueOf(key.charAt(0)), String.valueOf(key.charAt(1)));
                    }
                }, stringSerialized)
                .aggregate(new Initializer<String>() {

                    @Override
                    public String apply() {
                        return "";
                    }
                }, new Aggregator<String, String, String>() {
                    
                    @Override
                    public String apply(String aggKey, String value, String aggregate) {
                        return aggregate + value;
                    } 
                }, new Aggregator<String, String, String>() {

                    @Override
                    public String apply(String key, String value, String aggregate) {
                        return aggregate.replaceAll(value, "");
                    }
                }, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("someStore").withValueSerde(Serdes.String()))
                .toStream()
                .process(supplier);

        kstreamDriver.setUp(builder, TestUtils.tempDirectory());

        final MockProcessor<String, String> proc = supplier.theCapturedProcessor();

        kstreamDriver.process(input, "11", "A");
        kstreamDriver.flushState();
        kstreamDriver.process(input, "12", "B");
        kstreamDriver.flushState();
        kstreamDriver.process(input, "11", null);
        kstreamDriver.flushState();
        kstreamDriver.process(input, "12", "C");
        kstreamDriver.flushState();

        assertEquals(Utils.mkList(
                 "1:1",
                 "1:12",
                 "1:2",
                 "1:2"
                 ), proc.processed);
    }

    @Test
    public void shouldForwardToCorrectProcessorNodeWhenMultiCacheEvictions() {
        final String tableOne = "tableOne";
        final String tableTwo = "tableTwo";
        final StreamsBuilder builder = new StreamsBuilder();
        final String reduceTopic = "TestDriver-reducer-store-repartition";
        final Map<String, Long> reduceResults = new HashMap<>();

        final KTable<String, String> one = builder.table(tableOne, consumed);
        final KTable<Long, String> two = builder.table(tableTwo, Consumed.with(Serdes.Long(), Serdes.String()));


        final KTable<String, Long> reduce = two.groupBy(new KeyValueMapper<Long, String, KeyValue<String, Long>>() {
            @Override
            public KeyValue<String, Long> apply(final Long key, final String value) {
                return new KeyValue<>(value, key);
            }
        }, Serialized.with(Serdes.String(), Serdes.Long()))
                .reduce(new Reducer<Long>() {
                    @Override
                    public Long apply(final Long value1, final Long value2) {
                        return value1 + value2;
                    }
                }, new Reducer<Long>() {
                    @Override
                    public Long apply(final Long value1, final Long value2) {
                        return value1 - value2;
                    }
                }, Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("reducer-store"));

        reduce.toStream().foreach(new ForeachAction<String, Long>() {
            @Override
            public void apply(final String key, final Long value) {
                reduceResults.put(key, value);
            }
        });

        one.leftJoin(reduce, new ValueJoiner<String, Long, String>() {
            @Override
            public String apply(final String value1, final Long value2) {
                return value1 + ":" + value2;
            }
        })
                .mapValues(new ValueMapper<String, String>() {
                    @Override
                    public String apply(final String value) {
                        return value;
                    }
                });

        kstreamDriver.setUp(builder, TestUtils.tempDirectory(), 111);
        kstreamDriver.process(reduceTopic, "1", new Change<>(1L, null));
        kstreamDriver.process("tableOne", "2", "2");
        // this should trigger eviction on the reducer-store topic
        kstreamDriver.process(reduceTopic, "2", new Change<>(2L, null));
        // this wont as it is the same value
        kstreamDriver.process(reduceTopic, "2", new Change<>(2L, null));
        assertEquals(Long.valueOf(2L), reduceResults.get("2"));

        // this will trigger eviction on the tableOne topic
        // that in turn will cause an eviction on reducer-topic. It will flush
        // key 2 as it is the only dirty entry in the cache
        kstreamDriver.process("tableOne", "1", "5");
        assertEquals(Long.valueOf(4L), reduceResults.get("2"));
    }

}
