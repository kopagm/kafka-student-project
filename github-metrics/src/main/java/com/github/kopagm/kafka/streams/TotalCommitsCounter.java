package com.github.kopagm.kafka.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.kopagm.kafka.streams.transformer.DeduplicateByKeyTransformer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

public class TotalCommitsCounter {

    public Topology createTopology() {

        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
        final String DEDUPLICATE_COMMITS_STORE = "distinct-commits";

        StreamsBuilder builder = new StreamsBuilder();

        StoreBuilder<KeyValueStore<String, String>> keyValueStoreBuilder =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(DEDUPLICATE_COMMITS_STORE),
                        Serdes.String(), Serdes.String());
        builder.addStateStore(keyValueStoreBuilder);


        KStream<String, JsonNode> commits_stream = builder.stream("commits",
                Consumed.with(Serdes.String(), jsonSerde));
        commits_stream.mapValues(v -> "commit")
                .transform(() -> new DeduplicateByKeyTransformer(DEDUPLICATE_COMMITS_STORE), DEDUPLICATE_COMMITS_STORE)
                .selectKey((k,v) -> "total-commits")
                .groupByKey()
                .count()
                .toStream()
                .mapValues(v -> "total_commits: " + v)
                .to("out-total-commits", Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    public static void main(String[] args) {
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "metrics-total-commits");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Exactly once processing
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        TotalCommitsCounter app = new TotalCommitsCounter();

        KafkaStreams streams = new KafkaStreams(app.createTopology(), config);
        streams.cleanUp();
        streams.start();

        // print the topology
        //  streams.localThreadsMetadata().forEach(data -> System.out.println(data));

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
