package com.github.kopagm.kafka.streams.transformer;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Calculates top five committers.
 * Expects to receive records with counted commits by each committer (key - committer, value - number of commits).
 * Produces string value with the top five committers, e.g. "top5_committers: user1 (100), user2 (90)".
 */
public class TopFiveCommittersTransformer implements Transformer<String, Long, KeyValue<String, String>> {

    private final String storeName;

    private KeyValueStore<String, ValueAndTimestamp> store;

    /**
     * Constructs the transformer.
     *
     * @param storeName the name of the state's store
     */
    public TopFiveCommittersTransformer(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.store = (KeyValueStore) context.getStateStore(storeName);
    }

    @Override
    public KeyValue<String, String> transform(String key, Long value) {
        KeyValueIterator<String, ValueAndTimestamp> iterator = store.all();
        List<KeyValue<String, ValueAndTimestamp>> authors = new ArrayList<>();

        while (iterator.hasNext()) {
            authors.add(iterator.next());
        }

        authors.sort(Comparator.comparing(o ->
                ((KeyValue<String, ValueAndTimestamp<Long>>) o).value.value()).reversed());

//        System.out.println(authors.size());
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 5 && i < authors.size(); i++) {
//            System.out.println(authors.get(i).key);
            sb.append(authors.get(i).key)
                    .append(" (").append(authors.get(i).value.value()).append(")");
            if (i != 4 && i != authors.size() - 1) {
                sb.append(", ");
            }
        }

        return new KeyValue<>("top5-committers", "top5_committers: " + sb.toString());
    }

    @Override
    public void close() {
    }
}
