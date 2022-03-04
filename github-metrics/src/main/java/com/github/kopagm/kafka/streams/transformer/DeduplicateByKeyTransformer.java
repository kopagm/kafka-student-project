package com.github.kopagm.kafka.streams.transformer;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deduplicates records by their key - if a message was already processed before, it will be filtered out.
 */
public class DeduplicateByKeyTransformer implements Transformer<String, String, KeyValue<String, String>> {

    private static final Logger logger = LoggerFactory.getLogger(DeduplicateByKeyTransformer.class);

    private final String storeName;

    private KeyValueStore<String, String> store;

    /**
     * Constructs the transformer.
     *
     * @param storeName the name of the state's store
     */
    public DeduplicateByKeyTransformer(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.store = (KeyValueStore) context.getStateStore(storeName);
    }

    @Override
    public KeyValue<String, String> transform(String key, String value) {
        String res = store.putIfAbsent(key, value);
        logger.debug("lookup result for key " + key + ": " + res);
        if (res != null) {
            return null;
        }
        return new KeyValue<>(key, value);
    }

    @Override
    public void close() {
    }
}
