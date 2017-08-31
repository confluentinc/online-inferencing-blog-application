package org.apache.kafka.inference.blog.streams;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.inference.blog.ml.Predictor;
import org.apache.kafka.inference.blog.model.DataRegression;
import org.apache.kafka.inference.blog.util.JsonDeserializer;
import org.apache.kafka.inference.blog.util.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class KStreamsOnLineInferencing {

    private static final Logger LOG = LoggerFactory.getLogger(KStreamsOnLineInferencing.class);

    public static void main(String[] args) throws Exception {

        final Serde<byte[]> byteArraySerde = Serdes.ByteArray();

        KStreamBuilder builder = new KStreamBuilder();

        // this stream reads in the raw airline data and does the updating of onlineRegression
        KStream<String, String> rawDataStream = builder.stream("raw-airline-data");

        GlobalKTable<String, byte[]> regressionsByAirPortTable = builder.globalTable(Serdes.String(), byteArraySerde, "onlineRegression-by-airport");

        // stream reads raw data joins with co-efficients then makes prediction
        rawDataStream.join(regressionsByAirPortTable,
                           (k, v) -> k,
                           DataRegression::new)
            .mapValues(Predictor::predict)
            .filter((k, v) -> v != null)
            .peek((k, v) -> System.out.println("Prediction " + v))
            .to("predictions");

        // this is our update stream that will continuously update our model
        // in reality this would be run in a separate process

        Serde<List<String>> listSerde = getListStringSerde();

        builder.addSource("new-data-source","ml-data-input");
        builder.addProcessor("modelUpdater",
                             AirlinePredictorProcessor::new,
                             "new-data-source");
        builder.addStateStore(Stores.create("flights").withStringKeys()
                                  .withValues(listSerde)
                                  .inMemory()
                                  .maxEntries(250)
                                  .build(),"modelUpdater");
        builder.addSink("updates-sink",
                        "onlineRegression-by-airport",
                        Serdes.String().serializer(),
                        byteArraySerde.serializer(),
                        "modelUpdater");

        KafkaStreams kafkaStreams = new KafkaStreams(builder, new StreamsConfig(getProperties()));
        CountDownLatch doneSignal = new CountDownLatch(1);

        kafkaStreams.setUncaughtExceptionHandler((t, e) -> LOG.info("Error in thread " + t + " for " + e));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Kill signal received shutting down");
            doneSignal.countDown();
            kafkaStreams.close(5, TimeUnit.SECONDS);
            LOG.info("Closed, bye");
        }));

        kafkaStreams.cleanUp();
        try {
            kafkaStreams.start();
            LOG.info("Waiting for shutdown");
            doneSignal.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private static Serde<List<String>> getListStringSerde() {
        Map<String, Object> serdeProps = new HashMap<>();
        final Serializer<List<String>> listJsonSerializer = new JsonSerializer<>();
        final Deserializer<List<String>> listDeserializer = new JsonDeserializer<>();
        serdeProps.put("class", List.class);
        listDeserializer.configure(serdeProps, false);
        return Serdes.serdeFrom(listJsonSerializer, listDeserializer);
    }


    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-online-inferencing");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "streams-online-inferencing-clientID");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);

        return props;
    }

}
