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

package org.apache.kafka.inference.blog.streams;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.inference.blog.data.DataLoader;
import org.apache.kafka.inference.blog.ml.ModelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * This builds the initial model
 */
public final class DataProducer {

    private static final Logger LOG = LoggerFactory.getLogger(DataProducer.class);

    private DataProducer() {}

    public static void main(String[] args) throws IOException, InterruptedException {

        if (args.length > 0) {
            LOG.info("Running in populate GlobalKTable Mode");
            populateGlobalKTable();
        } else {
            LOG.info("Sending simulated for updating model");
            List<String> sampleFiles = Arrays.asList("incoming_data1.csv", "incoming_data1.csv", "incoming_data1.csv");
            for (String sampleFile : sampleFiles) {
                sendRawData(sampleFile);
                Thread.sleep(30000);
            }
        }
    }


    public static void sendRawData(String fileName) throws InterruptedException, IOException {
        LOG.info("Sending some raw data and \"new\" update data to run demo");
        Map<String, List<String>> dataToLoad = DataLoader.getFlightDataByAirport("src/main/resources/" + fileName);
        Producer<String, String> producer = getDataProducer();
        int counter = 0;
        for (Map.Entry<String, List<String>> entry : dataToLoad.entrySet()) {
                String key = entry.getKey();
            for (String flight : entry.getValue()) {

                ProducerRecord<String, String> rawDataRecord = new ProducerRecord<>("raw-airline-data", key, flight);
                ProducerRecord<String, String> incomingMlData = new ProducerRecord<>("ml-data-input", key, flight);
                producer.send(rawDataRecord);
                producer.send(incomingMlData);
                counter++;
                if (counter > 0 && counter % 10 == 0) {
                    Thread.sleep(10000);
                }
            }
        }

        LOG.info("Sent {} number records to raw data feed, closing down", counter);
        producer.close();
    }

    public static void populateGlobalKTable() throws IOException {
        LOG.info("Building the model");

        Map<String, byte[]> model = ModelBuilder.buildModel("src/main/resources/allFlights.txt");

        LOG.info("Model built, populating topic for GlobalKTable with {} ", model);

        Producer<String, byte[]> producer = getGlobalKTableProducer();

        for (Map.Entry<String, byte[]> entry : model.entrySet()) {
            ProducerRecord<String, byte[]> record = new ProducerRecord<>("onlineRegression-by-airport", entry.getKey(), entry.getValue());
            producer.send(record);
        }
        LOG.info("Done publishing to topic, shutting down");
        producer.close();
    }

    private static Producer<String, byte[]> getGlobalKTableProducer() {
        Properties props = getProps("org.apache.kafka.common.serialization.ByteArraySerializer");
        return new KafkaProducer<>(props);
    }

    private static Producer<String, String> getDataProducer() {
        Properties props = getProps("org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(props);
    }

    private static Properties getProps(String valueSerializer) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", valueSerializer);
        return props;
    }


}
