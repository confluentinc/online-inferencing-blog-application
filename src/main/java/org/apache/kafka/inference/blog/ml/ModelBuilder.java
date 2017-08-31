package org.apache.kafka.inference.blog.ml;

import org.apache.kafka.inference.blog.data.DataLoader;
import org.apache.mahout.classifier.evaluation.Auc;
import org.apache.mahout.classifier.sgd.L1;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class ModelBuilder {

    private static final int NUM_EPOCS = 20;
    private static final double PERCENTAGE_OF_DATASET_TO_USE = .5;
    private static final double TRAINING_PERCENTAGE = .40;
    private static final Logger LOG = LoggerFactory.getLogger(ModelBuilder.class);

    private ModelBuilder() {}

    public static void main(String[] args) throws Exception {
        LOG.info("Training now");
        train("src/main/resources/allFlights.txt");
    }

    public static Map<String, OnlineLogisticRegression> train(String path) throws IOException {
        final Map<String, List<String>> data = DataLoader.getFlightDataByAirport(path);

        final Map<String, List<String>> sample = getRandomSampling(data);

        final List<FlightData> flightData = new ArrayList<>();
        final Map<String, OnlineLogisticRegression> regressionMap = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : sample.entrySet()) {
            List<String> airportValues = entry.getValue();
            int trainIndex = (int) (airportValues.size() * TRAINING_PERCENTAGE);
            List<String> train = airportValues.subList(0, trainIndex);
            List<String> test = airportValues.subList(trainIndex + 1, airportValues.size());

            for (String flight : train) {
                flightData.add(new FlightData(flight));
            }
            LOG.info("Training for " + entry.getKey());
            OnlineLogisticRegression trainedRegression = onlineRegression(flightData);
            LOG.info("Training complete, now testing");
            testTrainedRegression(trainedRegression, entry.getKey(), test);

            regressionMap.put(entry.getKey(), trainedRegression);
        }
        return regressionMap;
    }

    private static Map<String, List<String>> getRandomSampling(Map<String, List<String>> allData) {
        Map<String, List<String>> sample = new HashMap<>();
        SecureRandom random = new SecureRandom();
        for (Map.Entry<String, List<String>> entry : allData.entrySet()) {
            int total = (int) (entry.getValue().size() * PERCENTAGE_OF_DATASET_TO_USE);
            String key = entry.getKey();
            List<String> allFlights = entry.getValue();
            Collections.shuffle(allFlights);
            Set<String> flights = new HashSet<>();
            while (flights.size() < total) {
                flights.add(allFlights.get(random.nextInt(allFlights.size())));
            }
            sample.put(key, new ArrayList<>(flights));
        }
        return sample;
    }

    public static byte[] train(List<String> flights) throws IOException {
        List<FlightData> allFlightData = new ArrayList<>();
        for (String flight : flights) {
              allFlightData.add(new FlightData(flight));
        }
        return getBytesFromOnlineRegression(onlineRegression(allFlightData));
    }

    private static byte[] getBytesFromOnlineRegression(OnlineLogisticRegression logisticRegression) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(baos);
        logisticRegression.write(dataOutputStream);
        return baos.toByteArray();
    }

    private static OnlineLogisticRegression onlineRegression(List<FlightData> allFlightData) {
        OnlineLogisticRegression logisticRegression = new OnlineLogisticRegression(2, FlightData.NUM_FEATURES, new L1());

        for (int i = 0; i < NUM_EPOCS; i++) {
            for (FlightData flightData : allFlightData) {
                logisticRegression.train(flightData.realResult, flightData.vector);
            }
        }
        return logisticRegression;
    }

    private static void testTrainedRegression(OnlineLogisticRegression onlineLogisticRegression, String key, List<String> testFights) {
        Auc eval = new Auc(0.5);
        for (String testFight : testFights) {
            FlightData flightData = new FlightData(testFight);
            eval.add(flightData.realResult, onlineLogisticRegression.classifyScalar(flightData.vector));
        }
        LOG.info("Training accuracy for {} {}", key, eval.auc());
    }

    public static Map<String, byte[]> buildModel(String path) throws IOException {
        Map<String, OnlineLogisticRegression> airlineData = train(path);
        Map<String, byte[]> coefficientMap = new HashMap<>();
        for (Map.Entry<String, OnlineLogisticRegression> regressionEntry : airlineData.entrySet()) {
            coefficientMap.put(regressionEntry.getKey(), getBytesFromOnlineRegression(regressionEntry.getValue()));
        }
        return coefficientMap;
    }

}
