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

package org.apache.kafka.inference.blog.ml;

import org.apache.kafka.inference.blog.model.DataRegression;
import org.apache.kafka.inference.blog.model.Flight;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

public final class Predictor {

    private static final Logger LOG = LoggerFactory.getLogger(Predictor.class);

    private Predictor(){}

    public static String predict(DataRegression dataRegression) {
        try (OnlineLogisticRegression logisticRegression = new OnlineLogisticRegression()) {
            FlightData flightData = new FlightData(dataRegression.data);
            logisticRegression.readFields(new DataInputStream(new ByteArrayInputStream(dataRegression.coefficients)));
            double prediction = logisticRegression.classifyScalar(flightData.vector);
            String arrivalPrediction = prediction > 0.5 ? "on-time" : "late";
            return String.format("%s predicted to be %s", new Flight(dataRegression.data), arrivalPrediction);
        } catch (Exception e) {
            LOG.error("Problems with predicting " + dataRegression.data, e);
            return null;
        }
    }

}
