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

import org.apache.kafka.inference.blog.data.Fields;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.vectorizer.encoders.ConstantValueEncoder;
import org.apache.mahout.vectorizer.encoders.ContinuousValueEncoder;
import org.apache.mahout.vectorizer.encoders.FeatureVectorEncoder;
import org.apache.mahout.vectorizer.encoders.StaticWordValueEncoder;

import static org.apache.kafka.inference.blog.data.Fields.ARR_DELAY_NEW;
import static org.apache.kafka.inference.blog.data.Fields.DISTANCE;

public class FlightData {

    public static final int NUM_FEATURES = 6;
    public final RandomAccessSparseVector vector = new RandomAccessSparseVector(NUM_FEATURES);
    public final int realResult;

    private final ConstantValueEncoder bias = new ConstantValueEncoder("bias");
    private final FeatureVectorEncoder categoryValueEncoder = new StaticWordValueEncoder("categories");
    private final ContinuousValueEncoder numericEncoder = new ContinuousValueEncoder("numbers");


    public FlightData(String data) {
        String[] dataParts = data.split(",");

        String late = dataParts[ARR_DELAY_NEW.ordinal()];
        late = late.isEmpty() ? "0.0" : late;
        realResult = Double.parseDouble(late) == 0.0 ? 1 : 0;
        bias.addToVector("1", vector);

        for (Fields field : Fields.values()) {
            switch (field) {
                case DAY_OF_WEEK:
                case UNIQUE_CARRIER:
                case ORIGIN:
                case DEST:
                    categoryValueEncoder.addToVector(dataParts[field.ordinal()], vector);
                    break;
                case DISTANCE:
                    Double distance = Double.parseDouble(dataParts[DISTANCE.ordinal()]) / 100000;
                    numericEncoder.addToVector(distance.toString(), vector);
                    break;
                default:
                    // don't care
            }
        }
    }
}
