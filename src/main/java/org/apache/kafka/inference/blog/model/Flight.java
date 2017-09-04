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

package org.apache.kafka.inference.blog.model;

import org.apache.kafka.inference.blog.data.Fields;

import static org.apache.kafka.inference.blog.data.Fields.ARR_DELAY_NEW;
import static org.apache.kafka.inference.blog.data.Fields.CRS_DEP_TIME;
import static org.apache.kafka.inference.blog.data.Fields.DAY_OF_WEEK;
import static org.apache.kafka.inference.blog.data.Fields.FL_NUM;
import static org.apache.kafka.inference.blog.data.Fields.MONTH;
import static org.apache.kafka.inference.blog.data.Fields.ORIGIN;
import static org.apache.kafka.inference.blog.data.Fields.UNIQUE_CARRIER;

public class Flight {
    private final String month;
    private final String day;
    private final String flightNumber;
    private final String time;
    private final String airLine;
    private final String actualDelay;
    private final String airport;


    public Flight(String data) {
        String[] parts = data.split(",");
        int arrivalDelayIndex = parts.length == Fields.values().length ? ARR_DELAY_NEW.ordinal() : ARR_DELAY_NEW.ordinal() - 2;
        this.month = parts[MONTH.ordinal()];
        this.day = parts[DAY_OF_WEEK.ordinal()];
        this.flightNumber = parts[FL_NUM.ordinal()];
        this.time = parts[CRS_DEP_TIME.ordinal()];
        this.airLine = parts[UNIQUE_CARRIER.ordinal()];
        this.airport = parts[ORIGIN.ordinal()];
        this.actualDelay = parts[arrivalDelayIndex];
    }

    @Override
    public String toString() {
        return "Flight{" +
               "month='" + month + '\'' +
               ", day='" + day + '\'' +
               ", flightNumber='" + flightNumber + '\'' +
               ", time='" + time + '\'' +
               ", airLine='" + airLine + '\'' +
               ", actualDelay='" + actualDelay + '\'' +
               ", airport='" + airport + '\'' +
               '}';
    }

}
