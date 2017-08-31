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

package org.apache.kafka.inference.blog.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.groupingBy;

public final class DataLoader {

    private static final Pattern TOP_15_BUSIEST_AIRPORTS =
        Pattern.compile("\"(SLC|SEA|EWR|MCO|BOS|CLT|LAS|PHX|SFO|IAH|LAX|DEN|DFW|ORD|ATL)\"");

    private static final Logger LOG = LoggerFactory.getLogger(DataLoader.class);

    private static final int AIRPORT_INDEX = 4;

    private static Function<Integer, Predicate<String>> airportMatcher =
        index -> line -> TOP_15_BUSIEST_AIRPORTS.matcher(line.split(",")[index]).matches();

    private static BiFunction<Integer, String, String> getFieldAt = (index, line) -> line.split(",")[index];
    private static Function<String, String> cleanQuotes = line -> line.replaceAll("\"", "");

    private DataLoader() {}


    public static Map<String, List<String>> getFlightDataByAirport(String path) throws IOException {
         return loadFilteredByAirportRegex(AIRPORT_INDEX, new File(path));
    }

    private static Map<String, List<String>> loadFilteredByAirportRegex(int airportIndex, File flightsFile) throws IOException {
        return Files.readAllLines(flightsFile.toPath()).stream()
            .filter(airportMatcher.apply(airportIndex))
            .map(cleanQuotes)
            .collect(groupingBy(line -> getFieldAt.apply(AIRPORT_INDEX, line)));
    }

    public static void main(String[] args) throws Exception {

        LOG.info("Getting Flights");
        Map<String, List<String>> trainingData = getFlightDataByAirport("src/main/resources/allFlights.txt");
        printMap(trainingData);

    }

    private static void printMap(Map<String, List<String>> map) {
        for (Map.Entry<String, List<String>> stringListEntry : map.entrySet()) {
            LOG.info("{} number flights {}", stringListEntry.getKey(), stringListEntry.getValue().size());
        }
    }
}
