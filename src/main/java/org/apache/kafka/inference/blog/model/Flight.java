package org.apache.kafka.inference.blog.model;

import static org.apache.kafka.inference.blog.data.Fields.ARR_DELAY_NEW;
import static org.apache.kafka.inference.blog.data.Fields.CRS_DEP_TIME;
import static org.apache.kafka.inference.blog.data.Fields.DAY_OF_WEEK;
import static org.apache.kafka.inference.blog.data.Fields.FL_NUM;
import static org.apache.kafka.inference.blog.data.Fields.MONTH;
import static org.apache.kafka.inference.blog.data.Fields.UNIQUE_CARRIER;

public class Flight {
    private final String month;
    private final String day;
    private final String flightNumber;
    private final String time;
    private final String airLine;
    private final String actualDelay;


    public Flight(String data) {
        String[] parts = data.split(",");
        this.month = parts[MONTH.ordinal()];
        this.day = parts[DAY_OF_WEEK.ordinal()];
        this.flightNumber = parts[FL_NUM.ordinal()];
        this.time = parts[CRS_DEP_TIME.ordinal()];
        this.airLine = parts[UNIQUE_CARRIER.ordinal()];
        this.actualDelay = parts[ARR_DELAY_NEW.ordinal()];
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
               '}';
    }

}
