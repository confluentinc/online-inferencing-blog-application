package org.apache.kafka.inference.blog.model;

import java.util.Arrays;

public class DataRegression {

    public final String data;
    public final byte[] coefficients;

    public DataRegression(String data, byte[] coefficients) {
        this.data = data;
        this.coefficients = Arrays.copyOf(coefficients, coefficients.length);
    }

    @Override
    public String toString() {
        return "DataRegression{" +
               "data='" + data + '\'' +
               ", coefficients (length)=" + coefficients.length +
               '}';
    }
}
