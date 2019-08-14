package app.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;

import java.io.IOException;

public class Variant {
    private long startValue;
    private long endValue;
    private String referenceBases;
    private String alternateBases;
    private double minorAF;
    private int alleleCount;

    public long getStartValue() {
        return startValue;
    }

    public void setStartValue(long startValue) {
        this.startValue = startValue;
    }

    public long getEndValue() {
        return endValue;
    }

    public void setEndValue(long endValue) {
        this.endValue = endValue;
    }

    public String getReferenceBases() {
        return referenceBases;
    }

    public void setReferenceBases(String referenceBases) {
        this.referenceBases = referenceBases;
    }

    public String getAlternateBases() {
        return alternateBases;
    }

    public void setAlternateBases(String alternateBases) {
        this.alternateBases = alternateBases;
    }

    public double getMinorAF() {
        return minorAF;
    }

    public void setMinorAF(double minorAF) {
        this.minorAF = minorAF;
    }

    public int getAlleleCount() {
        return alleleCount;
    }

    public void setAlleleCount(int alleleCount) {
        this.alleleCount = alleleCount;
    }

    @Override
    public String toString() {
        try {
            return (new ObjectMapper()).writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static Variant fromString(String s) throws IOException {
        ObjectMapper om = new ObjectMapper();
        return om.readValue(s, Variant.class);
    }
}
