import org.apache.avro.util.Utf8;

import java.util.Objects;

public class AllJoinedString {
    long faresTst;
    double convFare;
    String fareCcyIsoCode;
    long fareRatesTs;
    Double realRate;
    String ccyIsoCode;
    String ccyIsoName;
    Double rate;
    Long ccyRatesTs;

    public long getFaresTst() {
        return faresTst;
    }

    public void setFaresTst(long faresTst) {
        this.faresTst = faresTst;
    }

    public double getConvRate() {
        return convFare;
    }

    public void setConvRate(double convRate) {
        this.convFare = convRate;
    }

    public long getFareRatesTs() {
        return fareRatesTs;
    }

    public void setFareRatesTs(long fareRatesTs) {
        this.fareRatesTs = fareRatesTs;
    }

    public Double getRealRate() {
        return realRate;
    }

    public void setRealRate(Double realRate) {
        this.realRate = realRate;
    }

    public Double getRate() {
        return rate;
    }

    public void setRate(Double rate) {
        this.rate = rate;
    }

    public AllJoinedString() {
    }

    public void setConvFare(double convFare) {
        this.convFare = convFare;
    }

    public double getConvFare() {
        return convFare;
    }

    public Long getCcyRatesTs() {
        return ccyRatesTs;
    }

    public String getFareCcyIsoCode() {
        return fareCcyIsoCode;
    }

    public void setFareCcyIsoCode(String fareCcyIsoCode) {
        this.fareCcyIsoCode = fareCcyIsoCode;
    }

    public String getCcyIsoCode() {
        return ccyIsoCode;
    }

    public void setCcyIsoCode(String ccyIsoCode) {
        this.ccyIsoCode = ccyIsoCode;
    }

    public String getCcyIsoName() {
        return ccyIsoName;
    }

    public void setCcyIsoName(String ccyIsoName) {
        this.ccyIsoName = ccyIsoName;
    }

    public AllJoinedString(long faresTst, double convFare, String fareCcyIsoCode, long fareRatesTs, Double realRate, String ccyIsoCode, String ccyIsoName, Double rate, Long ccyRatesTs) {
        this.faresTst = faresTst;
        this.convFare = convFare;
        this.fareCcyIsoCode = fareCcyIsoCode;
        this.fareRatesTs = fareRatesTs;
        this.realRate = realRate;
        this.ccyIsoCode = ccyIsoCode;
        this.ccyIsoName = ccyIsoName;
        this.rate = rate;
        this.ccyRatesTs = ccyRatesTs;
    }

    @Override
    public String toString() {
        return "AllJoined{" +
                "faresTst=" + faresTst +
                ", convFare=" + convFare +
                ", fareCcyIsoCode=" + fareCcyIsoCode +
                ", fareRatesTs=" + fareRatesTs +
                ", realRate=" + realRate +
                ", ccyIsoCode=" + ccyIsoCode +
                ", ccyIsoName=" + ccyIsoName +
                ", rate=" + rate +
                ", ccyRatesTs=" + ccyRatesTs +
                '}';
    }

    public void setCcyRatesTs(Long ccyRatesTs) {
        this.ccyRatesTs = ccyRatesTs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AllJoinedString allJoined = (AllJoinedString) o;
        return faresTst == allJoined.faresTst &&
                Double.compare(allJoined.convFare, convFare) == 0 &&
                fareRatesTs == allJoined.fareRatesTs &&
                Objects.equals(fareCcyIsoCode, allJoined.fareCcyIsoCode) &&
                Objects.equals(realRate, allJoined.realRate) &&
                Objects.equals(ccyIsoCode, allJoined.ccyIsoCode) &&
                Objects.equals(ccyIsoName, allJoined.ccyIsoName) &&
                Objects.equals(rate, allJoined.rate) &&
                Objects.equals(ccyRatesTs, allJoined.ccyRatesTs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(faresTst, convFare, fareCcyIsoCode, fareRatesTs, realRate, ccyIsoCode, ccyIsoName, rate, ccyRatesTs);
    }
}
