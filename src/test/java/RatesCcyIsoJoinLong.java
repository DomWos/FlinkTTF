import org.apache.avro.util.Utf8;

import java.sql.Timestamp;

public class RatesCcyIsoJoinLong {
    Utf8 ccyIsoCode;
    Utf8 ccyIsoName;
    Double rate;

    public Utf8 getCcyIsoCode() {
        return ccyIsoCode;
    }

    public void setCcyIsoCode(Utf8 ccyIsoCode) {
        this.ccyIsoCode = ccyIsoCode;
    }

    public Utf8 getCcyIsoName() {
        return ccyIsoName;
    }

    public void setCcyIsoName(Utf8 ccyIsoName) {
        this.ccyIsoName = ccyIsoName;
    }

    @Override
    public String toString() {
        return "RatesCcyIsoJoinTimestamp{" +
                "ccyIso='" + ccyIsoCode + '\'' +
                ", ccyIsoName='" + ccyIsoName + '\'' +
                ", rate=" + rate +
                ", ratesTs=" + ratesLong +
                '}';
    }

    public Double getRate() {
        return rate;
    }

    public void setRate(Double rate) {
        rate = rate;
    }

    public Long getRatesLong() {
        return ratesLong;
    }

    public void setRatesLong(Long ratesLong) {
        this.ratesLong = ratesLong;
    }

    public RatesCcyIsoJoinLong() {
    }

    public RatesCcyIsoJoinLong(Utf8 ccyIsoCode, Utf8 ccyIsoName, Double rate, Long ratesLong) {
        this.ccyIsoCode = ccyIsoCode;
        this.ccyIsoName = ccyIsoName;
        rate = rate;
        this.ratesLong = ratesLong;
    }

    Long ratesLong;
}