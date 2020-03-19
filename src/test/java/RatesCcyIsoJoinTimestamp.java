import org.apache.avro.util.Utf8;

import java.sql.Timestamp;

public class RatesCcyIsoJoinTimestamp {
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
                ", ratesTs=" + ratesTs +
                '}';
    }

    public Double getRate() {
        return rate;
    }

    public void setRate(Double rate) {
        rate = rate;
    }

    public Timestamp getRatesTs() {
        return ratesTs;
    }

    public void setRatesTs(Timestamp ratesTs) {
        this.ratesTs = ratesTs;
    }

    public RatesCcyIsoJoinTimestamp() {
    }

    public RatesCcyIsoJoinTimestamp(Utf8 ccyIsoCode, Utf8 ccyIsoName, Double rate, Timestamp ratesTs) {
        this.ccyIsoCode = ccyIsoCode;
        this.ccyIsoName = ccyIsoName;
        rate = rate;
        this.ratesTs = ratesTs;
    }

    Timestamp ratesTs;
}