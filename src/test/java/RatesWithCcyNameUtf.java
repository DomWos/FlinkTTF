import org.apache.avro.util.Utf8;

import java.util.Objects;

public class RatesWithCcyNameUtf {

    private Utf8 ratesCcyIsoCode;
    private Double rate;
    private long ts;
    private Utf8 ccyName;

    public RatesWithCcyNameUtf(Utf8 ratesCcyIsoCode, Double rate, long ts, Utf8 ccyName) {
        this.ratesCcyIsoCode = ratesCcyIsoCode;
        this.rate = rate;
        this.ts = ts;
        this.ccyName = ccyName;
    }

    public RatesWithCcyNameUtf() { }

    public Utf8 getRatesCcyIsoCode() {
        return ratesCcyIsoCode;
    }

    public void setRatesCcyIsoCode(Utf8 ratesCcyIsoCode) {
        this.ratesCcyIsoCode = ratesCcyIsoCode;
    }

    public Double getRate() {
        return rate;
    }

    public void setRate(Double rate) {
        this.rate = rate;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public Utf8 getCcyName() {
        return ccyName;
    }

    public void setCcyName(Utf8 ccyName) {
        this.ccyName = ccyName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RatesWithCcyNameUtf that = (RatesWithCcyNameUtf) o;
        return getTs() == that.getTs() &&
                getRatesCcyIsoCode().equals(that.getRatesCcyIsoCode()) &&
                getRate().equals(that.getRate()) &&
                getCcyName().equals(that.getCcyName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getRatesCcyIsoCode(), getRate(), getTs(), getCcyName());
    }

    @Override
    public String toString() {
        return "RatesWithCcyName{" +
                "ratesCcyIsoCode='" + ratesCcyIsoCode + '\'' +
                ", rate=" + rate +
                ", ts=" + ts +
                ", ccyName='" + ccyName + '\'' +
                '}';
    }
}
