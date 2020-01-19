import java.util.Objects;

public class RatesWithCcyName {

    private String ratesCcyIsoCode;
    private Double rate;
    private long ts;
    private String ccyName;

    public RatesWithCcyName() { }

    public String getRatesCcyIsoCode() {
        return ratesCcyIsoCode;
    }

    public void setRatesCcyIsoCode(String ratesCcyIsoCode) {
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

    public String getCcyName() {
        return ccyName;
    }

    public void setCcyName(String ccyName) {
        this.ccyName = ccyName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RatesWithCcyName that = (RatesWithCcyName) o;
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
