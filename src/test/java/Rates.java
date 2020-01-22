import java.util.Objects;

public class Rates {
    private String ratesCcyIsoCode;
    private double rate;
    private long ts;

    public Rates() {    }

    public String getRatesCcyIsoCode() {
        return ratesCcyIsoCode;
    }

    public void setRatesCcyIsoCode(String ratesCcyIsoCode) {
        this.ratesCcyIsoCode = ratesCcyIsoCode;
    }

    public double getRate() {
        return rate;
    }

    public void setRate(double rate) {
        this.rate = rate;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Rates rates = (Rates) o;
        return Double.compare(rates.getRate(), getRate()) == 0 &&
                getTs() == rates.getTs() &&
                getRatesCcyIsoCode().equals(rates.getRatesCcyIsoCode());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getRatesCcyIsoCode(), getRate(), getTs());
    }

    @Override
    public String toString() {
        return "Rates{" +
                "ratesCcyIsoCode='" + ratesCcyIsoCode + '\'' +
                ", rate=" + rate +
                ", ts=" + ts +
                '}';
    }
}
