import java.util.Objects;

public class TaxiFare {
    private String fareCcyIsoCode;
    private double price;
    private long ts;

    public TaxiFare() {    }

    public String getFareCcyIsoCode() {
        return fareCcyIsoCode;
    }

    public void setFareCcyIsoCode(String fareCcyIsoCode) {
        this.fareCcyIsoCode = fareCcyIsoCode;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
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
        TaxiFare taxiFare = (TaxiFare) o;
        return Double.compare(taxiFare.getPrice(), getPrice()) == 0 &&
                getTs() == taxiFare.getTs() &&
                getFareCcyIsoCode().equals(taxiFare.getFareCcyIsoCode());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getFareCcyIsoCode(), getPrice(), getTs());
    }

    @Override
    public String toString() {
        return "TaxiFare{" +
                "fareCcyIsoCode='" + fareCcyIsoCode + '\'' +
                ", price=" + price +
                ", ts=" + ts +
                '}';
    }
}
