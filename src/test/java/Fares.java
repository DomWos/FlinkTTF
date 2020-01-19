import java.util.Objects;

public class Fares {
    private String fareCcyIsoCode;
    private Double price;
    private long ts;
    public Fares() { }

    public String getFareCcyIsoCode() {
        return fareCcyIsoCode;
    }

    public void setFareCcyIsoCode(String fareCcyIsoCode) {
        this.fareCcyIsoCode = fareCcyIsoCode;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
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
        Fares fares = (Fares) o;
        return getTs() == fares.getTs() &&
                getFareCcyIsoCode().equals(fares.getFareCcyIsoCode()) &&
                getPrice().equals(fares.getPrice());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getFareCcyIsoCode(), getPrice(), getTs());
    }

    @Override
    public String toString() {
        return "Fares{" +
                "fareCcyIsoCode='" + fareCcyIsoCode + '\'' +
                ", price=" + price +
                ", ts=" + ts +
                '}';
    }
}
