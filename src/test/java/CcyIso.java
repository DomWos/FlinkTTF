import java.util.Objects;

public class CcyIso {
    private String ccyIsoCode;
    private String ccyIsoName;
    private long ts;
    public CcyIso(){}

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
        CcyIso ccyIso = (CcyIso) o;
        return getTs() == ccyIso.getTs() &&
                getCcyIsoCode().equals(ccyIso.getCcyIsoCode()) &&
                getCcyIsoName().equals(ccyIso.getCcyIsoName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getCcyIsoCode(), getCcyIsoName(), getTs());
    }

    @Override
    public String toString() {
        return "CcyIso{" +
                "ccyIsoCode='" + ccyIsoCode + '\'' +
                ", ccyIsoName='" + ccyIsoName + '\'' +
                ", ts=" + ts +
                '}';
    }
}
