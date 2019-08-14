package app.dao;

public class VariantDescriptor {
    private String referenceName;
    private Integer startPosition;
    private Integer endPosition;
    private String referenceBases;
    private String alternateBases;

    public VariantDescriptor(
            String referenceName,
            Integer startPosition,
            Integer endPosition,
            String referenceBases,
            String alternateBases
    ) {
        this.referenceName = referenceName;
        this.startPosition = startPosition;
        this.endPosition = endPosition;
        this.referenceBases = referenceBases;
        this.alternateBases = alternateBases;
    }

    public String getReferenceName() {
        return referenceName;
    }

    public Integer getStartPosition() {
        return startPosition;
    }

    public Integer getEndPosition() {
        return endPosition;
    }

    public String getReferenceBases() {
        return referenceBases;
    }

    public String getAlternateBases() {
        return alternateBases;
    }
}
