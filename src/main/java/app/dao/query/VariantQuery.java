package app.dao.query;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

public class VariantQuery {
    private String referenceName;
    private Long startPosition;
    private Long endPosition;
    private String referenceBases;
    private String alternateBases;

    private Double minorAF;
    private Double minorAFMarginOfErrorPercentage;

    private NumericValueComparisonOperator startPositionComparisonOperator;
    private NumericValueComparisonOperator endPositionComparisonOperator;
    private NumericValueComparisonOperator minorAFPositionComparisonOperator;


    private String tableIdentifier;
    //private boolean usePositionAsRange = false;

    /**
     * Construct while requiring a table identifier.
     * //@param tableIdentifier fully qualified table identifier
     */
    /*public VariantQuery(
            @NotNull String tableIdentifier
    ) {
        setTableIdentifier(tableIdentifier);
    }*/

    public VariantQuery() {
        setStartPositionComparisonOperator(NumericValueComparisonOperator.DEFAULT);
        setEndPositionComparisonOperator(NumericValueComparisonOperator.DEFAULT);
        setMinorAFComparisonOperator(NumericValueComparisonOperator.DEFAULT);
    }

    public enum NumericValueComparisonOperator {
        GREATER(">"),
        LESS("<"),
        GREATER_OR_EQUAL(">="),
        LESS_OR_EQUAL("<="),
        EQUAL("="),
        NOT_EQUAL("<>");

        static final NumericValueComparisonOperator DEFAULT = EQUAL;

        private String operator;
        NumericValueComparisonOperator(String operator) {
            this.operator = operator;
        }

        @Override
        public String toString() {
            return this.operator;
        }
    }

    public String getReferenceName() {
        return referenceName;
    }

    public void setReferenceName(@Nullable String referenceName) {
        this.referenceName = referenceName;
    }

    public Long getStartPosition() {
        return startPosition;
    }

    public void setStartPosition(@Nullable Long startPosition) {
        this.startPosition = startPosition;
    }

    public Long getEndPosition() {
        return endPosition;
    }

    public void setEndPosition(@Nullable Long endPosition) {
        this.endPosition = endPosition;
    }

    public String getReferenceBases() {
        return referenceBases;
    }

    public void setReferenceBases(@Nullable String referenceBases) {
        this.referenceBases = referenceBases;
    }

    public String getAlternateBases() {
        return alternateBases;
    }

    public void setAlternateBases(@Nullable String alternateBases) {
        this.alternateBases = alternateBases;
    }

    public Double getMinorAF() {
        return minorAF;
    }

    public void setMinorAF(@Nullable Double minorAF) {
        this.minorAF = minorAF;
    }

    public Double getMinorAFMarginOfErrorPercentage() {
        return this.minorAFMarginOfErrorPercentage;
    }

    public void setMinorAFMarginOfErrorPercentage(@Nullable Double minorAFMarginOfErrorPercentage) {
        this.minorAFMarginOfErrorPercentage = minorAFMarginOfErrorPercentage;
    }

    /**
     * Returns the minorAF upper bound, accounting for minorAFMarginOfErrorPercentage
     */
    public Double getMinorAFUpperBound() {
        if (getMinorAF() == null) {
            return null;
        } else if (getMinorAFMarginOfErrorPercentage() == null) {
            return getMinorAF();
        } else {
            return getMinorAF() + (getMinorAF() * getMinorAFMarginOfErrorPercentage());
        }
    }

    /**
     * Returns the minorAF lower bound, accounting for minorAFMarginOfErrorPercentage
     */
    public Double getMinorAFLowerBound() {
        if (getMinorAF() == null) {
            return null;
        } else if (getMinorAFMarginOfErrorPercentage() == null) {
            return getMinorAF();
        } else {
            return getMinorAF() - (getMinorAF() * getMinorAFMarginOfErrorPercentage());
        }
    }

    public NumericValueComparisonOperator getStartPositionOperator() {
        return startPositionComparisonOperator;
    }

    public void setStartPositionComparisonOperator(@NotNull NumericValueComparisonOperator startPositionComparisonOperator) {
        this.startPositionComparisonOperator = startPositionComparisonOperator;
    }

    public NumericValueComparisonOperator getEndPositionOperator() {
        return endPositionComparisonOperator;
    }

    public void setEndPositionComparisonOperator(@NotNull NumericValueComparisonOperator endPositionOperator) {
        this.endPositionComparisonOperator = endPositionOperator;
    }

    /**
     * Sets startPositionOperator to GREATER_OR_EQUAL and
     * endPositionOperator to LESS_OR_EQUAL in order to treat
     * the start and end as an inclusive range.
     */
    public void setUsePositionAsRange() {
        setStartPositionComparisonOperator(NumericValueComparisonOperator.GREATER_OR_EQUAL);
        setEndPositionComparisonOperator(NumericValueComparisonOperator.LESS_OR_EQUAL);
    }

    public void setMinorAFComparisonOperator(@NotNull NumericValueComparisonOperator minorAFComparisonOperator) {
        this.minorAFPositionComparisonOperator = minorAFComparisonOperator;
    }

    public NumericValueComparisonOperator getMinorAFPositionOperator() {
        return this.minorAFPositionComparisonOperator;
    }

    /**
     * Sets the table identifier.  This should be a sufficiently-qualified identifier for the table.
     *
     * For BigQuery, something of the form: `dataset.tablename` is sufficient.
     *
     * @param tableIdentifier
     */
    public void setTableIdentifier(String tableIdentifier) {
        this.tableIdentifier = tableIdentifier;
    }

    public String getTableIdentifier() {
        return this.tableIdentifier;
    }
}
