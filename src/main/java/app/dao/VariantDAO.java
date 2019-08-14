package app.dao;

public interface VariantDAO {

    /**
     * Returns true if a variant exists in the dataset
     * @param variant variant to look for
     * @return boolean
     */
    boolean variantExists(VariantDescriptor variant);

    /**
     * Get the number of variants matching the description
     * @param variant
     * @return
     */
    boolean variantCount(VariantDescriptor variant);

}
