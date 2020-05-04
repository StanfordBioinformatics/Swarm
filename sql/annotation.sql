SELECT
    VCF.reference_name,
    VCF.start_position,
    VCF.end_position,
    VCF.reference_bases,
    VCF.alternate_bases,
    Annotation1.* except( reference_name, start_position, end_position, reference_bases, alternate_bases)
  FROM (
    SELECT
      *
    FROM (
      SELECT
        REPLACE(reference_name, '', '') AS reference_name,
        start_position,
        `end_position`,
        reference_bases,
        alternate_bases
      FROM
        %s) ) AS VCF
  JOIN
    `gbsc-gcp-project-annohive-dev.swarm.hg19_Variant_9B_Table` AS Annotation1
  ON
    (Annotation1.reference_name = VCF.reference_name)
    AND (Annotation1.start_position = VCF.start_position)
    AND (Annotation1.end_position = VCF.end_position)
  WHERE
    (((Annotation1.reference_bases = "")
        AND (CONCAT(VCF.reference_bases, Annotation1.alternate_bases) = VCF.alternate_bases)
        OR Annotation1.alternate_bases = VCF.alternate_bases)
      AND (VCF.reference_bases = Annotation1.reference_bases))