SELECT
  reference_name,
  start_position,
  end_position,
  reference_bases,
  alternate_bases,
  quality,
  filters,
  ARRAY_AGG(AMR IGNORE NULLS) as AMR,
  ARRAY_AGG(EUR IGNORE NULLS) as EUR,
  ARRAY_AGG(AFR IGNORE NULLS) as AFR,
  ARRAY_AGG(SAS IGNORE NULLS) as SAS,
  ARRAY_AGG(EAS IGNORE NULLS) as EAS
FROM (
  SELECT
  *,
  CASE
    WHEN SuperPopulation="AMR" AND genotype="1" THEN ARRAY_LENGTH(call_set_name)
  END AS AMR,
  CASE
    WHEN SuperPopulation="EUR" AND genotype="1" THEN ARRAY_LENGTH(call_set_name)
  END AS EUR,
  CASE
    WHEN SuperPopulation="AFR" AND genotype="1" THEN ARRAY_LENGTH(call_set_name)
  END AS AFR,
  CASE
    WHEN SuperPopulation="SAS" AND genotype="1" THEN ARRAY_LENGTH(call_set_name)
  END AS SAS,
  CASE
    WHEN SuperPopulation="EAS" AND genotype="1" THEN ARRAY_LENGTH(call_set_name)
  END AS EAS
  FROM (
    SELECT
      reference_name,
      start_position,
      end_position,
      reference_bases,
      alternate_bases,
      quality,
      filters,
      ARRAY_AGG(call_set_name) AS call_set_name,
      SuperPopulation,
      genotype
    FROM
      `gbsc-gcp-project-annohive-dev.swarm.1000Genomes_Random_Flat2`
    WHERE
      reference_name="Y"
    GROUP BY
      reference_name,
      start_position,
      end_position,
      reference_bases,
      alternate_bases,
      quality,
      filters,
      SuperPopulation,
      genotype
    ORDER BY
      start_position ) AS A
)
GROUP BY
  reference_name,
  start_position,
  end_position,
  reference_bases,
  alternate_bases,
  quality,
  filters