SELECT
    main.start,
    main.`end`,
    main.reference_bases,
    main.alternate_bases,
    AC,
    AMR1 ,
    EUR1,
    AFR1,
    SAS1,
    EAS1,
    AMR as AMR2,
    EUR as EUR2,
    AFR as AFR2,
    SAS as SAS2,
    EAS as EAS2
  FROM
    (
  SELECT
    main.reference_name,
    main.start,
    main.`end`,
    main.reference_bases,
    main.alternate_bases,
    AC,
    AMR as AMR1,
    EUR as EUR1,
    AFR as AFR1,
    SAS as SAS1,
    EAS as EAS1
  FROM (
    SELECT
      reference_name,
      start,
      `end`,
      reference_bases,
      alternate_bases,
      AC
    FROM
      `genomics-public-data.1000_genomes_phase_3.variants_20150220_release`,
      UNNEST (alternate_bases) AS alternate_bases) AS main
  JOIN
    `gbsc-gcp-project-annohive-dev.swarm.1000Genomes_Random_Flat1_chrY_ACs`  AS half1
  ON
    main.reference_name=half1.reference_name
    AND main.reference_bases=half1.reference_bases
    AND main.start=half1.start_position
    AND main.end=half1.end_position
    AND main.alternate_bases=half1.alternate_bases) as main
  JOIN
    `gbsc-gcp-project-annohive-dev.swarm.1000Genomes_Random_Flat2_chrY_ACs`  AS half2
  ON
    main.reference_name=half2.reference_name
    AND main.reference_bases=half2.reference_bases
    AND main.alternate_bases=half2.alternate_bases
    AND main.start=half2.start_position
    AND main.end=half2.end_position
