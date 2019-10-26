SELECT
    start,
    `end`,
    reference_bases,
    alternate_bases,
    AMR1, EUR1, AFR1, SAS1, EAS1, AMR2, EUR2, AFR2, SAS2, EAS2,
    (AMR1+ EUR1+ AFR1+ SAS1+ EAS1+ AMR2+ EUR2+ AFR2+ SAS2+ EAS2) AS AC_Fake,
    AC
FROM (
  SELECT
    start,
    `end`,
    reference_bases,
    alternate_bases,
    AMR1,
    EUR1,
    AFR1,
    SAS1,
    EAS1,
    AMR2,
    EUR2,
    AFR2,
    SAS2,
    EAS2,
    AC
  FROM
    `gbsc-gcp-project-annohive-dev.swarm.1000Genomes_Flat1_AND_Flat2_vs_Orig_ChrY2`,
    UNNEST (AMR1) AS AMR1,
    UNNEST (EUR1) AS EUR1,
    UNNEST (AFR1) AS AFR1,
    UNNEST (SAS1) AS SAS1,
    UNNEST (EAS1) AS EAS1,
    UNNEST (AMR2) AS AMR2,
    UNNEST (EUR2) AS EUR2,
    UNNEST (AFR2) AS AFR2,
    UNNEST (SAS2) AS SAS2,
    UNNEST (EAS2) AS EAS2,
    UNNEST (AC) AS AC )
WHERE
  AC!=(AMR1+ EUR1+ AFR1+ SAS1+ EAS1+ AMR2+ EUR2+ AFR2+ SAS2+ EAS2 )
ORDER BY
  start