-- filter and sort to relevant values, remove from prod
select * from
    -- add up the counts, by expressions grouped by the superpopulation
    (select
        (HG01488_alleleCount + HG00632_alleleCount) as AFR_alleleCount
    from
        -- select the variant information, and convert the sample genotype to an variant allele count
        (select reference_name, pos, id, ref, alt,
            HG01488,
            (case HG01488
              when '0|0' then 0
              when '0|1' then 1
              when '0|2' then 1
              when '0|3' then 1
              when '1|0' then 1
              when '1|1' then 2
              when '1|2' then 2
              when '1|3' then 2
              when '2|0' then 1
              when '2|1' then 2
              when '2|2' then 2
              when '2|3' then 2
              when '3|0' then 1
              when '3|1' then 2
              when '3|2' then 2
              when '3|3' then 2
              else 0
              end
            ) as HG01488_alleleCount,
            HG00632,
            (case HG00632
              when '0|0' then 0
              when '0|1' then 1
              when '0|2' then 1
              when '0|3' then 1
              when '1|0' then 1
              when '1|1' then 2
              when '1|2' then 2
              when '1|3' then 2
              when '2|0' then 1
              when '2|1' then 2
              when '2|2' then 2
              when '2|3' then 2
              when '3|0' then 1
              when '3|1' then 2
              when '3|2' then 2
              when '3|3' then 2
              else 0
              end
            ) as HG00632_alleleCount
        from `gbsc-gcp-project-annohive-dev.1000.1000Orig_half1`) as count_conversion
    )
where AFR_alleleCount > 0
order by AFR_alleleCount desc