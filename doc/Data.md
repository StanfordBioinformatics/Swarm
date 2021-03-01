# Swarm Data Sources

Swarm reads several tables from cloud providers during the servicing of a query. The primary tables are variant (in VCF-style orientation) tables and annotation tables keyed on VCF-style variant columns. The variant table must contain a set of VCF columns alongside a set of sample columns. The required VCF columns are shown [in Controller.java](https://github.com/StanfordBioinformatics/swarm/blob/790efb53e74002692bcd70456f390adc02bc64d9/src/main/java/app/api/Controller.java#L58-L69). Annotation tables must contain the columns `reference_name`, `start_position`, `end_position`, `reference_bases`, `alternate_bases` as shown [in AthenaClient.java](https://github.com/StanfordBioinformatics/swarm/blob/790efb53e74002692bcd70456f390adc02bc64d9/src/main/java/app/dao/client/AthenaClient.java#L69-L74)

BigQuery can read tables with data stored in any region and write native tables to us-central1 (though reading from other regions *may* incur additional cost). For Athena, data can be read from any region and written to the same region by specifying the region in the API calls. This region right now must be configured in both the [awsRegion line at the top of Controller.java](https://github.com/StanfordBioinformatics/swarm/blob/790efb53e74002692bcd70456f390adc02bc64d9/src/main/java/app/api/Controller.java#L56), and [the region line of AthenaClient](https://github.com/StanfordBioinformatics/swarm/blob/790efb53e74002692bcd70456f390adc02bc64d9/src/main/java/app/dao/client/AthenaClient.java#L45).

The variant and annotation tables for each platform are specified and can be changed at the [top of Controller.java](https://github.com/StanfordBioinformatics/swarm/blob/790efb53e74002692bcd70456f390adc02bc64d9/src/main/java/app/api/Controller.java#L73-L78).

In addition to these tables, Swarm for some endpoints allows use of rsid to specify a variant, and uses a DBSNP table to resolve these identifiers. This table name is specified [in this line](https://github.com/StanfordBioinformatics/swarm/blob/790efb53e74002692bcd70456f390adc02bc64d9/src/main/java/app/api/Controller.java#L80) at the top of Controller.java, and Swarm expects this to exist in BigQuery. Replicating the [getRsidLocation](https://github.com/StanfordBioinformatics/swarm/blob/790efb53e74002692bcd70456f390adc02bc64d9/src/main/java/app/api/Controller.java#L341) function for each database client would make this more modular and swappable between database backends. The DBSNP table must contain at least the following schema, where `ID` contains rsid values to resolve queries against:
```
chrm  STRING
start INTEGER
end   INTEGER
base  STRING
alt   STRING
ID    STRING
```