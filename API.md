### Existence stat
```
/stat
params:
- reference_name (string)
- start_position (int64)
- end_position (int64)
- reference_bases (string of [ACTG]+)
- alternate_bases (string of [ACTG]+)
- position_range (boolean, default=true)

/stat_by_gene/{gene_name}
params: {same as /stat}
path_params:
- gene_name (string, must be a known gene name in UCSC GoldenPath dataset)
```
### Variant counting
```
/count
```
### Variant data query
```
/variants
params: {same as /stat}

/variants_by_gene/{gene_name}
params: {same as /variants}
path_params: {same as /stat_by_gene/{gene_name}}
```
### Annotation data query
The assumption is that a variant table already exists, either as a permanent table
or as the result of a variant query from `/variants`.
```
The 

```

### Response Format

If `data_count` is zero, then keys `"headers"` and `"data"` may or may not exist

```json
{
    "swarm_database_type": "athena|bigquery",
    "swarm_database_name": "swarm",
    "swarm_database_table": "genome_query_<random-value>",
    "data_count": 2, 
    "headers": [
        "reference_name",
        "start_position",
        "end_position",
        "reference_bases",
        "alternate_bases",
        "minor_af",
        "allele_count"],
    "data": [
        ["1", 1000, 1001, "A", "T", 0.005, 10],
        ["1", 1010, 1012, "AA", "AT", 0.005, 10]
    ]
}
```