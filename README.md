# tap-s3-csv
[Singer](singer.io) tap that produces JSON-formatted data following
the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

Given a configuration that specifies a bucket, a file pattern to match, a file format (`csv` or `excel`),
and a table name, this tap reads new files from S3, parses them, infers a schema, and outputs the data
according to the Singer spec.

### Installation

wip

### Example

Given a source file: `s3://csv-bucket/csv-exports/today.csv`

```csv
id,First Name, Last Name
1,Michael,Bluth
2,George Michael,Bluth
3,Tobias,Funke
```

And a config file:

```json
wip
```

An output record might look like:

```json
{
  "id": 3,
  "first_name": "Tobias",
  "last_name": "Funke",
  "_s3_source_bucket": "csv-bucket",
  "_s3_source_file": "csv-exports/today.csv",
  "_s3_source_lineno": 4
}
```

### Input File Requirements

Your CSV or Excel files MUST:

- Have a header row.
- Have cells fully populated. Empty / missing cells will break the integration.

### Configuration Format

### Output Format

- Column names have whitespace removed and replaced with underscores.
- They are also downcased.
- A few extra fields are added for help with auditing:
  - `_s3_source_bucket`: The bucket that this record came from
  - `_s3_source_file`: The path to the file that this record came from
  - `_s3_source_lineno`: The line number in the source file that this record was found on
 
