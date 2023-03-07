## Druid Median Extension

This extension allow executing query aggregation on Druid db using median function 

### Build

To build the extension, run `mvn package` and you'll get a file in `target` like this:

```
[INFO] Building tar: /src/druid-median-extension/target/druid-median-extension-0.13.0_1-SNAPSHOT-bin.tar.gz
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 4.841 s
[INFO] Finished at: 2045-11-04T20:00:53Z
[INFO] Final Memory: 21M/402M
[INFO] ------------------------------------------------------------------------
```

Unpack the tar.gz and you'll find a directory named `druid-median-extension` inside it:

```
$ tar xzf target/druid-median-extension-0.13.0_1-SNAPSHOT-bin.tar.gz
$ ls druid-median-extension-0.10.0_1-SNAPSHOT/
LICENSE                  README.md                druid-median-extension/
```

### Install

To install the extension:

1. Copy `druid-median-extension` into your Druid `extensions` directory.
2. Edit `conf/_common/common.runtime.properties` to add `"druid-median-extension"` to `druid.extensions.loadList`. (Edit `conf-quickstart/_common/common.runtime.properties` too if you are using the quickstart config.)
It should look like: `druid.extensions.loadList=["druid-median-extension"]`. There may be a few other extensions there
too.
3. Restart Druid.

### Use

#### Wiki example
To use the example extractionFn, call it like a normal extractionFn with type "example", e.g. in a
topN. It returns the first "length" characters of each value.

```json
{
  "queryType": "topN",
  "dataSource": "wikiticker",
  "intervals": [
    "2016-06-27/2016-06-28"
  ],
  "granularity": "all",
  "dimension": {
    "type": "extraction",
    "dimension": "page"
  },
  "metric": "edits",
  "threshold": 25,
  "aggregations": [
    {
      "type": "median",
      "name": "edits",
      "fieldName": "count"
    }
  ]
}
```