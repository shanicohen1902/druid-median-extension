## Druid Median Extension

The median extension allows executing query aggregation tasks on Druid db

### Build

To build the extension, run `mvn package` and you'll get a file in `target` like this:

```
[INFO] Building tar: /src/druid-median-extension/target/druid-median-extension-0.24.0-SNAPSHOT.jar
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
To use the median aggregation, call it like a normal aggregation with type "median", e.g. in a
topN. It returns the median value of each value.

```json
{
  "queryType": "groupBy",
  "dataSource": "wikipedia",
  "intervals": [
    "2016-06-27/2016-06-28"
  ],
  "granularity": "all",
  "dimensions": ["cityName"],
  "aggregations": [
    {
      "type": "median",
      "name": "commentLengthMedian",
      "fieldName": "commentLength"
    }
  ]
}
```

result -

| cityName   | commentLengthMedian |
|------------|---------------------|
| null       | 1                   |
| A Coruña   | 30                  |
| Aachen     | 23                  |
| Abbotsford | 10                  |
| Abu Dhabi  | 15                  |
| Afragola   | 40                  |
| Aglayan    | 11                  |
| Ahmedabad  | 13                  |
| Al Ain     | 24                  |