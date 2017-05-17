# Description

Teraslice reader to process JSON data stored in text files in HDFS.

# Output

An array of JSON format records

# Parameters

| Name | Description | Default | Required |
| ---- | ----------- | ------- | -------- |
| user | User to use when reading the files | hdfs | N |
| path | HDFS location to process. Most of the time this will be a directory that contains multiple files |  | Y |
| size | How big of a slice in bytes to take out of each file | 100000 | N |
| format | Format of the records that are stored in the file. Currently json_lines is the only option. | json_lines | N |
| connection | Name of the terafoundation HDFS connection to use | default | N |

# Job configuration example

Read a directory in HDFS and send the results to an elasticsearch index.

```
{
    "name": "Process",
    "lifecycle": "once",
    "workers": 2,
    "operations": [
        {
          "_op": "teraslice_hdfs_reader",
          "path": "/testpath/test-2017.04.24",
          "size": 5000000
        },
        {
            "_op": "elasticsearch_index_selector",
            "type": "change",
            "index": "example-logs-from-hdfs"
        },
        {
          "_op": "elasticsearch_bulk",
          "size": 10000
        }
    ]
}
```
