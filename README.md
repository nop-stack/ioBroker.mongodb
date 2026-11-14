# ioBroker.mongodb

An ioBroker history adapter that stores datapoint values in an external MongoDB database.

## Development status

The adapter records state changes for datapoints with MongoDB history enabled in their ioBroker object settings. It supports the core history message interface, including aggregated `getHistory` queries and paginated raw reads. It does not currently buffer writes while MongoDB is unavailable.

## Configuration

Configure the complete MongoDB connection string and collection. For MongoDB Atlas, use a URI such as:

```text
mongodb+srv://<username>:<password>@iobroker-data.s8yl5.mongodb.net/iobroker
```

For a local server, the default is `mongodb://127.0.0.1:27017/iobroker`. Usernames and passwords containing reserved URI characters must be percent-encoded. The connection string is stored encrypted but remains readable in the configuration field.

Enable logging on each datapoint in Admin's object settings under **History** and select this MongoDB instance. The adapter then records its future state changes automatically; it does not import old values from another history adapter. Per-datapoint settings include `changesOnly`, `debounceTime` (milliseconds), `blockTime` (milliseconds), `changesRelogInterval` (seconds), `changesMinDelta`, and `ignoreZero` (skip `0` and `null`).

The adapter creates a compound `{ id: 1, ts: 1 }` index and stores documents with this shape:

```json
{
  "id": "javascript.0.temperature",
  "ts": 1730000000000,
  "val": 21.5,
  "ack": true,
  "q": 0,
  "from": "system.adapter.javascript.0"
}
```

### Aggregation pipeline

The **Aggregation pipeline** tab accepts a MongoDB aggregation pipeline as a JSON array. The adapter checks JSON syntax, stage names, and field references against the input fields `_id`, `id`, `ts`, `val`, `ack`, `q`, and `from`. It also checks that `_id` remains available for the mapped output. Pipeline validation messages appear in the adapter log when it starts.

For each accepted history record, MongoDB runs the configured pipeline against that record and writes the result to the separate `history_mapped` collection by default. The regular history collection keeps the original ioBroker fields, so `getHistory` continues to work. For example, this maps a record to a time-series document:

```json
[
  {
    "$project": {
      "_id": 1,
      "source": { "$literal": "outdoor-temp" },
      "@timestamp": { "$toDate": "$ts" },
      "@version": { "$literal": "1" },
      "val": 1
    }
  }
]
```

The consistency check is static and covers known source fields and the one-to-one stages `$match`, `$project`, `$set`, `$addFields`, and `$unset`. Stages that can combine or multiply records, such as `$group` and `$unwind`, are rejected so each input history record remains its own output document. MongoDB performs the final expression validation when a document is processed; errors are logged while the original history record remains stored.

## History messages

- `getHistory`: reads a datapoint's history using the standard `{id, options}` message format and shared ioBroker aggregation.
- `getRawEntries`: returns stored rows for one datapoint with range, sorting, and pagination options.
- `storeState`: writes one or more supplied values.
- `delete`, `deleteAll`, `deleteRange`: deletes matching history documents.

See the SQL adapter's documentation for the broader history interface that this adapter aims to support.
