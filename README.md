# ioBroker.mongodb

An ioBroker history adapter that stores datapoint values in an external MongoDB database.

## Development status

The adapter records state changes for configured datapoint IDs and supports the core history message interface, including aggregated `getHistory` queries and paginated raw reads. It is not yet a drop-in replacement for `ioBroker.sql`: retention, buffering/retry handling, adapter-admin datapoint selection, and the full history API still need to be implemented.

## Configuration

Configure the complete MongoDB connection string and collection. For MongoDB Atlas, use a URI such as:

```text
mongodb+srv://<username>:<password>@iobroker-data.s8yl5.mongodb.net/iobroker
```

For a local server, the default is `mongodb://127.0.0.1:27017/iobroker`. Usernames and passwords containing reserved URI characters must be percent-encoded. The connection string is stored encrypted but remains readable in the configuration field. Enter each datapoint ID in the `enabledIds` field and confirm it to add it to the list. For example:

```json
["javascript.0.temperature", "zigbee.0.sensor.humidity"]
```

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

## History messages

- `getHistory`: reads a datapoint's history using the standard `{id, options}` message format and shared ioBroker aggregation.
- `getRawEntries`: returns stored rows for one datapoint with range, sorting, and pagination options.
- `storeState`: writes one or more supplied values.
- `delete`, `deleteAll`, `deleteRange`: deletes matching history documents.

See the SQL adapter's documentation for the broader history interface that this adapter aims to support.
