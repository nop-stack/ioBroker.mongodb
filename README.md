# ioBroker.mongodb

An ioBroker history adapter that stores datapoint values in an external MongoDB database.

## Development status

This is an early development scaffold. It currently supports direct MongoDB connection, recording configured datapoint IDs, and an initial subset of the history message interface. It is not yet a drop-in replacement for `ioBroker.sql`: chart aggregation, retention, buffering/retry handling, adapter-admin datapoint selection, and the full history API still need to be implemented.

## Configuration

Configure the MongoDB host, port, database, collection and optional credentials. `enabledIds` is currently a JSON array of datapoint IDs, for example:

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

## Message interface in this first version

- `getHistory`: returns stored values for one datapoint and optional `start` / `end` bounds.
- `storeState`: writes one or more supplied values.
- `delete`, `deleteAll`, `deleteRange`: deletes matching history documents.

See the SQL adapter's documentation for the broader history interface that this adapter aims to support.
