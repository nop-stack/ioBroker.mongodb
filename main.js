'use strict';

const utils = require('@iobroker/adapter-core');
const { MongoClient } = require('mongodb');

class MongoDBHistory extends utils.Adapter {
    constructor(options = {}) {
        super({ ...options, name: 'mongodb' });
        this.client = null;
        this.collection = null;
        this.enabledIds = new Set();
        this.on('ready', this.onReady.bind(this));
        this.on('stateChange', this.onStateChange.bind(this));
        this.on('message', this.onMessage.bind(this));
        this.on('unload', this.onUnload.bind(this));
    }

    async onReady() {
        const ids = Array.isArray(this.config.enabledIds) ? this.config.enabledIds : [];
        this.enabledIds = new Set(ids.filter(id => typeof id === 'string' && id.trim()).map(id => id.trim()));

        try {
            const auth = this.config.username
                ? `${encodeURIComponent(this.config.username)}:${encodeURIComponent(this.config.password || '')}@`
                : '';
            const host = this.config.host || '127.0.0.1';
            const port = Number(this.config.port) || 27017;
            const database = this.config.database || 'iobroker';
            const uri = `mongodb://${auth}${host}:${port}/${encodeURIComponent(database)}`;
            this.client = new MongoClient(uri, { serverSelectionTimeoutMS: 5000 });
            await this.client.connect();
            const db = this.client.db(database);
            this.collection = db.collection(this.config.collection || 'history');
            await this.collection.createIndex({ id: 1, ts: 1 });
            await this.setStateAsync('info.connection', true, true);
            this.subscribeForeignStates('*');
            this.log.info(`Connected to MongoDB ${host}:${port}/${database}`);
        } catch (error) {
            await this.setStateAsync('info.connection', false, true);
            this.log.error(`MongoDB connection failed: ${error.message}`);
            if (this.client) {
                try {
                    await this.client.close();
                } catch {
                    // Ignore cleanup errors while recovering from a failed connection.
                }
                this.client = null;
                this.collection = null;
            }
            this.setTimeout(() => this.onReady(), 15000);
        }
    }

    async onStateChange(id, state) {
        if (!state || !this.collection || !this.enabledIds.has(id)) return;
        try {
            await this.collection.insertOne(this.toDocument(id, state));
        } catch (error) {
            this.log.warn(`Could not store history for ${id}: ${error.message}`);
        }
    }

    toDocument(id, state) {
        return {
            id,
            ts: Number.isFinite(state.ts) ? state.ts : Date.now(),
            val: state.val,
            ack: Boolean(state.ack),
            q: Number.isFinite(state.q) ? state.q : 0,
            from: state.from || null
        };
    }

    async onMessage(obj) {
        if (!obj || !obj.callback) return;
        let result;
        try {
            switch (obj.command) {
                case 'getHistory':
                    result = await this.getHistory(obj.message || {});
                    break;
                case 'storeState':
                    result = await this.storeState(obj.message);
                    break;
                case 'update':
                    result = await this.updateEntries(obj.message);
                    break;
                case 'delete':
                    result = await this.deleteEntries(obj.message, false);
                    break;
                case 'deleteAll':
                    result = await this.deleteEntries(obj.message, true);
                    break;
                case 'deleteRange':
                    result = await this.deleteRange(obj.message);
                    break;
                default:
                    result = { error: `Unsupported command: ${obj.command}` };
            }
        } catch (error) {
            result = { error: error.message };
        }
        this.sendTo(obj.from, obj.command, result, obj.callback);
    }

    async getHistory(message) {
        if (!message.id) throw new Error('id is required');
        const query = { id: message.id };
        if (message.start != null || message.end != null) {
            query.ts = {};
            if (message.start != null) query.ts.$gte = this.toTimestamp(message.start);
            if (message.end != null) query.ts.$lte = this.toTimestamp(message.end);
        }
        const result = await this.collection.find(query).sort({ ts: 1 }).toArray();
        return {
            result: result.map(({ ts, val, ack, q, from }) => ({ ts, val, ack, q, from })),
            step: 0
        };
    }

    async storeState(message) {
        const entries = Array.isArray(message) ? message : [message];
        let successCount = 0;
        const errors = [];
        for (const entry of entries) {
            if (!entry || !entry.id) {
                errors.push('id is required');
                continue;
            }
            const states = Array.isArray(entry.state) ? entry.state : [entry.state];
            for (const state of states) {
                if (!state || !Number.isFinite(Number(state.ts))) {
                    errors.push(`A valid timestamp is required for ${entry.id}`);
                    continue;
                }
                try {
                    await this.collection.insertOne(this.toDocument(entry.id, { ...state, ts: Number(state.ts) }));
                    successCount++;
                } catch (error) {
                    errors.push(`${entry.id}: ${error.message}`);
                }
            }
        }
        return { success: errors.length === 0, successCount, errors };
    }

    async deleteEntries(message, all) {
        const entries = Array.isArray(message) ? message : [message];
        let deletedCount = 0;
        for (const entry of entries) {
            if (!entry || !entry.id) throw new Error('id is required');
            const filter = { id: entry.id };
            if (!all) {
                if (!entry.state || !Number.isFinite(Number(entry.state.ts))) {
                    throw new Error(`A valid timestamp is required for delete on ${entry.id}`);
                }
                filter.ts = Number(entry.state.ts);
            }
            const deleted = await this.collection.deleteMany(filter);
            deletedCount += deleted.deletedCount;
        }
        return { success: true, deletedCount };
    }

    async updateEntries(message) {
        const entries = Array.isArray(message) ? message : [message];
        let updatedCount = 0;
        for (const entry of entries) {
            if (!entry || !entry.id || !entry.state || !Number.isFinite(Number(entry.state.ts))) {
                throw new Error('id and a valid state.ts are required');
            }
            const fields = {};
            for (const key of ['val', 'ack', 'q', 'from']) {
                if (Object.prototype.hasOwnProperty.call(entry.state, key)) fields[key] = entry.state[key];
            }
            if (!Object.keys(fields).length) throw new Error(`No fields to update for ${entry.id}`);
            const result = await this.collection.updateOne(
                { id: entry.id, ts: Number(entry.state.ts) },
                { $set: fields }
            );
            updatedCount += result.modifiedCount;
        }
        return { success: true, updatedCount };
    }

    async deleteRange(message) {
        const entries = Array.isArray(message) ? message : [message];
        let deletedCount = 0;
        for (const entry of entries) {
            if (!entry || !entry.id) throw new Error('id is required');
            const filter = { id: entry.id, ts: {} };
            if (entry.start != null) filter.ts.$gte = this.toTimestamp(entry.start);
            if (entry.end != null) filter.ts.$lte = this.toTimestamp(entry.end);
            if (!Object.keys(filter.ts).length) throw new Error('start or end is required');
            const deleted = await this.collection.deleteMany(filter);
            deletedCount += deleted.deletedCount;
        }
        return { success: true, deletedCount };
    }

    toTimestamp(value) {
        const timestamp = typeof value === 'number' ? value : new Date(value).getTime();
        if (!Number.isFinite(timestamp)) throw new Error('Invalid timestamp');
        return timestamp;
    }

    async onUnload(callback) {
        try {
            await this.setStateAsync('info.connection', false, true);
            if (this.client) await this.client.close();
        } catch (error) {
            this.log.warn(`Error while closing MongoDB connection: ${error.message}`);
        }
        callback();
    }
}

if (module.parent) {
    module.exports = options => new MongoDBHistory(options);
} else {
    new MongoDBHistory();
}
