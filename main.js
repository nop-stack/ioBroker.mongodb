'use strict';

const utils = require('@iobroker/adapter-core');
const { MongoClient } = require('mongodb');
const { sendResponse } = require('@iobroker/aggregate');

class MongoDBHistory extends utils.Adapter {
    constructor(options = {}) {
        super({ ...options, name: 'mongodb' });
        this.client = null;
        this.collection = null;
        this.enabledIds = new Set();
        this.legacyEnabledIds = new Set();
        this.on('ready', this.onReady.bind(this));
        this.on('stateChange', this.onStateChange.bind(this));
        this.on('objectChange', this.onObjectChange.bind(this));
        this.on('message', this.onMessage.bind(this));
        this.on('unload', this.onUnload.bind(this));
    }

    async onReady() {
        const ids = Array.isArray(this.config.enabledIds) ? this.config.enabledIds : [];
        this.legacyEnabledIds = new Set(ids.filter(id => typeof id === 'string' && id.trim()).map(id => id.trim()));
        this.enabledIds = new Set(this.legacyEnabledIds);

        await this.setObjectNotExistsAsync('info', {
            type: 'channel',
            common: { name: 'Information' },
            native: {}
        });
        await this.setObjectNotExistsAsync('info.connection', {
            type: 'state',
            common: {
                name: 'MongoDB connection',
                type: 'boolean',
                role: 'indicator.connected',
                read: true,
                write: false
            },
            native: {}
        });
        await this.setStateAsync('info.connection', false, true);

        try {
            const configuredUri = String(this.config.connectionString || '').trim();
            const host = String(this.config.host || '127.0.0.1').trim();
            const port = Number(this.config.port) || 27017;
            let uri;
            let database;

            if (configuredUri) {
                if (!/^mongodb(?:\+srv)?:\/\//i.test(configuredUri)) {
                    throw new Error('Connection string must start with mongodb:// or mongodb+srv://');
                }
                uri = configuredUri;
                const parsedUri = new URL(configuredUri);
                database = decodeURIComponent(parsedUri.pathname.replace(/^\/+/, '').split('/')[0] || '') || 'iobroker';
            } else {
                const auth = this.config.username
                    ? `${encodeURIComponent(this.config.username)}:${encodeURIComponent(this.config.password || '')}@`
                    : '';
                const legacyDatabase = this.config.database || 'iobroker';
                const isAtlasHost = host.toLowerCase().endsWith('.mongodb.net');
                uri = isAtlasHost
                    ? `mongodb+srv://${auth}${host}/${encodeURIComponent(legacyDatabase)}`
                    : `mongodb://${auth}${host}:${port}/${encodeURIComponent(legacyDatabase)}`;
                database = legacyDatabase;
            }
            this.client = new MongoClient(uri, { serverSelectionTimeoutMS: 5000 });
            await this.client.connect();
            const db = this.client.db(database);
            this.collection = db.collection(this.config.collection || 'history');
            await this.collection.createIndex({ id: 1, ts: 1 });
            await this.setStateAsync('info.connection', true, true);
            await this.loadHistoryConfiguration();
            const target = configuredUri ? 'configured connection string' : `${host}:${port}/${database}`;
            this.log.info(`Connected to MongoDB ${target}`);
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

    onObjectChange(id, obj) {
        if (!this.collection || typeof id !== 'string') return;
        const custom = obj?.common?.custom?.[this.namespace];
        if (custom?.enabled === true || (custom == null && obj && this.legacyEnabledIds.has(id))) {
            this.enabledIds.add(id);
            this.subscribeForeignStates(id);
        } else {
            this.enabledIds.delete(id);
            this.unsubscribeForeignStates(id);
        }
    }

    async loadHistoryConfiguration() {
        this.subscribeForeignObjects('*');
        try {
            const view = await new Promise((resolve, reject) => {
                this.getObjectView('system', 'custom', {}, (error, result) => {
                    if (error) reject(error);
                    else resolve(result);
                });
            });
            const objectConfig = new Map();
            for (const row of view?.rows || []) {
                const settings = row.value?.[this.namespace];
                if (settings && typeof settings === 'object') objectConfig.set(row.id, settings);
            }

            const enabled = new Set();
            for (const [id, settings] of objectConfig) {
                if (settings.enabled === true) enabled.add(id);
            }
            for (const id of this.legacyEnabledIds) {
                if (!objectConfig.has(id)) enabled.add(id);
            }
            this.enabledIds = enabled;
            if (enabled.size) this.subscribeForeignStates([...enabled]);
            this.log.info(`History logging enabled for ${enabled.size} datapoint(s)`);
        } catch (error) {
            this.log.warn(`Could not load datapoint history settings: ${error.message}`);
            if (this.enabledIds.size) this.subscribeForeignStates([...this.enabledIds]);
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
        let responseSent = false;
        try {
            switch (obj.command) {
                case 'features':
                    result = { supportedFeatures: ['update', 'delete', 'deleteRange', 'deleteAll', 'storeState'] };
                    break;
                case 'getHistory':
                    result = await this.getHistory(obj, obj.message || {});
                    responseSent = true;
                    break;
                case 'getRawEntries':
                    result = await this.getRawEntries(obj.message || {});
                    break;
                case 'enableHistory':
                    result = await this.setHistoryEnabled(obj.message, true);
                    break;
                case 'disableHistory':
                    result = await this.setHistoryEnabled(obj.message, false);
                    break;
                case 'getEnabledDPs':
                    result = await this.getEnabledDatapoints();
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
        if (!responseSent) this.sendTo(obj.from, obj.command, result, obj.callback);
    }

    async getHistory(obj, message) {
        const input = message.options && typeof message.options === 'object' ? message.options : message;
        const id = message.id || input.id;
        if (!id) throw new Error('id is required');

        const end = input.end == null ? Date.now() + 5000000 : this.toTimestamp(input.end);
        const start = input.start == null ? undefined : this.toTimestamp(input.start);
        if (start !== undefined && start > end) throw new Error('start must be before end');

        const aggregate = input.aggregate || 'average';
        const requestedLimit = this.toPositiveInteger(input.limit, 2000, 2000);
        const limit = requestedLimit || 2000;
        const requestedCount = this.toPositiveInteger(
            input.count === true ? undefined : input.count,
            aggregate === 'none' || aggregate === 'onchange' ? limit : 500,
            2000
        );
        const count = requestedCount || (aggregate === 'none' || aggregate === 'onchange' ? limit : 500);
        const effectiveStart = start === undefined ? end - 86400000 : start;
        const returnNewestEntries = Boolean(input.returnNewestEntries) || (start === undefined && input.count != null);
        const options = {
            id,
            start: start === undefined && returnNewestEntries && (aggregate === 'none' || aggregate === 'onchange')
                ? undefined
                : effectiveStart,
            end,
            count,
            limit,
            step: this.toPositiveInteger(input.step, undefined),
            aggregate,
            ignoreNull: input.ignoreNull === true || input.ignoreNull === 'true'
                ? true
                : input.ignoreNull === 0 || input.ignoreNull === '0'
                    ? 0
                    : false,
            from: Boolean(input.from),
            q: Boolean(input.q),
            ack: Boolean(input.ack),
            addId: Boolean(input.addId),
            returnNewestEntries,
            sessionId: input.sessionId,
            percentile: Number.isFinite(Number(input.percentile)) ? Number(input.percentile) : undefined,
            quantile: Number.isFinite(Number(input.quantile)) ? Number(input.quantile) : undefined,
            integralUnit: Number.isFinite(Number(input.integralUnit)) ? Number(input.integralUnit) : undefined,
            integralInterpolation: input.integralInterpolation || undefined,
            removeBorderValues: Boolean(input.removeBorderValues)
        };

        const query = {};
        if (id !== '*') query.id = id;
        query.ts = { $lte: end };
        if (!(start === undefined && returnNewestEntries && (aggregate === 'none' || aggregate === 'onchange'))) {
            query.ts.$gte = effectiveStart;
        }

        let cursor = this.collection.find(query).sort({ ts: 1 });
        if (options.returnNewestEntries && start === undefined) {
            cursor = this.collection.find(query).sort({ ts: -1 }).limit(limit);
        } else if (aggregate === 'none' || aggregate === 'onchange') {
            cursor = cursor.limit(limit);
        }
        const rows = await cursor.toArray();
        if (options.returnNewestEntries && start === undefined) rows.reverse();
        const data = rows.map(({ id: rowId, ts, val, ack, q, from }) => ({
            id: rowId,
            ts,
            val,
            ack: Boolean(ack),
            q: Number.isFinite(q) ? q : 0,
            from: from || undefined
        }));
        sendResponse(this, obj, id, options, data, Date.now());
    }

    async getRawEntries(message) {
        if (!message || typeof message.id !== 'string' || !message.id) throw new Error('id is required');
        const query = { id: message.id };
        if (message.start != null || message.end != null) {
            query.ts = {};
            if (message.start != null) query.ts.$gte = this.toTimestamp(message.start);
            if (message.end != null) query.ts.$lte = this.toTimestamp(message.end);
        }
        const limit = this.toPositiveInteger(message.limit, 100, 5000) || 100;
        const offset = this.toPositiveInteger(message.offset, 0, Number.MAX_SAFE_INTEGER);
        const sort = message.sort === 'asc' ? 1 : -1;
        const total = message.noTotal ? undefined : await this.collection.countDocuments(query);
        const result = await this.collection.find(query)
            .sort({ ts: sort })
            .skip(offset)
            .limit(limit)
            .project({ _id: 0, id: 1, ts: 1, val: 1 })
            .toArray();
        return { result, total };
    }

    async setHistoryEnabled(message, enabled) {
        if (!message || typeof message.id !== 'string' || !message.id) throw new Error('id is required');
        const existing = await this.getForeignObjectAsync(message.id);
        if (!existing) throw new Error(`Datapoint not found: ${message.id}`);
        const current = existing.common?.custom?.[this.namespace] || {};
        const settings = {
            ...current,
            ...(enabled && message.options && typeof message.options === 'object' ? message.options : {}),
            enabled
        };
        await this.extendForeignObjectAsync(message.id, {
            common: { custom: { [this.namespace]: settings } }
        });
        return { success: true };
    }

    async getEnabledDatapoints() {
        const result = {};
        for (const id of this.enabledIds) {
            const obj = await this.getForeignObjectAsync(id);
            result[id] = obj?.common?.custom?.[this.namespace] || { enabled: true };
        }
        return result;
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
        const timestamp = typeof value === 'number'
            ? value
            : typeof value === 'string' && value.trim() !== '' && Number.isFinite(Number(value))
                ? Number(value)
                : new Date(value).getTime();
        if (!Number.isFinite(timestamp)) throw new Error('Invalid timestamp');
        return timestamp;
    }

    toPositiveInteger(value, fallback, maximum = Number.MAX_SAFE_INTEGER) {
        if (value == null || value === '') return fallback;
        const integer = Number(value);
        if (!Number.isInteger(integer) || integer < 0) throw new Error('Expected a non-negative integer');
        return Math.min(integer, maximum);
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
