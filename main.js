'use strict';

const utils = require('@iobroker/adapter-core');
const { MongoClient } = require('mongodb');
const { sendResponse } = require('@iobroker/aggregate');

class MongoDBHistory extends utils.Adapter {
    constructor(options = {}) {
        super({ ...options, name: 'mongodb' });
        this.client = null;
        this.collection = null;
        this.customs = Object.create(null);
        this.lastValues = new Map();
        this.debounceTimers = new Map();
        this.lastLoggedAt = new Map();
        this.aggregationPipeline = [];
        this.aggregationCollection = null;
        this.historyConfigLoaded = false;
        this.pendingHistoryObjectChanges = new Map();
        this.on('ready', this.onReady.bind(this));
        this.on('stateChange', this.onStateChange.bind(this));
        this.on('objectChange', this.onObjectChange.bind(this));
        this.on('message', this.onMessage.bind(this));
        this.on('unload', this.onUnload.bind(this));
    }

    async onReady() {
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
        this.configureAggregationPipeline();

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
            this.aggregationCollection = String(this.config.aggregationCollection || 'history_mapped').trim();
            if (this.aggregationPipeline.length && this.aggregationCollection === this.collection.collectionName) {
                this.log.error('Mapped output collection must differ from the history collection; mapped output is disabled');
                this.aggregationPipeline = [];
            }
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
        if (!state || !this.collection) return;
        const config = this.customs[id];
        if (!config) return;
        const debounce = this.getNonNegativeNumber(config.debounceTime ?? config.debounce, 0);
        if (debounce > 0) {
            const oldTimer = this.debounceTimers.get(id);
            if (oldTimer) this.clearTimeout(oldTimer);
            this.debounceTimers.set(id, this.setTimeout(() => {
                this.debounceTimers.delete(id);
                this.writeStateIfAllowed(id, state);
            }, debounce));
            return;
        }
        await this.writeStateIfAllowed(id, state);
    }

    async writeStateIfAllowed(id, state) {
        const config = this.customs[id];
        if (!config || !this.collection) return;
        if (config.ignoreZero === true && (state.val === 0 || state.val === null)) return;

        const now = Date.now();
        const lastValue = this.lastValues.get(id);
        const lastLoggedAt = this.lastLoggedAt.get(id);
        const blockTime = this.getNonNegativeNumber(config.blockTime, 0);
        if (lastLoggedAt && blockTime > 0 && now - lastLoggedAt < blockTime) return;

        if (config.changesOnly !== false && lastValue !== undefined) {
            if (Object.is(lastValue, state.val)) {
                const relogSeconds = this.getNonNegativeNumber(config.changesRelogInterval, 0);
                if (!relogSeconds || !lastLoggedAt || now - lastLoggedAt < relogSeconds * 1000) return;
            } else {
                const minDelta = this.getNonNegativeNumber(config.changesMinDelta, 0);
                if (minDelta > 0 && typeof lastValue === 'number' && typeof state.val === 'number'
                    && Number.isFinite(lastValue) && Number.isFinite(state.val)
                    && Math.abs(state.val - lastValue) < minDelta) return;
            }
        }
        await this.writeState(id, state);
    }

    async writeState(id, state) {
        if (!this.collection || !this.customs[id]) return;
        try {
            const result = await this.collection.insertOne(this.toDocument(id, state));
            await this.writeMappedDocument(result.insertedId, this.customs[id].aliasId);
            this.lastValues.set(id, state.val);
            this.lastLoggedAt.set(id, Date.now());
        } catch (error) {
            this.log.warn(`Could not store history for ${id}: ${error.message}`);
        }
    }

    async onObjectChange(id, obj) {
        if (!this.collection || typeof id !== 'string') return;
        if (!this.historyConfigLoaded) {
            this.pendingHistoryObjectChanges.set(id, obj);
            return;
        }
        try {
            await this.applyHistoryObjectChange(id, obj);
        } catch (error) {
            this.log.warn(`Could not update history subscription for ${id}: ${error.message}`);
        }
    }

    async applyHistoryObjectChange(id, obj) {
        const custom = obj?.common?.custom?.[this.namespace];
        if (custom?.enabled === true) {
            const alreadyEnabled = Boolean(this.customs[id]);
            this.customs[id] = custom;
            if (!alreadyEnabled) await this.subscribeForeignStatesAsync(id);
        } else {
            const wasEnabled = Boolean(this.customs[id]);
            delete this.customs[id];
            const timer = this.debounceTimers.get(id);
            if (timer) this.clearTimeout(timer);
            this.debounceTimers.delete(id);
            if (wasEnabled) this.unsubscribeForeignStates(id);
        }
    }

    async loadHistoryConfiguration() {
        this.historyConfigLoaded = false;
        try {
            await this.subscribeForeignObjectsAsync('*');
            const view = await this.getObjectViewAsync('system', 'custom', {});
            const customs = Object.create(null);
            for (const row of view?.rows || []) {
                const settings = row.value?.[this.namespace];
                if (settings && typeof settings === 'object' && settings.enabled === true) customs[row.id] = settings;
            }
            for (const [id, obj] of this.pendingHistoryObjectChanges) {
                const settings = obj?.common?.custom?.[this.namespace];
                if (settings?.enabled === true) customs[id] = settings;
                else delete customs[id];
            }
            this.pendingHistoryObjectChanges.clear();
            this.customs = customs;
            this.historyConfigLoaded = true;
            const ids = Object.keys(customs);
            for (const id of ids) await this.subscribeForeignStatesAsync(id);
            this.log.info(`History logging enabled for ${ids.length} datapoint(s)`);
        } catch (error) {
            this.historyConfigLoaded = true;
            this.log.warn(`Could not load datapoint history settings: ${error.message}`);
            for (const [id, obj] of this.pendingHistoryObjectChanges) {
                try {
                    await this.applyHistoryObjectChange(id, obj);
                } catch (subscriptionError) {
                    this.log.warn(`Could not update history subscription for ${id}: ${subscriptionError.message}`);
                }
            }
            this.pendingHistoryObjectChanges.clear();
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

    async writeMappedDocument(documentId, aliasId) {
        if (!this.aggregationPipeline.length || !this.collection || !this.aggregationCollection) return;
        try {
            const pipeline = [{ $match: { _id: documentId } }];
            const alias = typeof aliasId === 'string' ? aliasId.trim() : '';
            if (alias) pipeline.push({ $set: { id: { $literal: alias } } });
            pipeline.push(
                ...this.aggregationPipeline,
                {
                    $merge: {
                        into: this.aggregationCollection,
                        on: '_id',
                        whenMatched: 'replace',
                        whenNotMatched: 'insert'
                    }
                }
            );
            await this.collection.aggregate(pipeline).toArray();
        } catch (error) {
            this.log.warn(`Could not write mapped MongoDB document ${documentId}: ${error.message}`);
        }
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
        for (const id of Object.keys(this.customs)) {
            const obj = await this.getForeignObjectAsync(id);
            result[id] = obj?.common?.custom?.[this.namespace] || this.customs[id];
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
                    const result = await this.collection.insertOne(this.toDocument(entry.id, { ...state, ts: Number(state.ts) }));
                    await this.writeMappedDocument(result.insertedId, this.customs[entry.id]?.aliasId);
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

    getNonNegativeNumber(value, fallback) {
        if (value == null || value === '') return fallback;
        const number = Number(value);
        return Number.isFinite(number) && number >= 0 ? number : fallback;
    }

    configureAggregationPipeline() {
        this.aggregationPipeline = [];
        const source = this.config.aggregationPipeline;
        if (source == null || source === '') return;
        let pipeline;
        try {
            pipeline = typeof source === 'string' ? JSON.parse(source) : source;
        } catch (error) {
            this.log.warn(`Aggregation pipeline is not valid JSON: ${error.message}`);
            return;
        }
        if (!Array.isArray(pipeline)) {
            this.log.warn('Aggregation pipeline must be a JSON array of stages');
            return;
        }

        const issues = this.validateAggregationPipeline(pipeline);
        if (issues.length) {
            for (const issue of issues) this.log.warn(`Aggregation pipeline: ${issue}`);
        } else {
            this.aggregationPipeline = pipeline;
            this.log.info(`Aggregation pipeline is valid (${pipeline.length} stage(s)); source fields: _id, id, ts, val, ack, q, from`);
        }
    }

    validateAggregationPipeline(pipeline) {
        const issues = [];
        const fields = new Set(['_id', 'id', 'ts', 'val', 'ack', 'q', 'from']);
        // These stages keep the pipeline one-to-one: a source document can produce at most one mapped document.
        const supportedStages = new Set(['$addFields', '$set', '$match', '$project', '$unset']);
        const forbiddenOperators = new Set(['$out', '$merge', '$function', '$accumulator', '$where']);
        const hasField = path => {
            const fieldPath = String(path);
            return fields.has(fieldPath) || fieldPath.startsWith('val.');
        };
        const checkExpression = (value, stageIndex, path = '') => {
            if (typeof value === 'string') {
                if (value.startsWith('$') && !value.startsWith('$$')) {
                    const field = value.slice(1).split('.')[0].split('[')[0];
                    if (field && !hasField(value.slice(1))) {
                        issues.push(`stage ${stageIndex + 1} references unknown field "${value.slice(1)}"${path ? ` at ${path}` : ''}`);
                    }
                }
                return;
            }
            if (Array.isArray(value)) {
                value.forEach((entry, index) => checkExpression(entry, stageIndex, `${path}[${index}]`));
                return;
            }
            if (!value || typeof value !== 'object') return;
            for (const [key, entry] of Object.entries(value)) {
                if (forbiddenOperators.has(key)) {
                    issues.push(`stage ${stageIndex + 1} uses prohibited operator "${key}"`);
                    continue;
                }
                if (key === '$literal') continue;
                checkExpression(entry, stageIndex, path ? `${path}.${key}` : key);
            }
        };

        for (let index = 0; index < pipeline.length; index++) {
            const stage = pipeline[index];
            if (!stage || typeof stage !== 'object' || Array.isArray(stage) || Object.keys(stage).length !== 1) {
                issues.push(`stage ${index + 1} must be an object with exactly one operator`);
                continue;
            }
            const [operator, specification] = Object.entries(stage)[0];
            if (!operator.startsWith('$')) issues.push(`stage ${index + 1} has invalid operator "${operator}"`);
            else if (forbiddenOperators.has(operator)) issues.push(`stage ${index + 1} uses prohibited operator "${operator}"`);
            else if (!supportedStages.has(operator)) {
                issues.push(`stage ${index + 1} uses unsupported operator "${operator}"; mapped pipelines must preserve one output document per input`);
            }

            if (operator === '$match' && specification && typeof specification === 'object') {
                const checkMatchKeys = (query, prefix = '') => {
                    if (!query || typeof query !== 'object' || Array.isArray(query)) return;
                    for (const [key, value] of Object.entries(query)) {
                        if (key.startsWith('$')) {
                            if (forbiddenOperators.has(key)) {
                                issues.push(`stage ${index + 1} uses prohibited operator "${key}"`);
                                continue;
                            }
                            if (key === '$and' || key === '$or' || key === '$nor') checkMatchKeys(value, prefix);
                            else checkExpression(value, index, `${prefix}${key}`);
                            continue;
                        }
                        const fieldPath = prefix ? `${prefix}.${key}` : key;
                        if (!hasField(fieldPath)) issues.push(`stage ${index + 1} matches unknown field "${fieldPath}"`);
                        if (value && typeof value === 'object' && !Array.isArray(value)) checkMatchKeys(value, fieldPath);
                        else checkExpression(value, index, fieldPath);
                    }
                };
                checkMatchKeys(specification);
            } else if (operator === '$sort' && specification && typeof specification === 'object') {
                for (const field of Object.keys(specification)) {
                    if (!hasField(field)) issues.push(`stage ${index + 1} sorts by unknown field "${field}"`);
                }
            } else {
                checkExpression(specification, index, operator);
            }

            if (operator === '$project' && specification && typeof specification === 'object') {
                const nextFields = new Set();
                for (const [field, expression] of Object.entries(specification)) {
                    if (field === '_id' && expression !== 1 && expression !== true) {
                        issues.push(`stage ${index + 1} must preserve the original _id with "_id": 1`);
                    }
                    if (expression === 1 || expression === true) {
                        if (hasField(field)) nextFields.add(field);
                        else issues.push(`stage ${index + 1} includes unknown field "${field}"`);
                    } else if (expression !== 0 && expression !== false) {
                        nextFields.add(field);
                    } else if (field === '_id' && expression !== 0 && fields.has('_id')) {
                        nextFields.add('_id');
                    }
                }
                if (!Object.prototype.hasOwnProperty.call(specification, '_id') || specification._id !== 0) nextFields.add('_id');
                fields.clear();
                for (const field of nextFields) fields.add(field);
            } else if ((operator === '$set' || operator === '$addFields') && specification && typeof specification === 'object') {
                if (Object.prototype.hasOwnProperty.call(specification, '_id')) {
                    issues.push(`stage ${index + 1} must not overwrite _id`);
                }
                for (const field of Object.keys(specification)) fields.add(field);
            } else if (operator === '$unset') {
                const unsetFields = Array.isArray(specification) ? specification : [specification];
                if (!unsetFields.every(field => typeof field === 'string')) {
                    issues.push(`stage ${index + 1} $unset must contain a field name or an array of field names`);
                }
                for (const field of unsetFields) {
                    if (typeof field !== 'string') continue;
                    if (field === '_id') issues.push(`stage ${index + 1} must not remove _id`);
                    if (!hasField(field)) issues.push(`stage ${index + 1} removes unknown field "${field}"`);
                    fields.delete(field);
                }
            }
        }
        if (!fields.has('_id')) issues.push('pipeline output removes _id; _id is required for mapped documents');
        return [...new Set(issues)];
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
