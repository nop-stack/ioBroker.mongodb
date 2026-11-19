'use strict';

const utils = require('@iobroker/adapter-core');
const { MongoClient, ObjectId } = require('mongodb');
const { sendResponse } = require('@iobroker/aggregate');
const { cronMatches, normalizeJob, readFileRows, validatePipeline } = require('./lib/file-jobs');

class MongoDBHistory extends utils.Adapter {
    constructor(options = {}) {
        super({ ...options, name: 'mongodb' });
        this.client = null;
        this.db = null;
        this.collection = null;
        this.historyCollectionIsTimeSeries = false;
        this.customs = Object.create(null);
        this.lastValues = new Map();
        this.debounceTimers = new Map();
        this.lastLoggedAt = new Map();
        this.aggregationPipeline = [];
        this.aggregationRequested = false;
        this.aggregationCollection = null;
        this.aggregationCollectionIsTimeSeries = false;
        this.aggregationTimeField = null;
        this.supportsDocumentsStage = false;
        this.historyConfigLoaded = false;
        this.pendingHistoryObjectChanges = new Map();
        this.fileJobs = new Map();
        this.fileJobQueue = [];
        this.fileJobsInFlight = new Set();
        this.fileJobDebounceTimers = new Map();
        this.fileJobLastCronMinute = new Map();
        this.fileJobLastStateValues = new Map();
        this.fileJobCronTimer = null;
        this.fileJobsStopping = false;
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
            this.db = db;
            this.collection = db.collection(this.config.collection || 'history');
            this.aggregationCollection = String(this.config.aggregationCollection || 'history_mapped').trim();
            const historyInfo = await db.listCollections({ name: this.collection.collectionName }, { nameOnly: false }).next();
            this.historyCollectionIsTimeSeries = Boolean(historyInfo?.options?.timeseries);
            if (this.historyCollectionIsTimeSeries) {
                this.log.warn(`Configured history collection "${this.collection.collectionName}" is a time-series collection; the adapter will not create its regular { id: 1, ts: 1 } index`);
            }
            let buildInfo = {};
            try {
                buildInfo = await this.client.db('admin').command({ buildInfo: 1 });
            } catch (error) {
                this.log.warn(`Could not determine MongoDB server version; aggregation transformations will be disabled if configured: ${error.message}`);
            }
            const versionParts = Array.isArray(buildInfo.versionArray)
                ? buildInfo.versionArray
                : String(buildInfo.version || '0.0.0').split('.').map(part => Number.parseInt(part, 10) || 0);
            this.supportsDocumentsStage = versionParts[0] >= 6;
            const targetInfo = await db.listCollections({ name: this.aggregationCollection }, { nameOnly: false }).next();
            const timeSeriesOptions = targetInfo?.options?.timeseries;
            this.aggregationCollectionIsTimeSeries = Boolean(timeSeriesOptions);
            this.aggregationTimeField = typeof timeSeriesOptions?.timeField === 'string'
                ? timeSeriesOptions.timeField
                : null;
            if (this.aggregationCollectionIsTimeSeries) {
                this.log.info(`Mapped output collection "${this.aggregationCollection}" is a time-series collection (timeField: "${this.aggregationTimeField}")`);
            }
            this.configureAggregationPipeline();
            if (this.aggregationRequested && !this.supportsDocumentsStage) {
                this.log.warn(`MongoDB ${buildInfo.version || 'version unknown'} is older than 6.0 or could not be identified; aggregation transformations are disabled because they require the $documents stage. Values will not be written to the raw history collection as a fallback`);
                this.aggregationPipeline = [];
            }
            if (this.aggregationPipeline.length) {
                this.log.info(`Aggregation pipeline enabled on MongoDB ${buildInfo.version || versionParts.join('.')}; incoming documents will be transformed in memory and written directly to "${this.aggregationCollection}"`);
                this.log.warn(`With the aggregation pipeline enabled, new values are not stored as raw documents in "${this.collection.collectionName}"; getHistory and history update/delete commands cannot operate on the transformed output schema`);
            }
            if (!this.historyCollectionIsTimeSeries) {
                await this.collection.createIndex({ id: 1, ts: 1 });
            } else if (!this.aggregationRequested) {
                this.log.warn(`Raw history writes are disabled because "${this.collection.collectionName}" is time-series and raw records use a numeric ts; configure a MongoDB 6+ aggregation pipeline and use this as its output collection`);
            }
            await this.setStateAsync('info.connection', true, true);
            await this.loadHistoryConfiguration();
            await this.configureFileJobs();
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
                this.db = null;
                this.collection = null;
                this.historyCollectionIsTimeSeries = false;
            }
            this.setTimeout(() => this.onReady(), 15000);
        }
    }

    async onStateChange(id, state) {
        if (!state || !this.collection) return;
        await this.handleFileJobStateChange(id, state);
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
        if (this.aggregationRequested && !this.aggregationPipeline.length) return;
        if (!this.aggregationRequested && this.historyCollectionIsTimeSeries) return;
        try {
            if (this.aggregationRequested) {
                await this.writeMappedDocument(this.toDocument(id, state), this.customs[id].aliasId);
            } else {
                await this.collection.insertOne(this.toDocument(id, state));
            }
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
            await this.refreshStateFileJobs(id, obj);
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

    async writeMappedDocument(document, aliasId) {
        if (!this.aggregationPipeline.length || !this.db || !this.aggregationCollection) return;
        const sourceDocument = { _id: new ObjectId(), ...document };
        const pipeline = [{ $documents: [sourceDocument] }];
        const alias = typeof aliasId === 'string' ? aliasId.trim() : '';
        if (alias) pipeline.push({ $set: { id: { $literal: alias } } });
        pipeline.push(...this.aggregationPipeline);
        const documents = await this.db.aggregate(pipeline).toArray();
        if (documents.length) {
            // Insert each mapped event. This works for regular and time-series output collections.
            await this.db.collection(this.aggregationCollection).insertMany(documents);
        }
    }

    async configureFileJobs() {
        this.fileJobs.clear();
        const rows = Array.isArray(this.config.fileJobs) ? this.config.fileJobs.map(row => ({ ...row })) : [];
        const names = new Set();
        const nameCounts = new Map();
        let configChanged = false;
        for (const row of rows) {
            if (typeof row?.name === 'string') nameCounts.set(row.name, (nameCounts.get(row.name) || 0) + 1);
        }
        for (const row of rows) {
            if (!row?.enabled) continue;
            let job;
            try {
                job = normalizeJob(row);
                if (nameCounts.get(job.name) > 1) throw new Error(`duplicate job name "${job.name}"`);
                if (names.has(job.name)) throw new Error(`duplicate job name "${job.name}"`);
                names.add(job.name);
                const stat = await fs.promises.stat(job.file.path);
                if (!stat.isFile()) throw new Error('file.path does not point to a regular file');
                if (!this.db || !this.supportsDocumentsStage) throw new Error('file jobs require a connected MongoDB 6.0+ server');
                await this.db.aggregate([{ $documents: [] }, ...job.pipeline]).toArray();
                if (job.trigger.type === 'state') await this.validateStateTrigger(job);
                this.fileJobs.set(job.name, job);
            } catch (error) {
                const name = row?.name || '(unnamed)';
                this.log.warn(`File job "${name}" disabled: ${error.message}`);
                row.enabled = false;
                configChanged = true;
            }
        }
        if (configChanged) {
            this.config.fileJobs = rows;
            try {
                await this.extendForeignObjectAsync(`system.adapter.${this.namespace}`, { native: { fileJobs: rows } });
            } catch (error) {
                this.log.warn(`Could not save invalid file jobs as disabled: ${error.message}`);
            }
        }
        await this.subscribeFileJobStates();
        if ([...this.fileJobs.values()].some(job => job.trigger.type === 'cron')) this.scheduleFileJobCronCheck();
        this.log.info(`File jobs ready: ${this.fileJobs.size} enabled job(s)`);
    }

    async validateStateTrigger(job) {
        const object = await this.getForeignObjectAsync(job.trigger.id);
        if (!object) throw new Error(`state datapoint "${job.trigger.id}" does not exist`);
        const type = object.common?.type;
        if (!['string', 'number', 'boolean'].includes(type)) throw new Error(`state datapoint "${job.trigger.id}" has unsupported common.type "${type}"`);
        if (job.trigger.condition === 'becomesTrue' && type !== 'boolean') {
            throw new Error('becomesTrue requires a boolean datapoint');
        }
        if (job.trigger.condition === 'equals') {
            let value = job.trigger.value;
            if (type === 'string') value = String(value);
            else if (type === 'number') {
                value = Number(value);
                if (!Number.isFinite(value)) throw new Error('equals value must be a number for this datapoint');
            } else if (value === true || value === 'true' || value === 1 || value === '1') value = true;
            else if (value === false || value === 'false' || value === 0 || value === '0') value = false;
            else throw new Error('equals value must be true or false for this datapoint');
            job.trigger.value = value;
        }
        return object;
    }

    async subscribeFileJobStates() {
        const byId = new Map();
        for (const job of this.fileJobs.values()) {
            if (job.trigger.type !== 'state') continue;
            if (!byId.has(job.trigger.id)) byId.set(job.trigger.id, []);
            byId.get(job.trigger.id).push(job);
        }
        this.fileJobStatesById = byId;
        for (const id of byId.keys()) {
            await this.subscribeForeignStatesAsync(id);
            try {
                const state = await this.getForeignStateAsync(id);
                if (state) this.fileJobLastStateValues.set(id, state.val);
            } catch (error) {
                this.log.warn(`Could not read initial state for file-job trigger ${id}: ${error.message}`);
            }
        }
    }

    async refreshStateFileJobs(id, object) {
        if (!this.fileJobStatesById?.has(id)) return;
        const jobs = this.fileJobStatesById.get(id);
        for (const job of jobs) {
            try {
                await this.validateStateTrigger(job);
            } catch (error) {
                this.fileJobs.delete(job.name);
                this.log.warn(`File job "${job.name}" disabled after trigger object change: ${error.message}`);
            }
        }
        if (object === null) this.fileJobLastStateValues.delete(id);
    }

    async handleFileJobStateChange(id, state) {
        if (!state || !this.fileJobStatesById?.has(id)) return;
        const previous = this.fileJobLastStateValues.get(id);
        this.fileJobLastStateValues.set(id, state.val);
        for (const job of this.fileJobStatesById.get(id)) {
            if (!this.fileJobs.has(job.name)) continue;
            const trigger = job.trigger;
            let reason;
            if (trigger.condition === 'equals' && Object.is(state.val, trigger.value)) {
                reason = 'state equals';
            } else if (trigger.condition === 'becomesTrue' && previous === false && state.val === true) {
                reason = 'state false-to-true';
            } else if (trigger.condition === 'change' && !Object.is(previous, state.val)) {
                reason = 'state change';
            }
            if (reason && trigger.debounce > 0) {
                const oldTimer = this.fileJobDebounceTimers.get(job.name);
                if (oldTimer) this.clearTimeout(oldTimer);
                const timer = this.setTimeout(() => {
                    this.fileJobDebounceTimers.delete(job.name);
                    this.enqueueFileJob(job, reason);
                }, trigger.debounce);
                this.fileJobDebounceTimers.set(job.name, timer);
            } else if (reason) this.enqueueFileJob(job, reason);
        }
    }

    scheduleFileJobCronCheck() {
        if (this.fileJobsStopping || ![...this.fileJobs.values()].some(job => job.trigger.type === 'cron')) return;
        if (this.fileJobCronTimer) this.clearTimeout(this.fileJobCronTimer);
        const now = Date.now();
        const nextMinute = Math.floor(now / 60000) * 60000 + 60000;
        this.fileJobCronTimer = this.setTimeout(() => {
            const date = new Date();
            const minuteKey = `${date.getFullYear()}-${date.getMonth()}-${date.getDate()}-${date.getHours()}-${date.getMinutes()}`;
            for (const job of this.fileJobs.values()) {
                if (job.trigger.type === 'cron' && cronMatches(job.cron, date) && this.fileJobLastCronMinute.get(job.name) !== minuteKey) {
                    this.fileJobLastCronMinute.set(job.name, minuteKey);
                    this.enqueueFileJob(job, 'cron');
                }
            }
            this.scheduleFileJobCronCheck();
        }, Math.max(25, nextMinute - now + 25));
    }

    enqueueFileJob(job, reason) {
        if (this.fileJobsStopping || !this.fileJobs.has(job.name) || this.fileJobsInFlight.has(job.name)) return;
        this.fileJobsInFlight.add(job.name);
        this.fileJobQueue.push({ job, reason });
        if (!this.fileJobQueueRunning) this.fileJobQueuePromise = this.drainFileJobQueue();
    }

    async drainFileJobQueue() {
        if (this.fileJobQueueRunning) return;
        this.fileJobQueueRunning = true;
        try {
            while (this.fileJobQueue.length && !this.fileJobsStopping) {
                const { job, reason } = this.fileJobQueue.shift();
                try {
                    if (!this.fileJobs.has(job.name)) continue;
                    const summary = await this.runFileJob(job);
                    this.log.info(`File job "${job.name}" completed (${reason}): ${summary.rows} row(s), ${summary.written} document(s) written`);
                    if (job.trigger.type === 'state' && job.trigger.condition === 'equals' && job.trigger.resetAfterRun) {
                        await this.setForeignStateAsync(job.trigger.id, false, false);
                    }
                } catch (error) {
                    this.log.error(`File job "${job.name}" failed (${reason}): ${error.message}`);
                } finally {
                    this.fileJobsInFlight.delete(job.name);
                }
            }
        } finally {
            this.fileJobQueueRunning = false;
        }
    }

    async runFileJob(job) {
        const target = this.db.collection(job.target.collection);
        const batchSize = 750;
        const runId = new ObjectId().toHexString();
        const stagingName = `__iobroker_file_job_${runId}`;
        const originalIdField = `__iobroker_file_job_${runId}_originalId`;
        const hadOriginalIdField = `__iobroker_file_job_${runId}_hadOriginalId`;
        const staging = this.db.collection(stagingName);
        let inputBatch = [];
        let outputBatch = [];
        let rows = 0;
        let written = 0;
        const flushInput = async () => {
            if (!inputBatch.length) return;
            const documents = await this.db.aggregate([{ $documents: inputBatch }]).toArray();
            if (documents.length) await staging.insertMany(documents, { ordered: false });
            inputBatch = [];
        };
        const flushOutput = async () => {
            if (!outputBatch.length) return;
            if (job.target.upsertKey.length) {
                const operations = outputBatch.map(document => {
                    const filter = {};
                    for (const key of job.target.upsertKey) {
                        const value = this.getDocumentPath(document, key);
                        if (value === undefined || value === null) throw new Error(`pipeline output is missing upsertKey field "${key}"`);
                        filter[key] = value;
                    }
                    const { _id, ...fields } = document;
                    const update = { $set: fields };
                    if (_id !== undefined) update.$setOnInsert = { _id };
                    return { updateOne: { filter, update, upsert: true } };
                });
                const result = await target.bulkWrite(operations, { ordered: false });
                written += result.upsertedCount + result.modifiedCount;
            } else {
                const result = await target.insertMany(outputBatch, { ordered: false });
                written += result.insertedCount;
            }
            outputBatch = [];
        };

        try {
            for await (const row of readFileRows(job)) {
                const sourceHasId = Object.prototype.hasOwnProperty.call(row, '_id');
                inputBatch.push({
                    ...row,
                    _id: new ObjectId(),
                    [originalIdField]: sourceHasId ? row._id : null,
                    [hadOriginalIdField]: sourceHasId
                });
                rows++;
                if (inputBatch.length >= batchSize) await flushInput();
            }
            await flushInput();
            if (rows) {
                const pipeline = [
                    { $documents: [] },
                    { $unionWith: stagingName },
                    { $set: { _id: { $cond: [`$${hadOriginalIdField}`, `$${originalIdField}`, '$_id'] } } },
                    { $unset: [originalIdField, hadOriginalIdField] },
                    ...job.pipeline
                ];
                const cursor = this.db.aggregate(pipeline, { allowDiskUse: true });
                for await (const document of cursor) {
                    outputBatch.push(document);
                    if (outputBatch.length >= batchSize) await flushOutput();
                }
                await flushOutput();
            }
        } finally {
            try { await staging.drop(); } catch (error) {
                if (error.codeName !== 'NamespaceNotFound' && error.code !== 26) {
                    this.log.warn(`Could not remove temporary file-job collection ${stagingName}: ${error.message}`);
                }
            }
        }
        return { rows, written };
    }

    getDocumentPath(document, path) {
        return String(path).split('.').reduce((value, key) => value == null ? undefined : value[key], document);
    }

    async dryRunFileJob(message) {
        try {
            const row = (Array.isArray(this.config.fileJobs) ? this.config.fileJobs : [])
                .find(entry => entry?.name === message.name);
            let pipeline = message.pipeline ?? row?.pipeline;
            if (pipeline === undefined) return { success: false, error: `Job not found: ${message.name || '(empty name)'}` };
            if (typeof pipeline === 'string') pipeline = JSON.parse(pipeline || '[]');
            validatePipeline(pipeline);
            if (!this.db || !this.supportsDocumentsStage) throw new Error('File jobs require MongoDB 6.0+ and an active connection');
            await this.db.aggregate([{ $documents: [] }, ...pipeline]).toArray();
            return { success: true, message: `Pipeline for "${message.name || row?.name || 'file job'}" is valid` };
        } catch (error) {
            return { success: false, error: error.message };
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
                case 'dryRunFileJob':
                    result = await this.dryRunFileJob(obj.message || {});
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
                    const document = this.toDocument(entry.id, { ...state, ts: Number(state.ts) });
                    if (this.aggregationRequested && !this.aggregationPipeline.length) {
                        errors.push(`${entry.id}: aggregation pipeline is configured but unavailable; value was not stored`);
                        continue;
                    } else if (this.aggregationRequested) {
                        await this.writeMappedDocument(document, this.customs[entry.id]?.aliasId);
                    } else if (this.historyCollectionIsTimeSeries) {
                        errors.push(`${entry.id}: raw history writes require a regular collection; configure a MongoDB 6+ aggregation pipeline to write to a time-series collection`);
                        continue;
                    } else {
                        await this.collection.insertOne(document);
                    }
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
        this.aggregationRequested = false;
        const source = this.config.aggregationPipeline;
        if (source == null || source === '') return;
        this.aggregationRequested = true;
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
        this.aggregationRequested = pipeline.length > 0;

        const issues = this.validateAggregationPipeline(pipeline, this.aggregationTimeField);
        if (issues.length) {
            for (const issue of issues) this.log.warn(`Aggregation pipeline: ${issue}`);
        } else {
            this.aggregationPipeline = pipeline;
            this.log.info(`Aggregation pipeline is valid (${pipeline.length} stage(s)); source fields: _id, id, ts, val, ack, q, from`);
        }
    }

    validateAggregationPipeline(pipeline, requiredTimeField = null) {
        const issues = [];
        const fields = new Set(['_id', 'id', 'ts', 'val', 'ack', 'q', 'from']);
        const dateFields = new Set();
        const isDateExpression = expression => Boolean(
            expression && typeof expression === 'object' && !Array.isArray(expression) &&
            Object.keys(expression).length === 1 && Object.prototype.hasOwnProperty.call(expression, '$toDate')
        );
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
                const nextDateFields = new Set();
                for (const [field, expression] of Object.entries(specification)) {
                    if (field === '_id' && expression !== 1 && expression !== true) {
                        issues.push(`stage ${index + 1} must preserve the original _id with "_id": 1`);
                    }
                    if (expression === 1 || expression === true) {
                        if (hasField(field)) nextFields.add(field);
                        else issues.push(`stage ${index + 1} includes unknown field "${field}"`);
                        if (dateFields.has(field)) nextDateFields.add(field);
                    } else if (expression !== 0 && expression !== false) {
                        nextFields.add(field);
                        if (isDateExpression(expression)) nextDateFields.add(field);
                    } else if (field === '_id' && expression !== 0 && fields.has('_id')) {
                        nextFields.add('_id');
                    }
                }
                if (!Object.prototype.hasOwnProperty.call(specification, '_id') || specification._id !== 0) nextFields.add('_id');
                fields.clear();
                for (const field of nextFields) fields.add(field);
                dateFields.clear();
                for (const field of nextDateFields) dateFields.add(field);
            } else if ((operator === '$set' || operator === '$addFields') && specification && typeof specification === 'object') {
                if (Object.prototype.hasOwnProperty.call(specification, '_id')) {
                    issues.push(`stage ${index + 1} must not overwrite _id`);
                }
                for (const [field, expression] of Object.entries(specification)) {
                    fields.add(field);
                    if (isDateExpression(expression)) dateFields.add(field);
                    else dateFields.delete(field);
                }
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
                    dateFields.delete(field);
                }
            }
        }
        if (!fields.has('_id')) issues.push('pipeline output removes _id; _id is required for mapped documents');
        if (requiredTimeField && !fields.has(requiredTimeField)) {
            issues.push(`time-series target requires its timeField "${requiredTimeField}" in the pipeline output`);
        } else if (requiredTimeField && !dateFields.has(requiredTimeField)) {
            issues.push(`time-series target requires "${requiredTimeField}" to be a BSON Date; convert it with { "$toDate": ... }`);
        }
        return [...new Set(issues)];
    }

    async onUnload(callback) {
        this.fileJobsStopping = true;
        for (const timer of this.debounceTimers.values()) this.clearTimeout(timer);
        this.debounceTimers.clear();
        for (const timer of this.fileJobDebounceTimers.values()) this.clearTimeout(timer);
        this.fileJobDebounceTimers.clear();
        if (this.fileJobCronTimer) this.clearTimeout(this.fileJobCronTimer);
        this.fileJobQueue.length = 0;
        try {
            if (this.fileJobQueuePromise) await this.fileJobQueuePromise;
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
