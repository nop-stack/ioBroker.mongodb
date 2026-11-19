'use strict';

const fs = require('node:fs');
const path = require('node:path');
const readline = require('node:readline');

const SLUG = /^[a-z0-9_-]{1,40}$/;
const FORMATS = new Set(['csv', 'json', 'jsonl']);
const CONDITIONS = new Set(['equals', 'becomesTrue', 'change']);

function parseCronField(source, min, max, fieldName) {
    const values = new Set();
    for (const part of source.split(',')) {
        if (!part) throw new Error(`Invalid ${fieldName} field`);
        const slashParts = part.split('/');
        if (slashParts.length > 2 || slashParts.some(value => !value)) throw new Error(`Invalid ${fieldName} field`);
        const [rangeText, stepText] = slashParts;
        const step = stepText === undefined ? 1 : Number(stepText);
        if (!Number.isInteger(step) || step < 1) throw new Error(`Invalid ${fieldName} step`);
        let start;
        let end;
        if (rangeText === '*') {
            start = min;
            end = max;
        } else if (rangeText.includes('-')) {
            const pair = rangeText.split('-').map(Number);
            if (pair.length !== 2) throw new Error(`Invalid ${fieldName} range`);
            [start, end] = pair;
        } else {
            start = Number(rangeText);
            end = stepText === undefined ? start : max;
        }
        if (!Number.isInteger(start) || !Number.isInteger(end) || start < min || end > max || start > end) {
            throw new Error(`Invalid ${fieldName} value`);
        }
        for (let value = start; value <= end; value += step) values.add(value);
    }
    if (!values.size) throw new Error(`Empty ${fieldName} field`);
    return values;
}

function parseCron(expression) {
    const fields = String(expression || '').trim().split(/\s+/);
    if (fields.length !== 5 && fields.length !== 6) throw new Error('Cron expression must have 5 or 6 fields');
    const hasSeconds = fields.length === 6;
    const offset = hasSeconds ? 1 : 0;
    const seconds = hasSeconds ? parseCronField(fields[0], 0, 59, 'seconds') : new Set([0]);
    if (hasSeconds && (seconds.size !== 1 || !seconds.has(0))) {
        throw new Error('6-field cron expressions must use second 0 to keep the minimum interval at one minute');
    }
    return {
        seconds,
        minutes: parseCronField(fields[offset], 0, 59, 'minutes'),
        hours: parseCronField(fields[offset + 1], 0, 23, 'hours'),
        days: parseCronField(fields[offset + 2], 1, 31, 'days'),
        months: parseCronField(fields[offset + 3], 1, 12, 'months'),
        weekdays: parseCronField(fields[offset + 4], 0, 7, 'weekdays'),
        daysWildcard: fields[offset + 2] === '*',
        weekdaysWildcard: fields[offset + 4] === '*'
    };
}

function cronMatches(cron, date) {
    const weekday = date.getDay();
    const dayMatches = cron.days.has(date.getDate());
    const weekdayMatches = cron.weekdays.has(weekday) || (weekday === 0 && cron.weekdays.has(7));
    const calendarMatches = cron.daysWildcard ? weekdayMatches : cron.weekdaysWildcard ? dayMatches : dayMatches || weekdayMatches;
    return cron.seconds.has(date.getSeconds()) && cron.minutes.has(date.getMinutes()) &&
        cron.hours.has(date.getHours()) && calendarMatches && cron.months.has(date.getMonth() + 1);
}

function validateSlug(name) {
    if (typeof name !== 'string' || !SLUG.test(name)) throw new Error('name must be a unique slug with 1 to 40 lowercase letters, digits, _ or -');
}

function validatePipeline(pipeline) {
    if (!Array.isArray(pipeline)) throw new Error('pipeline must be an array of stages');
    const check = value => {
        if (!value || typeof value !== 'object') return;
        if (Array.isArray(value)) return value.forEach(check);
        for (const [key, child] of Object.entries(value)) {
            if (key === '$out' || key === '$merge') throw new Error(`pipeline operator ${key} is prohibited`);
            if (key === '$documents') throw new Error('$documents is added by the adapter and cannot be used inside a job pipeline');
            check(child);
        }
    };
    for (const stage of pipeline) {
        if (!stage || typeof stage !== 'object' || Array.isArray(stage) || Object.keys(stage).length !== 1) {
            throw new Error('each pipeline stage must be an object with exactly one operator');
        }
        check(stage);
    }
}

function normalizeJob(row) {
    if (!row || typeof row !== 'object') throw new Error('job must be an object');
    const job = {
        name: String(row.name || '').trim(),
        enabled: row.enabled === true,
        file: { path: String(row.path || '').trim(), format: String(row.format || '').toLowerCase() },
        trigger: { type: String(row.triggerType || '').toLowerCase() },
        pipeline: row.pipeline,
        target: {
            collection: String(row.targetCollection || '').trim(),
            upsertKey: String(row.upsertKey || '').split(',').map(key => key.trim()).filter(Boolean)
        }
    };
    validateSlug(job.name);
    if (!path.isAbsolute(job.file.path) || job.file.path.split(/[\\/]/).includes('..')) {
        throw new Error('file.path must be absolute and must not contain ..');
    }
    if (!FORMATS.has(job.file.format)) throw new Error('file.format must be csv, json or jsonl');
    if (!job.target.collection) throw new Error('target.collection is required');
    if (job.target.collection.includes('\0') || job.target.collection.includes('$')) throw new Error('target.collection contains an invalid character');
    if (new Set(job.target.upsertKey).size !== job.target.upsertKey.length || job.target.upsertKey.some(key =>
        key.startsWith('$') || key.split('.').some(part => !part || part.startsWith('$'))
    )) throw new Error('upsertKey must contain unique field paths without empty or $-prefixed segments');
    if (typeof row.pipeline === 'string') {
        try { job.pipeline = JSON.parse(row.pipeline || '[]'); } catch (error) { throw new Error(`pipeline JSON: ${error.message}`); }
    }
    validatePipeline(job.pipeline);

    if (job.trigger.type === 'cron') {
        job.trigger.expr = String(row.cronExpr || '').trim();
        job.cron = parseCron(job.trigger.expr);
    } else if (job.trigger.type === 'state') {
        job.trigger.id = String(row.stateId || '').trim();
        job.trigger.condition = String(row.condition || '').trim();
        if (!job.trigger.id) throw new Error('state trigger id is required');
        if (!CONDITIONS.has(job.trigger.condition)) throw new Error('state trigger condition must be equals, becomesTrue or change');
        const defaultDebounce = job.trigger.condition === 'change' ? 60000 : 0;
        job.trigger.debounce = row.debounce === '' || row.debounce === undefined || row.debounce === null
            ? defaultDebounce
            : Number.isFinite(Number(row.debounce)) && Number(row.debounce) >= 0 ? Number(row.debounce) : defaultDebounce;
        job.trigger.resetAfterRun = row.resetAfterRun === true;
        if (job.trigger.condition === 'equals') {
            if (row.value === undefined || row.value === null) throw new Error('equals trigger requires value');
            job.trigger.value = row.value;
        } else if (row.value !== '' && row.value !== undefined && row.value !== null) {
            throw new Error(`${job.trigger.condition} trigger does not use value`);
        }
        if (job.trigger.resetAfterRun && job.trigger.condition !== 'equals') throw new Error('resetAfterRun is only valid for equals triggers');
    } else {
        throw new Error('trigger.type must be cron or state');
    }
    return job;
}

async function* readCsv(filePath) {
    const stream = fs.createReadStream(filePath, { encoding: 'utf8' });
    let field = '';
    let row = [];
    let inQuotes = false;
    let firstRow = true;
    const header = [];
    let pendingQuote = false;
    let skipLf = false;
    const pendingRows = [];
    for await (const chunk of stream) {
        for (const char of chunk) {
            if (skipLf && char === '\n') { skipLf = false; continue; }
            skipLf = false;
            if (inQuotes) {
                if (pendingQuote) {
                    if (char === '"') { field += '"'; pendingQuote = false; }
                    else { inQuotes = false; pendingQuote = false; consumeOutside(char); }
                } else if (char === '"') pendingQuote = true;
                else field += char;
            } else if (char === '"' && field === '') inQuotes = true;
            else consumeOutside(char);
            while (pendingRows.length) yield pendingRows.shift();
        }
    }
    if (pendingQuote) { inQuotes = false; pendingQuote = false; }
    if (inQuotes) throw new Error('CSV contains an unterminated quoted field');
    if (field !== '' || row.length) {
        row.push(field);
        if (firstRow) throw new Error('CSV file must contain a header and at least one data row');
        if (row.some(value => value !== '')) yield Object.fromEntries(header.map((key, i) => [key, row[i] ?? '']));
    }
    if (firstRow) throw new Error('CSV file is empty');

    function consumeOutside(char) {
        if (char === ',') { row.push(field); field = ''; }
        else if (char === '\n' || char === '\r') {
            if (char === '\r') skipLf = true;
            row.push(field); field = '';
            if (firstRow) {
                header.push(...row.map((value, index) => (index === 0 ? value.replace(/^\uFEFF/, '') : value).trim()));
                firstRow = false;
            }
            else if (row.some(value => value !== '')) {
                // Yielding from a nested helper is not valid in an async generator; rows are queued below.
                pendingRows.push(Object.fromEntries(header.map((key, i) => [key, row[i] ?? ''])));
            }
            row = [];
        } else field += char;
    }
}

async function* readJsonLines(filePath) {
    const lines = readline.createInterface({ input: fs.createReadStream(filePath, { encoding: 'utf8' }), crlfDelay: Infinity });
    let lineNumber = 0;
    for await (const line of lines) {
        lineNumber++;
        if (!line.trim()) continue;
        let value;
        try { value = JSON.parse(line); } catch (error) { throw new Error(`invalid JSONL at line ${lineNumber}: ${error.message}`); }
        if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(`JSONL line ${lineNumber} must contain an object`);
        yield value;
    }
}

async function* readJson(filePath) {
    const stream = fs.createReadStream(filePath, { encoding: 'utf8' });
    let started = false;
    let inString = false;
    let escaped = false;
    let depth = 0;
    let buffer = '';
    let seenArray = false;
    let seenObject = false;
    let finished = false;
    let arrayNeedsItem = true;
    let afterArrayComma = false;
    for await (const chunk of stream) {
        for (const char of chunk) {
            if (!started) {
                if (/\s/.test(char)) continue;
                if (char === '{') {
                    started = true;
                    seenObject = true;
                    buffer = '{';
                    depth = 1;
                    continue;
                }
                if (char !== '[') throw new Error('json format must be a top-level object or array of objects');
                started = true;
                seenArray = true;
                continue;
            }
            if (finished) {
                if (!/\s/.test(char)) throw new Error('unexpected content after JSON array');
                continue;
            }
            if (depth === 0 && !inString) {
                if (/\s/.test(char)) continue;
                if (char === ']') {
                    if (afterArrayComma) throw new Error('JSON array has a trailing comma');
                    finished = true;
                    continue;
                }
                if (char === ',') {
                    if (arrayNeedsItem) throw new Error('JSON array contains an unexpected comma');
                    arrayNeedsItem = true;
                    afterArrayComma = true;
                    continue;
                }
                if (char !== '{') throw new Error('json array entries must be objects');
                if (!arrayNeedsItem) throw new Error('JSON array entries must be separated by commas');
                arrayNeedsItem = false;
                afterArrayComma = false;
                buffer = '{'; depth = 1; continue;
            }
            buffer += char;
            if (inString) {
                if (escaped) escaped = false;
                else if (char === '\\') escaped = true;
                else if (char === '"') inString = false;
                continue;
            }
            if (char === '"') inString = true;
            else if (char === '{' || char === '[') depth++;
            else if (char === '}' || char === ']') depth--;
            if (depth === 0) {
                let value;
                try { value = JSON.parse(buffer); } catch (error) { throw new Error(`invalid JSON object: ${error.message}`); }
                yield value;
                buffer = '';
                if (seenObject) finished = true;
            }
        }
    }
    if ((!seenArray && !seenObject) || !finished || depth !== 0 || buffer) throw new Error('JSON input is incomplete');
}

async function* readFileRows(job) {
    if (job.file.format === 'csv') yield* readCsv(job.file.path);
    else if (job.file.format === 'jsonl') yield* readJsonLines(job.file.path);
    else yield* readJson(job.file.path);
}

module.exports = { cronMatches, normalizeJob, parseCron, readFileRows, validatePipeline };
