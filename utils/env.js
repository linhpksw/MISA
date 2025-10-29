const dotenv = require('dotenv');

const envResult = dotenv.config();
const envFileVars = envResult.parsed || {};

function readEnv(key, fallback = undefined) {
    if (process.env[key] !== undefined) return process.env[key];
    if (envFileVars[key] !== undefined) return envFileVars[key];
    return fallback;
}

function ensureConfig(configs) {
    const missing = configs.filter(({ value }) => value === undefined || value === null || value === '').map(({ name }) => name);
    if (missing.length) {
        throw new Error(`Missing required configuration values: ${missing.join(', ')}. Update .env or context.json.`);
    }
}

function parseNumber(value, fallback) {
    if (value === undefined || value === null || value === '') return fallback;
    const num = Number(value);
    return Number.isFinite(num) ? num : fallback;
}

function parseList(value, fallback = []) {
    if (!value) return fallback;
    if (Array.isArray(value)) return value;
    return String(value)
        .split(',')
        .map((item) => item.trim())
        .filter((item) => item.length > 0);
}

module.exports = {
    readEnv,
    ensureConfig,
    parseNumber,
    parseList,
};
