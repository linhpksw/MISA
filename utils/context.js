const fs = require('fs');
const path = require('path');

let cachedContext = null;

function loadContext() {
    if (cachedContext) return cachedContext;

    const contextPath = path.join(__dirname, '..', 'context.json');
    let raw;
    try {
        raw = fs.readFileSync(contextPath, 'utf8');
    } catch (err) {
        throw new Error(`Unable to read context.json at ${contextPath}: ${err.message}`);
    }

    let parsed;
    try {
        parsed = JSON.parse(raw);
    } catch (err) {
        throw new Error(`context.json is not valid JSON: ${err.message}`);
    }

    cachedContext = { contextObj: parsed, contextStr: JSON.stringify(parsed) };
    return cachedContext;
}

module.exports = { loadContext };
