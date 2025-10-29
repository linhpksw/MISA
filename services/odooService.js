const axios = require('axios');

const { readEnv, ensureConfig, parseNumber, parseList } = require('../utils/env');
const { mapOdooRelation } = require('../utils/normalizer');

function buildOdooHeaders(cookie, baseUrl) {
    return {
        'Content-Type': 'application/json',
        Accept: 'application/json',
        ...(cookie ? { Cookie: cookie } : {}),
        Referer: `${baseUrl}/web`,
    };
}

function buildOdooPayload(options) {
    const {
        requestId,
        limit,
        offset,
        order,
        domain,
        context,
    } = options;

    const specification = {
        display_name: {},
        customer_code: {},
        complete_name: {},
        phone: {},
        mobile: {},
        email: {},
        user_id: { fields: { display_name: {} } },
        city: {},
        vat: {},
        company_id: { fields: { display_name: {} } },
        is_company: {},
        active: {},
    };

    return {
        id: requestId,
        jsonrpc: '2.0',
        method: 'call',
        params: {
            model: 'res.partner',
            method: 'web_search_read',
            args: [],
            kwargs: {
                specification,
                offset,
                order,
                limit,
                context,
                count_limit: options.countLimit,
                domain,
            },
        },
    };
}

function normaliseOdooRecords(records, onlyActive) {
    const normalised = records.map((record) => {
        const accountManager = mapOdooRelation(record.user_id);
        const company = mapOdooRelation(record.company_id);
        return {
            id: record.id ?? null,
            displayName: record.display_name ?? null,
            customerCode: record.customer_code ?? null,
            completeName: record.complete_name ?? null,
            phone: record.phone ?? record.mobile ?? null,
            email: record.email ?? null,
            city: record.city ?? null,
            taxCode: record.vat ?? null,
            accountManager,
            company,
            isCompany: Boolean(record.is_company),
            active: Boolean(record.active),
        };
    });

    return onlyActive ? normalised.filter((item) => item.active !== false) : normalised;
}

async function fetchOdooCustomers() {
    const BASE = readEnv('ODOO_BASE', 'http://localhost:8069').replace(/\/$/, '');
    const COOKIE = readEnv('ODOO_COOKIE');
    const LIMIT = parseNumber(readEnv('ODOO_LIMIT'), 80);
    const OFFSET = parseNumber(readEnv('ODOO_OFFSET'), 0);
    const COUNT_LIMIT = parseNumber(readEnv('ODOO_COUNT_LIMIT'), 10001);
    const ORDER = readEnv('ODOO_ORDER', '');
    const REQUEST_ID = parseNumber(readEnv('ODOO_REQUEST_ID'), 105);
    const DOMAIN_RAW = readEnv('ODOO_DOMAIN');
    const ONLY_ACTIVE = readEnv('ODOO_ONLY_ACTIVE', 'false').toLowerCase() === 'true';

    ensureConfig([{ name: 'ODOO_COOKIE', value: COOKIE }]);

    let domain = [];
    if (DOMAIN_RAW) {
        try {
            domain = JSON.parse(DOMAIN_RAW);
        } catch {
            console.warn('Unable to parse ODOO_DOMAIN; using default []');
        }
    }

    if (ONLY_ACTIVE) {
        domain = Array.isArray(domain) ? [...domain, ['active', '=', true]] : [['active', '=', true]];
    }

    const companyIds = parseList(readEnv('ODOO_COMPANY_IDS', '1')).map((id) => parseNumber(id, null)).filter((id) => id !== null);
    const companyId = parseNumber(readEnv('ODOO_COMPANY_ID'), companyIds[0] ?? 1);
    const uid = parseNumber(readEnv('ODOO_UID'), 2);

    const context = {
        lang: readEnv('ODOO_LANG', 'en_US'),
        tz: readEnv('ODOO_TZ', 'UTC'),
        uid,
        allowed_company_ids: companyIds.length ? companyIds : [companyId],
        bin_size: true,
        default_is_company: true,
        current_company_id: companyId,
    };

    const payload = buildOdooPayload({
        requestId: REQUEST_ID,
        limit: LIMIT,
        offset: OFFSET,
        order: ORDER,
        domain,
        context,
        countLimit: COUNT_LIMIT,
    });

    const headers = buildOdooHeaders(COOKIE, BASE);
    const url = `${BASE}/web/dataset/call_kw/res.partner/web_search_read`;
    const response = await axios.post(url, payload, { headers });

    if (response.data?.error) {
        const err = new Error(response.data.error?.message || 'Odoo request failed');
        err.responseBody = response.data;
        throw err;
    }

    const records = response.data?.result?.records || [];
    const rows = normaliseOdooRecords(records, ONLY_ACTIVE);

    return {
        rows,
        metadata: {
            total: response.data?.result?.length ?? rows.length,
            limit: LIMIT,
            offset: OFFSET,
            order: ORDER,
            domain,
            baseUrl: BASE,
        },
    };
}

module.exports = { fetchOdooCustomers };
