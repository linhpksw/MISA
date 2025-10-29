const dotenv = require('dotenv');
const axios = require('axios');
const fs = require('fs');
const path = require('path');
const xlsx = require('xlsx');

const envResult = dotenv.config();
const envFileVars = envResult.parsed || {};

function readEnv(key, fallback = undefined) {
    if (process.env[key] !== undefined) return process.env[key];
    if (envFileVars[key] !== undefined) return envFileVars[key];
    return fallback;
}

function ensureConfig(configs) {
    const missing = configs.filter(({ value }) => !value).map(({ name }) => name);
    if (missing.length) {
        throw new Error(
            `Missing required configuration values: ${missing.join(', ')}. Update .env or context.json.`,
        );
    }
}

let cachedContext = null;

function loadContext() {
    if (cachedContext) return cachedContext;

    const contextPath = path.join(__dirname, 'context.json');
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

function normalizeHeader(header) {
    if (!header) return null;
    return String(header)
        .trim()
        .replace(/\s+/g, ' ')
        .replace(/[^\w ]/g, '')
        .replace(/\s+/g, '_')
        .toLowerCase();
}

function toCamelCase(str) {
    return String(str || '')
        .replace(/[_\s]+(.)?/g, (_, chr) => (chr ? chr.toUpperCase() : ''))
        .replace(/^[A-Z]/, (c) => c.toLowerCase());
}

function parseNumber(value, fallback) {
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

function mapOdooRelation(rel) {
    if (!rel) return null;
    if (Array.isArray(rel)) {
        const [id, name] = rel;
        if (id === undefined && name === undefined) return null;
        return { id: id ?? null, name: name ?? null };
    }
    if (typeof rel === 'object') {
        return {
            id: rel.id ?? null,
            name: rel.display_name ?? rel.name ?? null,
        };
    }
    return { id: null, name: String(rel) };
}

async function fetchMisaCustomers() {
    const BASE = readEnv('BASE', 'https://actapp.misa.vn');
    const TOKEN = readEnv('TOKEN');
    const COOKIE = readEnv('COOKIE');
    const DEVICE = readEnv('DEVICE');

    const FILE_NAME = readEnv('FILE_NAME', 'customer_list');
    const FILE_TYPE = readEnv('FILE_TYPE', 'xlsx');
    const POLL_MAX = Number(readEnv('POLL_MAX', 20));
    const POLL_MS = Number(readEnv('POLL_INTERVAL_MS', 2000));
    const WRITE_FILE = readEnv('WRITE_FILE', 'false').toLowerCase() === 'true';

    const { contextObj, contextStr } = loadContext();
    const contextDbId = contextObj.DatabaseId || contextObj.database_id || contextObj.databaseId || null;
    const contextBranchId = contextObj.BranchId || contextObj.branch_id || contextObj.branchId || null;
    const contextUserId = contextObj.UserId || contextObj.user_id || contextObj.userId || null;

    const DB_ID = contextDbId || readEnv('DATABASE_ID');
    const BR_ID = contextBranchId || readEnv('BRANCH_ID');
    const US_ID = contextUserId || readEnv('USER_ID');

    ensureConfig([
        { name: 'TOKEN', value: TOKEN },
        { name: 'DEVICE', value: DEVICE },
        { name: 'DATABASE_ID', value: DB_ID },
        { name: 'BRANCH_ID', value: BR_ID },
        { name: 'USER_ID', value: US_ID },
    ]);

    const requestHeaders = {
        Authorization: TOKEN,
        'X-Device': DEVICE,
        'X-MISA-Context': contextStr,
        'Content-Type': 'application/json',
        Accept: 'application/json',
        ...(COOKIE ? { Cookie: COOKIE } : {}),
    };

    const filterArr = [['is_customer', '=', true], 'and', ['is_employee', '=', false]];
    const sortArr = [{ property: 'account_object_code', desc: false }];

    const queuePayload = {
        Columns: [
            { Key: 'account_object_code', Caption: 'Customer Code', FormatType: 12, Width: 180 },
            { Key: 'account_object_name', Caption: 'Customer Name', FormatType: 12, Width: 360 },
            { Key: 'address', Caption: 'Address', FormatType: 12, Width: 344 },
            { Key: 'closing_amount', Caption: 'Outstanding Amount', FormatType: 2, Width: 150 },
            { Key: 'company_tax_code', Caption: 'Tax Code', FormatType: 12, Width: 200 },
            { Key: 'tel', Caption: 'Phone', FormatType: 12, Width: 150 },
            { Key: 'custom_field1', Caption: 'Custom Field 1', FormatType: 12, Width: 120 },
            { Key: 'email', Caption: 'Email', FormatType: 12, Width: 220 },
            { Key: 'contact_name', Caption: 'Contact Name', FormatType: 12, Width: 220 },
        ],
        GetDataUrl: `${BASE}/g2/api/db/v1/list/get_data`,
        GetDataMethod: 'POST',
        GetDataParam: {
            sort: JSON.stringify(sortArr),
            filter: JSON.stringify(filterArr),
            pageIndex: 1,
            pageSize: 100,
            useSp: false,
            view: 'view_account_object_customer',
            dataType: 'di_customer',
            isGetTotal: true,
            is_filter_branch: false,
            current_branch: BR_ID,
            is_multi_branch: false,
            is_dependent: true,
            loadMode: 1,
        },
        DataCount: 3,
        FileType: FILE_TYPE,
        ReportTitle: 'Customer List',
    };

    const queueUrl = `${BASE}/g2/api/export/v1/export/save_param_worker_queue`;
    const queueResponse = await axios.post(queueUrl, queuePayload, { headers: requestHeaders });
    const exportId = queueResponse?.data?.Data?.export_id;
    if (!exportId) {
        const err = new Error('Export service did not return export_id');
        err.responseBody = queueResponse?.data;
        throw err;
    }

    const pollUrl = `${BASE}/g2/api/export/v1/export/get_notify_export_by_pull/${exportId}`;
    let fileUrl = null;
    let fileNameDownload = null;
    let lastPollPayload = null;

    for (let i = 0; i < POLL_MAX; i++) {
        await new Promise((resolve) => setTimeout(resolve, POLL_MS));
        const pollResponse = await axios.get(pollUrl, { headers: requestHeaders });
        const data = pollResponse?.data?.Data || {};
        lastPollPayload = pollResponse?.data;

        fileNameDownload =
            data.file_name_download ||
            data.FileNameDownload ||
            data.file_name ||
            data.FileName ||
            fileNameDownload;

        fileUrl =
            data.FileUrl ||
            data.DownloadUrl ||
            data.file_url ||
            data.file_download_url ||
            data.DownloadPath ||
            fileUrl;

        const status = data.Status ?? data.status ?? data.ExportStatus ?? null;
        if (fileUrl) break;
        if (status === 3) break;
    }

    if (!fileUrl && !fileNameDownload) {
        const err = new Error('Export finished without providing a download URL.');
        err.lastPoll = lastPollPayload;
        throw err;
    }

    const downloadHeaders = { ...requestHeaders };
    delete downloadHeaders['Content-Type'];

    const cleanBase = BASE.replace(/\/$/, '');
    let resolvedFileUrl = null;
    if (fileUrl) {
        resolvedFileUrl = fileUrl.startsWith('http')
            ? fileUrl
            : `${cleanBase}/${fileUrl.replace(/^\//, '')}`;
    }

    const candidates = [];
    if (resolvedFileUrl)
        candidates.push({ method: 'get', url: resolvedFileUrl, headers: downloadHeaders });
    if (fileNameDownload) {
        const cleanName = fileNameDownload.replace(/^\//, '');
        const encodedName = encodeURIComponent(cleanName);
        const encodedDb = encodeURIComponent(DB_ID || contextObj.DatabaseId || '');
        const encodedOut = encodeURIComponent(`${FILE_NAME}.${FILE_TYPE}`);
        candidates.push({
            method: 'get',
            url: `${cleanBase}/g2/api/file/v1/file/download?type=Temp&file=${encodedName}&dbid=${encodedDb}&name=${encodedOut}`,
            headers: downloadHeaders,
        });
        candidates.push({
            method: 'get',
            url: `${cleanBase}/g2/api/export/v1/export/download_file/${cleanName}`,
            headers: downloadHeaders,
        });
    }

    let fileResp = null;
    const attemptLog = [];
    for (const candidate of candidates) {
        try {
            const resp = await axios.get(candidate.url, {
                responseType: 'arraybuffer',
                headers: candidate.headers,
            });
            fileResp = resp;
            resolvedFileUrl = candidate.url;
            break;
        } catch (downloadErr) {
            attemptLog.push({
                url: candidate.url,
                status: downloadErr.response?.status,
            });
        }
    }

    if (!fileResp) {
        const err = new Error('Unable to download exported file from remote service.');
        err.attempted = attemptLog;
        err.lastPoll = lastPollPayload;
        throw err;
    }

    const buffer = Buffer.from(fileResp.data);

    if (WRITE_FILE) {
        const outName = `${FILE_NAME}.${FILE_TYPE}`;
        fs.writeFileSync(outName, buffer);
    }

    const workbook = xlsx.read(buffer, { type: 'buffer' });
    if (!workbook.SheetNames.length) {
        throw new Error('Downloaded workbook does not contain any sheets.');
    }

    const sheetName = workbook.SheetNames[0];
    const sheet = workbook.Sheets[sheetName];
    const sheetRows = xlsx.utils.sheet_to_json(sheet, { header: 1, defval: null, blankrows: false });
    if (!sheetRows.length) {
        return {
            rows: [],
            metadata: {
                exportId,
                sheetName,
                sourceUrl: resolvedFileUrl,
                rowCount: 0,
                databaseId: DB_ID,
                branchId: BR_ID,
                userId: US_ID,
                fileName: fileNameDownload,
            },
        };
    }

    const headerRowIndex = sheetRows.findIndex(
        (row) =>
            Array.isArray(row) &&
            row.some((cell) => {
                const key = normalizeHeader(cell);
                return key === 'customer_code' || key === 'customer_name';
            }),
    );

    if (headerRowIndex === -1) {
        throw new Error('Unable to locate header row in exported workbook.');
    }

    const rawHeaderRow = sheetRows[headerRowIndex];
    const dataRows = sheetRows.slice(headerRowIndex + 1);

    const HEADER_MAP = {
        customer_list: null,
        stt: null,
        customer_code: 'customerCode',
        customer_name: 'customerName',
        address: 'address',
        outstanding_amount: 'outstandingAmount',
        tax_code: 'taxCode',
        phone: 'phone',
        custom_field_1: 'customField1',
        email: 'email',
        contact_name: 'contactName',
    };

    const normalizedHeaders = rawHeaderRow.map((header, index) => {
        const key = normalizeHeader(header) || `column_${index + 1}`;
        if (Object.prototype.hasOwnProperty.call(HEADER_MAP, key)) {
            return HEADER_MAP[key];
        }
        return toCamelCase(key);
    });

    const normalizedRows = dataRows
        .map((row) => {
            if (!Array.isArray(row)) return null;
            const normalized = {};
            normalizedHeaders.forEach((headerKey, index) => {
                if (!headerKey) return;
                const value = row[index];
                if (value === null || value === '') return;
                normalized[headerKey] = typeof value === 'string' ? value.trim() : value;
            });
            return normalized;
        })
        .filter((row) => row && Object.keys(row).length > 0 && (row.customerCode || row.customerName));

    return {
        rows: normalizedRows,
        metadata: {
            exportId,
            sheetName,
            sourceUrl: resolvedFileUrl,
            rowCount: normalizedRows.length,
            databaseId: DB_ID,
            branchId: BR_ID,
            userId: US_ID,
            fileName: fileNameDownload,
        },
    };
}

async function fetchOdooCustomers() {
    const BASE = readEnv('ODOO_BASE', 'http://localhost:8069');
    const COOKIE = readEnv('ODOO_COOKIE');
    const LIMIT = parseNumber(readEnv('ODOO_LIMIT'), 80);
    const OFFSET = parseNumber(readEnv('ODOO_OFFSET'), 0);
    const COUNT_LIMIT = parseNumber(readEnv('ODOO_COUNT_LIMIT'), 10001);
    const ORDER = readEnv('ODOO_ORDER', '');
    const REQUEST_ID = parseNumber(readEnv('ODOO_REQUEST_ID'), 105);
    const DOMAIN_RAW = readEnv('ODOO_DOMAIN');
    const ONLY_ACTIVE = readEnv('ODOO_ONLY_ACTIVE', 'false').toLowerCase() === 'true';

    ensureConfig([{ name: 'ODOO_COOKIE', value: COOKIE }]);

    const cleanBase = BASE.replace(/\/$/, '');
    const url = `${cleanBase}/web/dataset/call_kw/res.partner/web_search_read`;

    let domain = [];
    if (DOMAIN_RAW) {
        try {
            domain = JSON.parse(DOMAIN_RAW);
        } catch {
            console.warn('Unable to parse ODOO_DOMAIN; falling back to default []');
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

    const payload = {
        id: REQUEST_ID,
        jsonrpc: '2.0',
        method: 'call',
        params: {
            model: 'res.partner',
            method: 'web_search_read',
            args: [],
            kwargs: {
                specification,
                offset: OFFSET,
                order: ORDER,
                limit: LIMIT,
                context,
                count_limit: COUNT_LIMIT,
                domain,
            },
        },
    };

    const headers = {
        'Content-Type': 'application/json',
        Accept: 'application/json',
        ...(COOKIE ? { Cookie: COOKIE } : {}),
        Referer: `${cleanBase}/web`,
    };

    const response = await axios.post(url, payload, { headers });

    if (response.data?.error) {
        const err = new Error(response.data.error?.message || 'Odoo request failed');
        err.responseBody = response.data;
        throw err;
    }

    const records = response.data?.result?.records || [];

    const normalized = records.map((record) => {
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

    const filtered = ONLY_ACTIVE ? normalized.filter((item) => item.active !== false) : normalized;

    return {
        rows: filtered,
        metadata: {
            total: response.data?.result?.length ?? filtered.length,
            limit: LIMIT,
            offset: OFFSET,
            order: ORDER,
            domain,
            baseUrl: cleanBase,
        },
    };
}

module.exports = { fetchMisaCustomers, fetchOdooCustomers };

if (require.main === module) {
    fetchMisaCustomers()
        .then((data) => {
            console.log(JSON.stringify(data, null, 2));
        })
        .catch((err) => {
            if (err.response) {
                console.error('HTTP ERROR', err.response.status, err.response.statusText);
                console.error('Body:', err.response.data);
            } else {
                console.error(err.message);
            }
            if (err.attempted) {
                console.error('Attempted download URLs:', err.attempted);
            }
            if (err.lastPoll) {
                fs.writeFileSync('last-notify.json', JSON.stringify(err.lastPoll, null, 2));
                console.error('Wrote last-notify.json for inspection.');
            }
            process.exit(1);
        });
}
