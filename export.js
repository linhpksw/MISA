const dotenv = require('dotenv');
const axios = require('axios');
const fs = require('fs');
const path = require('path');

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
        throw new Error(`Missing required configuration values: ${missing.join(', ')}. Update .env or context.json.`);
    }
}

const MIME_TYPES = {
    xlsx: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    xls: 'application/vnd.ms-excel',
    csv: 'text/csv',
};

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

async function exportCustomerReport() {
    const BASE = readEnv('BASE', 'https://actapp.misa.vn');
    const TOKEN = readEnv('TOKEN');
    const COOKIE = readEnv('COOKIE');

    const FILE_NAME = readEnv('FILE_NAME', 'Danh_sach_khach_hang');
    const FILE_TYPE = readEnv('FILE_TYPE', 'xlsx');

    const pollMaxCandidate = Number(readEnv('POLL_MAX', 20));
    const pollMsCandidate = Number(readEnv('POLL_INTERVAL_MS', 2000));
    const POLL_MAX = Number.isFinite(pollMaxCandidate) && pollMaxCandidate > 0 ? pollMaxCandidate : 20;
    const POLL_MS = Number.isFinite(pollMsCandidate) && pollMsCandidate > 0 ? pollMsCandidate : 2000;

    const { contextObj, contextStr } = loadContext();

    const contextDbId = contextObj.DatabaseId || contextObj.database_id || contextObj.databaseId || null;
    const contextBranchId = contextObj.BranchId || contextObj.branch_id || contextObj.branchId || null;
    const contextUserId = contextObj.UserId || contextObj.user_id || contextObj.userId || null;

    const DEVICE = readEnv('DEVICE');
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

    const headers = {
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
            { Key: 'account_object_code', Caption: 'Ma khach hang', FormatType: 12, Width: 180 },
            { Key: 'account_object_name', Caption: 'Ten khach hang', FormatType: 12, Width: 360 },
            { Key: 'address', Caption: 'Dia chi', FormatType: 12, Width: 344 },
            { Key: 'closing_amount', Caption: 'Cong no', FormatType: 2, Width: 150 },
            { Key: 'company_tax_code', Caption: 'Ma so thue/CCCD', FormatType: 12, Width: 200 },
            { Key: 'tel', Caption: 'Dien thoai', FormatType: 12, Width: 150 },
            { Key: 'custom_field1', Caption: 'Truong mo rong 1', FormatType: 12, Width: 120 },
        ],
        GetDataUrl: `${BASE}/g2/api/db/v1/list/get_data`,
        GetDataMethod: 'POST',
        GetDataParam: {
            sort: JSON.stringify(sortArr),
            filter: JSON.stringify(filterArr),
            pageIndex: 1,
            pageSize: 20,
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
        ReportTitle: 'Danh sach khach hang',
    };

    const outName = `${FILE_NAME}.${FILE_TYPE}`;

    const queueUrl = `${BASE}/g2/api/export/v1/export/save_param_worker_queue`;
    const qRes = await axios.post(queueUrl, queuePayload, { headers });
    const exportId = qRes?.data?.Data?.export_id;
    if (!exportId) {
        const err = new Error('Export service did not return export_id');
        err.responseBody = qRes?.data;
        throw err;
    }

    const pollUrl = `${BASE}/g2/api/export/v1/export/get_notify_export_by_pull/${exportId}`;
    let fileUrl = null;
    let fileNameDownload = null;
    let lastPollPayload = null;

    for (let i = 0; i < POLL_MAX; i++) {
        await new Promise((resolve) => setTimeout(resolve, POLL_MS));
        const st = await axios.get(pollUrl, { headers });
        const data = st?.data?.Data || {};
        lastPollPayload = st?.data;

        const status = data.Status ?? data.status ?? data.ExportStatus ?? null;
        const nextFileName =
            data.file_name_download || data.FileNameDownload || data.file_name || data.FileName || null;
        if (nextFileName && !fileNameDownload) {
            fileNameDownload = nextFileName;
        }

        fileUrl =
            data.FileUrl || data.DownloadUrl || data.file_url || data.file_download_url || data.DownloadPath || null;

        if (fileUrl) break;
        if (status === 3) break;
    }

    if (!fileUrl && !fileNameDownload) {
        const err = new Error('Export finished without providing a download URL.');
        err.lastPoll = lastPollPayload;
        throw err;
    }

    const downloadHeaders = { ...headers };
    delete downloadHeaders['Content-Type'];

    const cleanBase = BASE.replace(/\/$/, '');
    let resolvedFileUrl = null;
    if (fileUrl) {
        resolvedFileUrl = fileUrl.startsWith('http') ? fileUrl : `${cleanBase}/${fileUrl.replace(/^\//, '')}`;
    }

    const candidates = [];
    if (resolvedFileUrl) candidates.push({ method: 'get', url: resolvedFileUrl, headers: downloadHeaders });
    if (fileNameDownload) {
        const cleanName = fileNameDownload.replace(/^\//, '');
        const encodedName = encodeURIComponent(cleanName);
        const encodedDb = encodeURIComponent(DB_ID || contextObj.DatabaseId || '');
        const encodedOut = encodeURIComponent(outName);
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
    const contentType = MIME_TYPES[(FILE_TYPE || '').toLowerCase()] || 'application/octet-stream';

    return {
        buffer,
        fileName: outName,
        contentType,
        sourceUrl: resolvedFileUrl,
        metadata: {
            exportId,
            fileNameDownload,
            dbId: DB_ID,
            userId: US_ID,
        },
    };
}

module.exports = { exportCustomerReport };

if (require.main === module) {
    exportCustomerReport()
        .then(({ buffer, fileName, sourceUrl }) => {
            fs.writeFileSync(fileName, buffer);
            console.log(`Saved ${fileName} from ${sourceUrl}`);
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
