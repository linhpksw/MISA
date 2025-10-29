const axios = require('axios');
const fs = require('fs');
const xlsx = require('xlsx');

const { readEnv, ensureConfig } = require('../utils/env');
const { loadContext } = require('../utils/context');
const { normalizeHeader, toCamelCase } = require('../utils/normalizer');

const HEADER_MARKERS = new Set([
    'customer_code',
    'customer_name',
    'ma_khach_hang',
    'ten_khach_hang',
]);

const HEADER_MAP = {
    customer_list: null,
    stt: null,
    customer_code: 'customerCode',
    customer_name: 'customerName',
    ma_khach_hang: 'customerCode',
    ten_khach_hang: 'customerName',
    address: 'address',
    dia_chi: 'address',
    closing_amount: 'outstandingAmount',
    outstanding_amount: 'outstandingAmount',
    cong_no: 'outstandingAmount',
    company_tax_code: 'taxCode',
    tax_code: 'taxCode',
    ma_so_thuecccd_chu_ho: 'taxCode',
    tel: 'phone',
    phone: 'phone',
    dien_thoai: 'phone',
    contact_mobile: 'contactMobile',
    dt_di_dong_nlh: 'contactMobile',
    is_local_object: 'isInternal',
    la_doi_tuong_noi_bo: 'isInternal',
    custom_field_1: 'additionalField1',
    custom_field1: 'additionalField1',
    truong_mo_rong: 'additionalField1',
    truong_mo_rong_1: 'additionalField1',
    email: 'email',
    contact_name: 'contactName',
};

function buildRequestHeaders(token, device, contextStr, cookie) {
    return {
        Authorization: token,
        'X-Device': device,
        'X-MISA-Context': contextStr,
        'Content-Type': 'application/json',
        Accept: 'application/json',
        ...(cookie ? { Cookie: cookie } : {}),
    };
}

function buildQueuePayload(baseUrl, branchId, fileType) {
    return {
        Columns: [
            { Key: 'account_object_code', Caption: 'Mã khách hàng', Width: 180, FormatType: 12, FooterText: 'Tổng' },
            { Key: 'account_object_name', Caption: 'Tên khách hàng', Width: 360, FormatType: 12 },
            { Key: 'address', Caption: 'Địa chỉ', Width: 344, FormatType: 12 },
            { Key: 'closing_amount', Caption: 'Công nợ', Width: 150, FormatType: 2 },
            { Key: 'company_tax_code', Caption: 'Mã số thuế/CCCD chủ hộ', Width: 200, FormatType: 12 },
            { Key: 'tel', Caption: 'Điện thoại', Width: 150, FormatType: 12 },
            { Key: 'contact_mobile', Caption: 'ĐT di động NLH', Width: 150, FormatType: 12 },
            { Key: 'is_local_object', Caption: 'Là Đối tượng nội bộ', Width: 200, FormatType: 13 },
            { Key: 'custom_field1', Caption: 'Trường mở rộng 1', Width: 120, FormatType: 12 },
            { Key: 'email', Caption: 'Email', Width: 220, FormatType: 12 },
            { Key: 'contact_name', Caption: 'Contact Name', Width: 220, FormatType: 12 },
        ],
        GetDataUrl: `${baseUrl}/g2/api/db/v1/list/get_data`,
        GetDataMethod: 'POST',
        GetDataParam: {
            sort: JSON.stringify([{ property: 'account_object_code', desc: false }]),
            filter: JSON.stringify([['is_customer', '=', true], 'and', ['is_employee', '=', false]]),
            pageIndex: 1,
            pageSize: 100,
            useSp: false,
            view: 'view_account_object_customer',
            dataType: 'di_customer',
            isGetTotal: true,
            is_filter_branch: false,
            current_branch: branchId,
            is_multi_branch: false,
            is_dependent: true,
            loadMode: 1,
        },
        DataCount: 3,
        FileType: fileType,
        ReportTitle: 'Customer List',
    };
}

async function queueExport(baseUrl, headers, payload) {
    const queueUrl = `${baseUrl}/g2/api/export/v1/export/save_param_worker_queue`;
    const response = await axios.post(queueUrl, payload, { headers });
    const exportId = response?.data?.Data?.export_id;
    if (!exportId) {
        const err = new Error('Export service did not return export_id');
        err.responseBody = response?.data;
        throw err;
    }
    return exportId;
}

async function pollExportStatus(baseUrl, exportId, headers, pollMax, pollDelay) {
    const pollUrl = `${baseUrl}/g2/api/export/v1/export/get_notify_export_by_pull/${exportId}`;
    let fileUrl = null;
    let fileNameDownload = null;
    let lastPollPayload = null;

    for (let i = 0; i < pollMax; i += 1) {
        await new Promise((resolve) => setTimeout(resolve, pollDelay));
        const pollResponse = await axios.get(pollUrl, { headers });
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

    return { fileUrl, fileNameDownload, lastPollPayload };
}

function buildDownloadCandidates(baseUrl, downloadHeaders, fileUrl, fileNameDownload, dbId, fileName, contextObj) {
    const candidates = [];
    const cleanBase = baseUrl.replace(/\/$/, '');
    if (fileUrl) {
        const resolved = fileUrl.startsWith('http')
            ? fileUrl
            : `${cleanBase}/${fileUrl.replace(/^\//, '')}`;
        candidates.push({ method: 'get', url: resolved, headers: downloadHeaders });
    }
    if (fileNameDownload) {
        const cleanName = fileNameDownload.replace(/^\//, '');
        const encodedName = encodeURIComponent(cleanName);
        const encodedDb = encodeURIComponent(dbId || contextObj.DatabaseId || '');
        const encodedOut = encodeURIComponent(fileName);
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
    return candidates;
}

async function downloadExportFile(candidates, lastPollPayload) {
    const attemptLog = [];
    for (const candidate of candidates) {
        try {
            const resp = await axios.get(candidate.url, {
                responseType: 'arraybuffer',
                headers: candidate.headers,
            });
            return { buffer: Buffer.from(resp.data), sourceUrl: candidate.url };
        } catch (downloadErr) {
            attemptLog.push({
                url: candidate.url,
                status: downloadErr.response?.status,
            });
        }
    }

    const err = new Error('Unable to download exported file from remote service.');
    err.attempted = attemptLog;
    err.lastPoll = lastPollPayload;
    throw err;
}

function parseWorkbook(buffer, exportId, metadata, options) {
    const workbook = xlsx.read(buffer, { type: 'buffer' });
    if (!workbook.SheetNames.length) {
        throw new Error('Downloaded workbook does not contain any sheets.');
    }

    const sheetName = workbook.SheetNames[0];
    const sheet = workbook.Sheets[sheetName];
    const sheetRows = xlsx.utils.sheet_to_json(sheet, { header: 1, defval: null, blankrows: false });
    if (!sheetRows.length) {
        return { rows: [], metadata: { ...metadata, exportId, sheetName, rowCount: 0 } };
    }

    const headerRowIndex = sheetRows.findIndex(
        (row) =>
            Array.isArray(row) &&
            row.some((cell) => HEADER_MARKERS.has(normalizeHeader(cell))),
    );

    if (headerRowIndex === -1) {
        throw new Error('Unable to locate header row in exported workbook.');
    }

    const rawHeaderRow = sheetRows[headerRowIndex];
    const dataRows = sheetRows.slice(headerRowIndex + 1);

    const normalizedHeaders = rawHeaderRow.map((header, index) => {
        const key = normalizeHeader(header) || `column_${index + 1}`;
        if (Object.prototype.hasOwnProperty.call(HEADER_MAP, key)) {
            return HEADER_MAP[key];
        }
        return toCamelCase(key);
    });

    const rows = dataRows
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
        .filter(
            (row) =>
                row &&
                row.customerCode !== 'Tổng' &&
                Object.keys(row).length > 0 &&
                (row.customerCode || row.customerName || row.additionalField1),
        );

    return {
        rows,
        metadata: {
            ...metadata,
            exportId,
            sheetName,
            rowCount: rows.length,
            sourceUrl: options.sourceUrl,
        },
    };
}

async function fetchMisaCustomers() {
    const BASE = readEnv('BASE', 'https://actapp.misa.vn').replace(/\/$/, '');
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

    const requestHeaders = buildRequestHeaders(TOKEN, DEVICE, contextStr, COOKIE);
    const queuePayload = buildQueuePayload(BASE, BR_ID, FILE_TYPE);
    const exportId = await queueExport(BASE, requestHeaders, queuePayload);

    const { fileUrl, fileNameDownload, lastPollPayload } = await pollExportStatus(
        BASE,
        exportId,
        requestHeaders,
        POLL_MAX,
        POLL_MS,
    );

    const downloadHeaders = { ...requestHeaders };
    delete downloadHeaders['Content-Type'];
    const downloadCandidates = buildDownloadCandidates(
        BASE,
        downloadHeaders,
        fileUrl,
        fileNameDownload,
        DB_ID,
        `${FILE_NAME}.${FILE_TYPE}`,
        contextObj,
    );

    const { buffer, sourceUrl } = await downloadExportFile(downloadCandidates, lastPollPayload);

    if (WRITE_FILE) {
        const outName = `${FILE_NAME}.${FILE_TYPE}`;
        fs.writeFileSync(outName, buffer);
    }

    return parseWorkbook(buffer, exportId, {
        databaseId: DB_ID,
        branchId: BR_ID,
        userId: US_ID,
        fileName: fileNameDownload,
    }, { sourceUrl });
}

module.exports = { fetchMisaCustomers };
