// export.js (CommonJS)
// Run: node export.js
const dotenv = require('dotenv');
const envResult = dotenv.config();
const envVars = envResult.parsed || {};
const axios = require('axios');
const fs = require('fs');
const path = require('path');

(async () => {
    const BASE = process.env.BASE || envVars.BASE || 'https://actapp.misa.vn';
    const TOKEN = process.env.TOKEN || envVars.TOKEN;
    const DEVICE = process.env.DEVICE || envVars.DEVICE;
    const COOKIE = process.env.COOKIE || envVars.COOKIE;
    const DB_ID = process.env.DATABASE_ID || envVars.DATABASE_ID;
    const BR_ID = process.env.BRANCH_ID || envVars.BRANCH_ID;
    const US_ID = process.env.USER_ID || envVars.USER_ID;

    const FILE_NAME = process.env.FILE_NAME || envVars.FILE_NAME || 'Danh_sach_khach_hang';
    const FILE_TYPE = process.env.FILE_TYPE || envVars.FILE_TYPE || 'xlsx';
    const POLL_MAX = Number(process.env.POLL_MAX || envVars.POLL_MAX || 20);
    const POLL_MS = Number(process.env.POLL_INTERVAL_MS || envVars.POLL_INTERVAL_MS || 2000);

    if (!TOKEN || !DEVICE || !DB_ID || !BR_ID || !US_ID) {
        console.error('Missing required envs. Check .env for TOKEN, DEVICE, DATABASE_ID, BRANCH_ID, USER_ID.');
        process.exit(1);
    }

    // Load context as an object, then stringify once for headers/extra_data
    const contextObj = JSON.parse(fs.readFileSync(path.join(__dirname, 'context.json'), 'utf8'));
    const contextStr = JSON.stringify(contextObj);

    const headers = {
        Authorization: TOKEN,
        'X-Device': DEVICE,
        'X-MISA-Context': contextStr,
        'Content-Type': 'application/json',
        Accept: 'application/json',
        ...(COOKIE ? { Cookie: COOKIE } : {}),
    };

    // Build filter/sort and stringify (server expects strings here)
    const filterArr = [['is_customer', '=', true], 'and', ['is_employee', '=', false]];
    const sortArr = [{ property: 'account_object_code', desc: false }];

    const queuePayload = {
        Columns: [
            {
                Key: 'account_object_code',
                Caption: 'Mã khách hàng',
                FormatType: 12,
                Width: 180,
            },
            {
                Key: 'account_object_name',
                Caption: 'Tên khách hàng',
                FormatType: 12,
                Width: 360,
            },
            { Key: 'address', Caption: 'Địa chỉ', FormatType: 12, Width: 344 },
            { Key: 'closing_amount', Caption: 'Công nợ', FormatType: 2, Width: 150 },
            {
                Key: 'company_tax_code',
                Caption: 'Mã số thuế/CCCD chủ hộ',
                FormatType: 12,
                Width: 200,
            },
            { Key: 'tel', Caption: 'Điện thoại', FormatType: 12, Width: 150 },
            {
                Key: 'custom_field1',
                Caption: 'Trường mở rộng 1',
                FormatType: 12,
                Width: 120,
            },
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
        ReportTitle: 'Danh sách khách hàng',
    };

    try {
        // 1) Queue export
        const queueUrl = `${BASE}/g2/api/export/v1/export/save_param_worker_queue`;
        const qRes = await axios.post(queueUrl, queuePayload, { headers });
        const exportId = qRes?.data?.Data?.export_id;
        if (!exportId) {
            console.error('No export_id returned:', qRes?.data);
            process.exit(1);
        }
        console.log('Queued export_id:', exportId);

        // 2) Poll for status + file URL
        const pollUrl = `${BASE}/g2/api/export/v1/export/get_notify_export_by_pull/${exportId}`;
        let fileUrl = null;
        let fileNameDownload = null;
        for (let i = 0; i < POLL_MAX; i++) {
            await new Promise((r) => setTimeout(r, POLL_MS));
            const st = await axios.get(pollUrl, { headers });
            const data = st?.data?.Data || {};

            const status = data.Status ?? data.status ?? data.ExportStatus ?? null;
            const nextFileName =
                data.file_name_download ||
                data.FileNameDownload ||
                data.file_name ||
                data.FileName ||
                null;
            if (nextFileName && !fileNameDownload) {
                fileNameDownload = nextFileName;
                console.log(`Found file_name_download: ${fileNameDownload}`);
            }
            fileUrl =
                data.FileUrl ||
                data.DownloadUrl ||
                data.file_url ||
                data.file_download_url ||
                data.DownloadPath ||
                null;

            console.log(`poll#${i} status=${status ?? 'unknown'} url=${fileUrl ?? ''}`);
            if (fileUrl) break;
            if (status === 3) break;
        }

        if (!fileUrl && !fileNameDownload) {
            console.warn(
                'No download URL found in notify response. Inspect the full response to locate the file field.'
            );
            fs.writeFileSync('last-notify.json', JSON.stringify((await axios.get(pollUrl, { headers })).data, null, 2));
            console.warn('Wrote last-notify.json for inspection.');
            process.exit(2);
        }

        // 3) Download file
        const outName = `${FILE_NAME}.${FILE_TYPE}`;
        const downloadHeaders = { ...headers };
        delete downloadHeaders['Content-Type'];

        let resolvedFileUrl = null;
        if (fileUrl) {
            resolvedFileUrl = fileUrl.startsWith('http')
                ? fileUrl
                : `${BASE.replace(/\/$/, '')}/${fileUrl.replace(/^\//, '')}`;
        }

        const candidates = [];
        if (resolvedFileUrl)
            candidates.push({ method: 'get', url: resolvedFileUrl, headers: downloadHeaders });
        if (fileNameDownload) {
            const cleanBase = BASE.replace(/\/$/, '');
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
        let attempted = [];
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
                attempted.push({
                    url: candidate.url,
                    method: candidate.method,
                    status: downloadErr.response?.status,
                    body: downloadErr.response?.data,
                });
            }
        }

        if (!fileResp) {
            console.warn('Unable to download file. Attempted endpoints:', attempted);
            fs.writeFileSync('last-notify.json', JSON.stringify((await axios.get(pollUrl, { headers })).data, null, 2));
            console.warn('Wrote last-notify.json for inspection.');
            process.exit(3);
        }

        fs.writeFileSync(outName, fileResp.data);
        console.log('Saved', outName, 'from', resolvedFileUrl);
    } catch (err) {
        if (err.response) {
            console.error('HTTP ERROR', err.response.status, err.response.statusText);
            console.error('Body:', err.response.data);
        } else {
            console.error('ERROR', err.message);
        }
        process.exit(1);
    }
})();
