const express = require('express');
const morgan = require('morgan');
const { exportCustomerReport } = require('./export');

const PORT = Number(process.env.PORT || 3000);
const app = express();

app.use(morgan('tiny'));

app.get('/health', (_req, res) => {
    res.json({ status: 'ok' });
});

app.get('/export', async (_req, res) => {
    try {
        const { buffer, fileName, contentType } = await exportCustomerReport();
        const encodedFileName = encodeURIComponent(fileName);

        res.set({
            'Content-Type': contentType,
            'Content-Disposition': `attachment; filename="${fileName}"; filename*=UTF-8''${encodedFileName}`,
            'Cache-Control': 'no-store',
        });

        res.send(buffer);
    } catch (error) {
        console.error('Export failed', error);
        const status = error.response?.status;
        res.status(status && status >= 400 ? status : 500).json({
            message: 'Failed to generate export file.',
            detail: error.message,
        });
    }
});

app.listen(PORT, () => {
    console.log(`Export server listening on port ${PORT}`);
});
