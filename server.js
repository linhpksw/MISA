const express = require('express');
const morgan = require('morgan');
const { fetchMisaCustomers, fetchOdooCustomers } = require('./services');

const PORT = Number(process.env.PORT || 3000);
const app = express();

app.use(morgan('tiny'));

app.get('/health', (_req, res) => {
    res.json({ status: 'ok' });
});

app.get('/misa/customers', async (_req, res) => {
    try {
        const { rows, metadata } = await fetchMisaCustomers();
        res.json({ data: rows, meta: metadata });
    } catch (error) {
        console.error('MISA customer fetch failed', error);
        const status = error.response?.status;
        res.status(status && status >= 400 ? status : 500).json({
            message: 'Failed to retrieve MISA customers.',
            detail: error.message,
        });
    }
});

app.get('/odoo/customers', async (_req, res) => {
    try {
        const { rows, metadata } = await fetchOdooCustomers();
        res.json({ data: rows, meta: metadata });
    } catch (error) {
        console.error('Odoo customer fetch failed', error);
        const status = error.response?.status;
        res.status(status && status >= 400 ? status : 500).json({
            message: 'Failed to retrieve Odoo customers.',
            detail: error.message,
        });
    }
});

app.listen(PORT, () => {
    console.log(`Customer service listening on port ${PORT}`);
});
