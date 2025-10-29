const { fetchMisaCustomers, fetchOdooCustomers } = require('./services');

module.exports = { fetchMisaCustomers, fetchOdooCustomers };

if (require.main === module) {
    const source = (process.argv[2] || 'misa').toLowerCase();
    const action = source === 'odoo' ? fetchOdooCustomers : fetchMisaCustomers;

    action()
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
                try {
                    const fs = require('fs');
                    fs.writeFileSync('last-notify.json', JSON.stringify(err.lastPoll, null, 2));
                    console.error('Wrote last-notify.json for inspection.');
                } catch (writeErr) {
                    console.error('Unable to write last-notify.json:', writeErr.message);
                }
            }
            process.exit(1);
        });
}
