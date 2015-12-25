/* globals before, describe, it, beforeEach, after, afterEach */
var rethinkHost = process.env.RETHINKDB_HOST || 'localhost',
    rethinkPort = process.env.RETHINKDB_PORT || '28015',
    rethinkCfg = 'http://' + rethinkHost + ':' + rethinkPort + '/agenda_test';

module.exports = require('rethinkdbdash')({
    host: rethinkHost,
    port: rethinkPort,
    db: 'agenda_test'
});
