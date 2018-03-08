const orientdb = require('./gremlin/orientdb')

const gremlin = (config) => new Promise((resolve, reject) =>
  !config ? reject('Non config for gremlin given')
  : (config.driver === 'orientdb') ? resolve(orientdb(config))
  : reject(`Unknown gremlin driver: ${config.driver}`)
)

module.exports = gremlin
