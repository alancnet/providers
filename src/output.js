const stdout = require('./output/stdout')
const tcp = require('./output/tcp')
const orientdb = require('./output/orientdb')
const orientGraph = require('./output/orient-graph')
const clock = require('utc-clock')
const _ = require('lodash')

const output = (_config) => {
  const config = _.defaults({}, _config, {
    defaults: {},
    overrides: {}
  })
  return new Promise((resolve, reject) =>
    !config ? reject('No output config given.')
    : (config.driver === 'stdout') ? resolve(stdout(config))
    : (config.driver === 'tcp') ? resolve(tcp(config))
    : (config.driver === 'orientdb') ? resolve(orientdb(config))
    : (config.driver === 'orient-graph') ? resolve(orientGraph(config))
    : reject(`Unknown output driver ${config.driver}`)
  )
  .then((observer) =>
    config.codec === 'json' ? jsonCodec(config, observer)
    : observer
  )
}

const decorateObject = (config, obj) => _.defaultsDeep({},
  config.overrides,
  obj,
  config.defaults,
  config.timestamp ? {
    '@timestamp': clock.now.ms()
  } : {}
)

const jsonCodec = (config, observer) => ({
  next: (val) => observer.next(
    Array.isArray(val) ? val
    : typeof val === 'object' ? JSON.stringify(decorateObject(config, val)) : val
  ),
  error: (err) => observer.error(err),
  complete: () => observer.complete()
})

module.exports = output
