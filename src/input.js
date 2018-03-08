const stdin = require('./input/stdin')
const tcp = require('./input/tcp')
const orientdb = require('./input/orientdb')

const input = (config) => new Promise((resolve, reject) =>
  !config ? reject('No input config given.')
  : (config.driver === 'stdin') ? resolve(stdin(config))
  : (config.driver === 'tcp') ? resolve(tcp(config))
  : (config.driver === 'orientdb') ? resolve(orientdb(config))
  : reject(`Unknown input driver ${config.driver}`)
)
  .then((stream) =>
    config.codec === 'json' ? jsonCodec(stream)
    : stream
  )

const jsonCodec = (stream) => stream.map((input) =>
  typeof input === 'string' ? JSON.parse(input)
  : input
)

module.exports = input
