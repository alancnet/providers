const stdin = require('./input/stdin')
const tcp = require('./input/tcp')
const orientdb = require('./input/orientdb')
const googlePubsub = require('./input/googlePubsub')
const kafka = require('./input/kafka')
const {createStreamingLogInput} = require('./input/streaming-log')

const input = (config) => new Promise((resolve, reject) =>
  !config ? reject('No input config given.')
  : (config.driver === 'stdin') ? resolve(stdin(config))
  : (config.driver === 'tcp') ? resolve(tcp(config))
  : (config.driver === 'orientdb') ? resolve(orientdb(config))
  : (config.driver === 'googlePubsub') ? resolve(googlePubsub(config))
  : (config.driver === 'kafka') ? resolve(kafka(config))
  : (config.driver === 'streamingLog') ? resolve(createStreamingLogInput(config))
  : reject(`Unknown input driver ${config.driver}`)
)
  .then((stream) =>
    config.codec === 'json' ? jsonCodec(stream)
    : stream
  )

const jsonCodec = (stream) => {
  const ret = stream.map((input) =>
    typeof input === 'string' ? JSON.parse(input)
    : input
  )
  ret.acknowledge = stream.acknowledge
  return ret
}

module.exports = input
