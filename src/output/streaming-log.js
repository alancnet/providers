const { createStreamingLogClient } = require('streaming-log-client')
const { Subject } = require('rxjs')

const createStreamingLogOutput = (config) => {
  if (!config.url) throw new Error('StreamingLog requires url')
  if (!config.topic) throw new Error('StreamingLog requires topic')

  const client = createStreamingLogClient(config.url)
  const subject = new Subject()
  const stream = subject.map((val) => {
    if (!(val instanceof Buffer) && typeof val === 'object') {
      return JSON.stringify(val)
    } else {
      return val
    }
  })
  const publisher = client.publish(config.topic, stream)
  publisher.on('error', (err) => {
    console.error(err)
    process.exit(1)
  })
  return subject
}

module.exports = { createStreamingLogOutput }
