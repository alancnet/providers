const _ = require('lodash')
const { createStreamingLogClient } = require('streaming-log-client')
const { Observable } = require('rxjs')

const createStreamingLogInput = (_config) => {
  const providers = require('../..')
  const config = _.defaults(_config, {
    mode: 'string'
  })
  if (!config.url) throw new Error('StreamingLog requires url')
  if (!config.topic && !config.topics) throw new Error('StreamingLog requires topic or topics')
  if (!config.state) throw new Error('StreamingLog requires state')

  const topics = config.topics ? config.topics.split(',') : [config.topic]
  const client = createStreamingLogClient(config.url)
  const offsetQueue = []
  return providers.state(config.state).then((state) => {
    const currentState = state.initialState
    const ret = Observable.from(topics)
      .flatMap((topicName) => {
        const defaultOffset = currentState[topicName] || 0
        const beginOffset = Math.max(0,
          config.offset && (config.offset[0] === '-' || config.offset[0] === '+')
          ? defaultOffset + +config.offset
          : config.offset ? +config.offset
          : defaultOffset
        )
        return client.subscribe(topicName, {
          retryOnError: config.retryOnError,
          retryInterval: config.retryInterval,
          encoding: config.encoding,
          offset: beginOffset,
          pollTimeout: config.pollTimeout,
          requestTimeout: config.requestTimeout,
          pageSize: config.pageSize
        })
          .map((message) => {
            offsetQueue.push({topic: topicName, offset: message.offset})
            return config.mode === 'object'
            ? Object.assign({}, message, {topic: topicName})
            : message.value
          })
      })
    ret.acknowledge = () => {
      if (!offsetQueue.length) {
        console.error('More acks than inputs')
        process.exit(1)
      } else {
        const offset = offsetQueue.shift()
        currentState[offset.topic] = offset.offset + 1
        state.put(currentState)
      }
    }
    return ret
  })
}

module.exports = { createStreamingLogInput }
