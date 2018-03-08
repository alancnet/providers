const _ = require('lodash')
const { createStreamingLogClient } = require('streaming-log-client')
const { Observable } = require('rxjs')

const createStreamingLogInput = (_config) => {
  const config = _.defaults(_config, {
    mode: 'string'
  })
  if (!config.url) throw new Error('StreamingLog requires url')
  if (!config.topic && !config.topics) throw new Error('StreamingLog requires topic or topics')

  const topics = config.topics ? config.topics.split(',') : [config.topic]
  const client = createStreamingLogClient(config.url)
  return Observable.from(topics)
    .flatMap((topicName) =>
      client.subscribe(topicName, {
        retryOnError: config.retryOnError,
        retryInterval: config.retryInterval,
        encoding: config.encoding,
        offset: config.offset,
        pollTimeout: config.pollTimeout,
        requestTimeout: config.requestTimeout,
        pageSize: config.pageSize
      })
        .map((message) =>
          config.mode === 'object'
          ? Object.assign({}, message, {topic: topicName})
          : message.value
        )
    )
}

module.exports = { createStreamingLogInput }
