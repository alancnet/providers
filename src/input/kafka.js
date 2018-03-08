const _ = require('lodash')
const KafkaRest = require('kafka-rest')
const objFlatten = require('obj-flatten')
const { Observable } = require('rxjs')

const kafkaInput = (_config) => new Promise((resolve, reject) => {
  const config = objFlatten(_config)

  if (!config.url) return reject('Kafka requires url')
  if (!config.consumerGroup) return reject('Kafka requires consumerGroup')
  if (!config.topic && !config.topics) return reject('Kafka requires topic or topics')
  if (config.topic && config.topics) return reject('Kafka requires only topic or topics')

  const kafka = new KafkaRest(_.pick(config, ['url']))
  kafka.consumer(config.consumerGroup)
    .join(_.omit(config, ['url, consumerGroup', 'topic', 'topics']), (err, consumer) => {
      if (err) return reject(err)
      const shutdown = () => consumer.shutdown((err) => {
        if (err) console.error(err)
      })
      resolve(Observable.create((observer) => {
        const stream = consumer.subscribe(config.topic)
        stream.on('data', (messages) =>
          messages.forEach((message) =>
            observer.next(Object.assign(new String(message.value), { // eslint-disable-line no-new-wrappers
              partition: message.partition,
              key: message.key
            }))
          )
        )
        stream.on('error', (err) => observer.error(err))
        stream.on('end', () => observer.complete())
        return shutdown
      }))
    })
})

module.exports = kafkaInput
