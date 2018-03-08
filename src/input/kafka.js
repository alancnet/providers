const _ = require('lodash')
const KafkaRest = require('kafka-rest')
const objFlatten = require('obj-flatten')
const { Observable } = require('rxjs')
const { onExit } = require('../util')

const kafkaInput = (_config) => new Promise((resolve, reject) => {
  const config = _.defaults(objFlatten(_config), {
    mode: 'string', // string, object, or binary
    consumerGroup: new Date().getTime().toString(),
    'auto.commit.enable': 'true',
    'auto.offset.reset': 'smallest'
  })

  if (!config.url) return reject('Kafka requires url')
  if (!config.consumerGroup) return reject('Kafka requires consumerGroup')
  if (!config.topic && !config.topics) return reject('Kafka requires topic or topics')
  if (config.topic && config.topics) return reject('Kafka requires only topic or topics')

  const topics = config.topic ? [config.topic] : config.topics.split(',')
  const consumerConfig = _.pickBy(config, (v, key) => key.includes('.'))

  const kafka = new KafkaRest(_.pick(config, ['url']))
  kafka.consumer(config.consumerGroup)
    .join(
      consumerConfig,
      (err, consumer) => {
        if (err) return reject(err)
        var shutdowned = false
        const shutdown = () => new Promise((resolve, reject) => {
          if (!shutdowned) {
            console.error(`Shutting down consumer: ${config.consumerGroup}`)
            consumer.shutdown((err) => {
              if (err) reject(err)
              else resolve(console.error('Shutdown succeeded'))
            })
            shutdowned = true
          } else {
            resolve()
          }
        })
        onExit(shutdown)
        resolve(Observable.merge.apply(null,
          topics.map((topic) =>
            Observable.create((observer) => {
              const stream = consumer.subscribe(topic)
              stream.on('data', (messages) => {
                try {
                  messages.forEach((message) => {
                    config.mode === 'object'
                    ? observer.next(Object.assign({}, message, {topic}))
                    : config.mode === 'string'
                    ? observer.next(message.value && message.value.toString())
                    : observer.next(message.value)
                  })
                } catch (err) {
                  console.error(err)
                  observer.error(err)
                }
              })
              stream.on('error', (err) => observer.error(err))
              stream.on('end', () => observer.complete())
              return shutdown
            })
          )
        ))
      })
})

module.exports = kafkaInput
