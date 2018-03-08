const _ = require('lodash')
const KafkaRest = require('kafka-rest')
const objFlatten = require('obj-flatten')

const kafkaOutput = (_config) => {
  const config = objFlatten(_config)

  if (!config.url) throw new Error('Kafka requires url')
  if (!config.topic) throw new Error('Kafka requires topic or topics')

  const kafka = new KafkaRest(_.pick(config, ['url']))
  const topic = kafka.topic(config.topic)
  return {
    next: (message) => topic.produce({
      key: message.key,
      partition: message.partition,
      value: message.toString()
    }, (err, res) => {
      if (err) {
        console.error(err)
        process.exit(1)
      }
    }),
    error: (err) => {
      console.error(err)
      process.exit(1)
    },
    complete: () => {}
  }
}

module.exports = kafkaOutput
