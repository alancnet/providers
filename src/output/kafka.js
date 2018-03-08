const _ = require('lodash')
const KafkaRest = require('kafka-rest')
const objFlatten = require('obj-flatten')

const kafkaOutput = (_config) => {
  const config = _.defaults(objFlatten(_config), {
    partitionMode: 'time'
  })

  if (!config.url) throw new Error('Kafka requires url')
  if (!config.topic) throw new Error('Kafka requires topic or topics')

  const partitionKeyProvider =
    config.partitionMode === 'time'
    ? timeParitionKeyProvider()
    : () => {}

  const kafka = new KafkaRest(_.pick(config, ['url']))
  const topic = kafka.topic(config.topic)
  return {
    next: (message) => topic.produce({
      key: partitionKeyProvider(message),
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

const timeParitionKeyProvider = () => {
  console.log('Using time partition key provider')
  var partition = null
  const reset = () => {
    partition = null
  }
  return (record) => {
    if (!partition) {
      partition = new Date().getTime().toString(16)
      setImmediate(reset)
    }
    console.log('parition', partition)
    return partition
  }
}

module.exports = kafkaOutput
