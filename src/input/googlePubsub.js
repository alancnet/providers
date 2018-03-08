const googlePubsub = require('@google-cloud/pubsub')
const { Observable } = require('rxjs')

const googleError = (err) =>
  err
  ? [err.toString()].concat(
    err.metadata &&
    err.metadata._internal_repr &&
    err.metadata._internal_repr['google.rpc.debuginfo-bin'] &&
    err.metadata._internal_repr['google.rpc.debuginfo-bin'].map((rep) =>
      Buffer.from(rep.toString(), 'base64').toString()
    ) || []
  ).join('\n')
  : ''

const googlePubsubInput = (config) => {
  const gps = googlePubsub({
    projectId: config.projectId,
    keyFilename: config.keyFilename,
    credentials: config.credentials
  })

  const topic = config.topic
  ? Promise.resolve(gps.topic(config.topic))
  : Promise.reject('No topic specified')

  return topic.then((topic) =>
    topic.exists().then((exists) =>
      exists
      ? topic.subscribe(config.name).then(([sub]) => {
        const progressQueue = []
        const ret = Observable.create((observer) => {
          const next = (message) => {
            progressQueue.push(message.ackId)
            observer.next(message.data)
          }
          const error = observer.error.bind(observer)

          sub.on('message', next)
          sub.on('error', error)

          return () => {
            sub.removeListener('message', next)
            sub.removeListener('error', error)
          }
        }).share()

        ret.acknowledge = () => {
          if (!progressQueue.length) {
            throw new Error('input.acknowledge() called more that inputs received.')
          }
          sub.ack(progressQueue.shift())
        }

        return ret
      })
      : Promise.reject('Topic does not exist')
    )
  )
  .catch((err) => Promise.reject(googleError(err)))
}

module.exports = googlePubsubInput
