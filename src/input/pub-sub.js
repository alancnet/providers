const pubSub = require('../pub-sub')

const pubSubInput = (config) =>
  !config ? Promise.reject('No input.pubsub config given.')
  : pubSub(config.pubSub)
    .then((ps) =>
      ps.getTopic(config.topic)
    )

module.exports = pubSubInput
