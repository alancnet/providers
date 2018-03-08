const pubSub = require('../pub-sub');

const pubSubOutput = (config) =>
  !config ? Promise.reject('No output.pubsub config given.') :
    pubSub(config.pubSub)
    .then((ps) => ({
      next: (val) => ps.publish(config.topic, val),
      error: console.error,
      complete: () => {}
    }));
