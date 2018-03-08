const createTeeOutput = (config) => {
  const providers = require('../..')
  return Promise.all([
    providers.output(config.left || {
      driver: 'stdout',
      codec: 'json'
    }),
    providers.output(config.right || {
      driver: 'stdout',
      codec: 'json'
    })
  ]).then(([left, right]) => ({
    next: (val) => [left.next(val), right.next(val)],
    error: (err) => [left.error(err), right.error(err)],
    complete: () => [left.complete(), right.complete()],
    acknowledge: () => [left.acknowledge(), right.acknowledge()]
  }))
}

module.exports = {createTeeOutput}
