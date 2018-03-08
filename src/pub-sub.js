// TODO: jsdoc
const simple = require('./pub-sub/simple.js')

const pubSub = (config) => new Promise((resolve, reject) =>
  !config ? reject('No pubSub config given.')
  : (config.driver === 'simple') ? resolve(simple(config))
  : reject(`Unknown driver ${config.driver}`)
)

module.exports = pubSub
