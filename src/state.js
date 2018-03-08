const file = require('./state/file')
const _ = require('lodash');
const { Subject } = require('rxjs');

const getStateProvider = (config) => new Promise((resolve, reject) =>
  !config ? reject('No file config given.') :
  (config.driver === 'file')  ? resolve(file(config)) :
  reject(`Unknown state driver ${config.driver}`)
)

const state = (_config) => {
  const config = _.defaults(_config, {
    interval: 1000
  });
  return getStateProvider(config).then((provider) => {
    const input = new Subject();

    input.sampleTime(config.interval).subscribe((val) => provider.put(val));

    var latest;

    return {
      put: (val) => {
        latest = val;
        return provider.put(val);
      },
      get: () => {
        if (latest !== undefined) return Promise.resolve(latest);
        return provider.get();
      }
    };
  })
}

module.exports = state;
