const file = require('./state/file')
const _ = require('lodash')

const getStateProvider = (config) => new Promise((resolve, reject) =>
  !config ? reject('No file config given.')
  : (config.driver === 'file') ? resolve(file(config))
  : reject(`Unknown state driver ${config.driver}`)
)

const state = (_config) => {
  const config = _.defaults(_config, {
    interval: 1000
  })
  return getStateProvider(config).then((provider) => {
    // const input = new Subject();
    //
    // input.sampleTime(config.interval).subscribe((val) => provider.put(val));

    var timer = null
    var state = null

    const check = () => {
      if (state) {
        provider.put(state)
          .then(() => { timer = setTimeout(check, config.interval) })
        state = null
      } else {
        timer = null
      }
    }

    const onNewState = (newState) => {
      state = newState
      if (!timer) {
        // Save immediately because enough time elapsed without a new state
        // to expire the timer.
        timer = -1 // prevent simultaneous instant saves
        provider.put(newState)
          .then(() => { timer = setTimeout(check, config.interval) })
      }
    }

    var latest

    return {
      put: (val) => {
        latest = val
        onNewState(val)
        return Promise.resolve(val)
      },
      get: () => {
        if (latest !== undefined) return Promise.resolve(latest)
        return provider.get()
      }
    }
  })
    .then((stateProvider) =>
      stateProvider.get().catch(() => ({})).then((initialState) =>
      Object.assign(stateProvider, {initialState})
    ))
}

module.exports = state
