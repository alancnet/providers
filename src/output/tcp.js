const _ = require('lodash')
const net = require('net')

const tcpOutput = (_config) => {
  const config = _.defaults({}, _config, {
    retry: Infinity,
    retryTimeout: Infinity,
    interval: 1000,
    quiet: false,
    noDelay: true,
    port: 3000
  })
  const url = `tcp://${config.host || 'localhost'}:${config.port}`

  const log = (text) => config.quiet || console.warn(text)

  const tryConnect = (count) => new Promise((resolve, reject) => {
    var fin = false
    log(`Attempting to connect to ${url}. (Attempt ${count})`)
    const sock = net.connect({
      host: config.host,
      port: config.port
    }, () => {
      log(`Connected to ${url}.`)
      fin = true
      resolve(sock)
    })
    sock.on('error', (err) => {
      if (!fin) {
        fin = true
        log(`Could not connect to ${url}: ${err.message}`)
        if (count >= config.retry) {
          reject(err)
        } else {
          setTimeout(() => {
            resolve(tryConnect(count + 1))
          }, config.interval)
        }
      }
    })
  })

  return tryConnect(1)
    .then((sock) => {
      sock.setNoDelay(config.noDelay)
      sock.on('error', (err) => {
        log(`Error on connection to ${url}: ${err.message}`)
        process.exit(1) // Microservices are required to be volatile.
      })
      sock.on('end', () => {
        log(`Disconnected from ${url}.`)
        process.exit(1) // Microservices are required to be volatile.
      })
      sock.on('data', (data) => {
        log(`Unexpected input from ${url}: ${data.toString()}`)
      })

      return {
        next: (val) => sock.write(new Buffer(val + '\n')),
        error: (err) => {
          console.error(err)
          sock.end()
          process.exit(1)
        },
        complete: () => {
          sock.end()
        }
      }
    })
}

module.exports = tcpOutput
