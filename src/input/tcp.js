const _ = require('lodash')
const net = require('net')
const Chunker = require('chunk-by')
const { Observable } = require('rxjs')

const tcpInput = (_config) => Observable.create((observer) => {
  const config = _.defaults({}, _config, {
    host: '0.0.0.0',
    port: 3000,
    quiet: false,
    noDelay: true
  })
  const url = `tcp://${config.host || 'localhost'}:${config.port}`

  const log = (text) => config.quiet || console.warn(text)

  const server = net.createServer((sock) => {
    sock.setNoDelay(config.noDelay)
    const onLine = (data) => {
      observer.next(data.slice(0, data.length - 1).toString())
    }

    const chunker = new Chunker(onLine, {pattern: '\n'})

    log(`Client connected.`)
    sock.on('data', (data) => {
      chunker.write(data)
    })
    sock.on('end', () => {
      log(`Client disconnected.`)
    })
    sock.on('error', (err) => {
      log(`Client error: ${err.message}`)
    })
  })

  server.on('error', (err) => {
    observer.error(err)
  })

  server.listen(config.port, () => {
    log(`Listening on ${url}.`)
  })
}).share()

module.exports = tcpInput
