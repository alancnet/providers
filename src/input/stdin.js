const { Observable } = require('rxjs')
const readline = require('readline')

const stdin = (config) => {
  const ret = Observable.create((observer) => {
    const rl = readline.createInterface({
      input: config.stdin || process.stdin,
      output: config.stdout || process.stdout,
      terminal: config.terminal || false
    })

    rl.on('line', (line) => observer.next(line))
    rl.on('close', () => observer.complete())
    return () => rl.close()
  })
  ret.acknowledge = () => {}
  return ret
}

module.exports = stdin
