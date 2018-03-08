const { Observable } = require('rxjs');
const readline = require('readline');

const stdin = (config) => Observable.create((observer) => {
  const rl = readline.createInterface({
    input: config.stdin || process.stdin,
    output: config.stdout || process.stdout,
    terminal: config.terminal || false
  });

  rl.on('line', (line) => observer.next(line));
  rl.on('close', () => observer.complete());
  return () => rl.close()
});

module.exports = stdin;
