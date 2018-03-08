const pubSub = require('./output/pub-sub');
const stdout = require('./output/stdout');
const tcp = require('./output/tcp');
const orientdb = require('./output/orientdb');

const output = (config) => new Promise((resolve, reject) =>
  !config ? reject('No output config given.') :
  (config.driver === 'pubSub')  ? resolve(pubSub(config)) :
  (config.driver === 'stdout') ? resolve(stdout(config)) :
  (config.driver === 'tcp') ? resolve(tcp(config)) :
  (config.driver === 'orientdb') ? resolve(orientdb(config)) :
  reject(`Unknown output driver ${config.driver}`)
)
  .then((observer) =>
    config.codec === 'json' ? jsonCodec(observer) :
    observer
  );

const jsonCodec = (observer) => ({
  next: (val) => observer.next(
    typeof val === 'object' ? JSON.stringify(val) : val
  ),
  error: (err) => observer.error(err),
  complete: () => observer.complete()
});


module.exports = output;
