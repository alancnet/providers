const { Subject, Observable } = require('rxjs');
const Websocket = require('ws');

const simple = (config) => new Promise((resolve, reject) => {
  const ws = new Websocket(config.url);
  ws.on('error', reject);
  ws.on('open', () => {
    const logic = {
      initialize: () => {
        resolve({
          getTopic: (name) => right.call('getTopic', name),
          publish: (name, obj) => right.call('publish', name, obj)
        })
      }
    };
    const right = new RxRpc({provider: logic});
    right.output.forEach((obj) => ws.send(JSON.stringify(obj)));
    ws.on('message', (msg) => right.input.next(JSON.parse(msg)));
    ws.on('close', () => right.input.complete());
  })
});

module.exports = simple;
