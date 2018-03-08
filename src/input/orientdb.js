const _ = require('lodash');
const { ODatabase, utils } = require('orientjs');
const { Observable } = require('rxjs');

const orientdbOutput = (_config) => {
  const providers = require('../..');
  const config = _.defaults({}, _config, {
    port: 2424,
    ensureClass: false,
    incrementingField: undefined,
    state: {
      driver: 'file'
    },
    batchReadSize: 100
  });

  if (!config.host) throw new Error('orientdb requires host');
  if (!config.port) throw new Error('orientdb requires port');
  if (!config.username) throw new Error('orientdb requires username');
  if (!config.password) throw new Error('orientdb requires password');
  if (!config.database) throw new Error('orientdb requires database');
  if (!config.class && !config.query) throw new Error('orientdb requires class or query');

  const dummyState = {
    get: function() { return this.state ? Promise.resolve(this.state) : Promise.reject('No state'); },
    put: function(s) { this.state = s; return Promise.resolve(); }
  };

  return (
    config.state.name ?
    providers.state(config.state) :
    Promise.resolve(dummyState)
  ).then((stateProvider) => {

    const db = new ODatabase({
      host: config.host,
      port: config.port,
      username: config.username,
      password: config.password,
      name: config.database,
      useToken: true
    });

    db
      .open()
      .catch((err) => {
        console.error(err);
        process.exit(1);
      })
    ;


// //TODO:
//   1. Establish live query that writes to a buffer
//   2. Query existing data, 10 records at a time.
//   3. For each query we get back, trim anything in the buffer that preceeds it
//   4. Write query response to stream.
//   5. When no more data comes back from initial queries, emit the buffer.
//   6. Switch live query to emit instead of cache to buffer
    return stateProvider.get().catch(() => ({}))
      .then((state) => {
        const batchRead = (skip) => {
          var query = `SELECT * FROM ${config.class} `;
          if (config.incrementingField) {
            if (state.hasOwnProperty('progress')) {
              query = query + `WHERE ${config.incrementingField} > ${utils.encode(state.progress)} `
            }
            query = query + `ORDER BY ${config.incrementingField} LIMIT ${config.batchReadSize} `
          } else {
            query = query + `LIMIT ${config.batchReadSize} SKIP ${skip}`
          }
          return db.query(query).all()
          // var query = db.select().from(config.class);
          // if (config.incrementingField) {
          //   if (state.hasOwnProperty('progress')) {
          //     // If we know our progress, use it.
          //     query = query.gt(config.incrementingField, state.progress);
          //   }
          //   query = query.order(config.incrementingField);
          // }
          // query = query.limit(config.batchReadSize);
          // return query.all()
        }

        return Observable.create((observer) => {
          const cache = [];
          var abort = false;
          var saveTimer;
          var onLiveRecord = (val) => cache.push(val);

          const liveQuery = db.liveQuery(`LIVE SELECT FROM ${config.class}`)
            .on('live-insert', (v) => onLiveRecord(v.content))
            .on('live-update', (v) => onLiveRecord(v.content));

          const next = observer.next.bind(observer);

          var lastState;

          const onProgress = (val) => {
            const newProgress = val[config.incrementingField];
            if (!state.progress || newProgress > state.progress) {
              state.progress = val[config.incrementingField];
            }
            stateProvider.put(state);
          }

          const doInitial = (skip) =>
            batchRead(skip || 0)
              .then((results) => {
                if (results.length) {
                  results.forEach((val) => {onProgress(val); next(val)})
                  return doInitial((skip||0) + results.length);
                } else {
                  return null;
                }
              })

          const flushCache = () => {
            cache
              .filter((val) => val[config.incrementingField] > state.progress)
              .forEach((val) => {onProgress(val); next(val)})
          }

          const goLive = () => {
            onLiveRecord = (val) => {onProgress(val); next(val)}
          }

          doInitial()
          .then(flushCache)
          .then(goLive)
          .catch((err) => {
            console.error(err);
            process.exit(1);
          })


          return () => {
            abort = true;
            liveQuery.close();
          }
        })
      });



  });

}

module.exports = orientdbOutput;
