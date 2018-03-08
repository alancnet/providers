const _ = require('lodash');
const { ODatabase } = require('orientjs');

const orientdbOutput = (_config) => {
  const config = _.defaults({}, _config, {
    port: 2424,
    ensureClass: true
  });

  if (!config.host) throw new Error('orientdb requires host');
  if (!config.port) throw new Error('orientdb requires port');
  if (!config.username) throw new Error('orientdb requires username');
  if (!config.password) throw new Error('orientdb requires password');
  if (!config.database) throw new Error('orientdb requires database');
  if (!config.class) throw new Error('orientdb requires class');

  const db = new ODatabase({
    host: config.host,
    port: config.port,
    username: config.username,
    password: config.password,
    name: config.database
  });

  return (
    config.ensureClass ?
    db.class.create(config.class).catch((err) => db.class.get(config.class)) :
    db.class.get(config.class)
  )
    .then((myClass) => {
      return {
        next: (val) => {
          if (config.upsert) {
            const upsertCondition = _.pick(val, config.upsert.split(','));
            if (!Object.keys(upsertCondition).length) {
              console.error(JSON.stringify(val));
              console.error('Empty upsert clause.');
              process.exit(1);
            } else {
              return db.update(config.class)
                .set(val)
                .upsert(upsertCondition)
                .scalar()
                .catch((err) => {
                  console.error(JSON.stringify(val))
                  console.error(err)
                  console.error('upsert', upsertCondition, Object.keys(upsertCondition))
                  process.exit(1);
                })
            }
          } else {
            return myClass.create(val)
              .catch((err) => {
                console.error(JSON.stringify(val))
                console.error(err)
                process.exit(1);
              })
          }
        },
        error: (err) => {
          db.close();
          console.error(err)
        },
        complete: () => {
          db.close();
        }
      }
    })
}

module.exports = orientdbOutput;
