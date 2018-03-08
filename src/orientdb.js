const _ = require('lodash');
const { ODatabase, utils } = require('orientjs');
const orientdbGremlin = require('./gremlin/orientdb');

const orientdbProvider = (_config) => {
  const config = _.defaults({}, _config, {
    port: 2424
  });

  if (!config.host) throw new Error('orientdb requires host');
  if (!config.port) throw new Error('orientdb requires port');
  if (!config.username) throw new Error('orientdb requires username');
  if (!config.password) throw new Error('orientdb requires password');
  if (!config.database) throw new Error('orientdb requires database');


  return orientdbGremlin(config).then((gremlin) => {
    var db = new ODatabase({
      host: config.host,
      port: config.port,
      username: config.username,
      password: config.password,
      name: config.database
    });
    return Object.assign(db,
      {gremlin},
      {
        upsertEdge: (from, to, className, extra) =>
          db.select().from(className)
          .where(Object.assign(
            {
              out: from,
              in: to
            },
            extra||{}
          ))
          .all()
          .then((results) =>
            results.length ? Promise.resolve(results[0]) :
            db.create('EDGE', className)
              .from(from)
              .to(to)
              .set(extra)
              .one()

          )
      }
    )
  });
}

module.exports = orientdbProvider;
