const _ = require('lodash')
const { ODatabase } = require('orientjs')

const orientdbOutput = (_config) => {
  const config = _.defaults({}, _config, {
    port: 2424,
    ensureClass: false,
    upsert: undefined,
    timestamp: true
  })

  if (!config.host) throw new Error('orientdb requires host')
  if (!config.port) throw new Error('orientdb requires port')
  if (!config.username) throw new Error('orientdb requires username')
  if (!config.password) throw new Error('orientdb requires password')
  if (!config.database) throw new Error('orientdb requires database')

  // const server = OrientDB({
  //   host: config.host,
  //   port: config.port,
  //   username: config.username,
  //   password: config.password,
  //   logger: {
  //     log: (msg) => console.log('LOG', msg),
  //     debug: (msg) => console.log('DEBUG', msg),
  //     error: (err) => console.error('CAUGHT', err)
  //   }
  //
  // })
  //
  // const db = server.use({
  //   name: config.database,
  //   username: config.username,
  //   password: config.password,
  //   logger: {
  //     log: (msg) => console.log('LOG', msg),
  //     debug: (msg) => console.log('DEBUG', msg),
  //     error: (err) => console.error('CAUGHT', err)
  //   }
  //
  // })

  const db = new ODatabase({
    host: config.host,
    port: config.port,
    username: config.username,
    password: config.password,
    name: config.database
  })

  return db.class.list().then(() => ({
    next: (_val) => {
      if (typeof _val === 'object') {
        const val = _.defaultsDeep({},
          config.overrides,
          _val,
          config.defaults
        )
        const myClass = val['@class'] || config.class
        delete val['@class']
        if (config.upsert) {
          const upsertCondition = _.pick(val, config.upsert.split(','))
          if (!Object.keys(upsertCondition).length) {
            console.error(JSON.stringify(val))
            console.error('Empty upsert clause.')
            process.exit(1)
          } else {
            return db.update(myClass)
              .set(val)
              .upsert(upsertCondition)
              .scalar()
              .catch((err) => {
                console.error(JSON.stringify(val))
                console.error(err)
                console.error('upsert', upsertCondition, Object.keys(upsertCondition))
                process.exit(1)
              })
          }
        } else {
          return db.insert().into(myClass).set(val).one()
            .catch((err) => {
              console.error(JSON.stringify(val))
              console.error(err)
              process.exit(1)
            })
        }
      } else if (typeof _val === 'string') {
        return db.query(_val).all()
          .catch((err) => {
            console.error(JSON.stringify(_val))
            console.error(err)
            process.exit(1)
          })
      } else {
        console.error(`Unexpected data type: ${typeof _val}`)
        process.exit(1)
      }
    },
    error: (err) => {
      db.close()
      console.error(err)
    },
    complete: () => {
      db.close()
    }
  }))
  .catch((err) => {
    console.error(err)
    process.exit(1)
  })
}

module.exports = orientdbOutput
