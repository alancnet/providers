const _ = require('lodash')
const { ODatabase, utils } = require('orientjs')
const rp = require('request-promise-native')

const gremlinEncode = (val) =>
  (val === 'null' || val === 'undefined') ? 'null'
  : Array.isArray(val) ? `[${val.map(gremlinEncode).join(', ')}]`
  : (val instanceof Object) ? (
    val.hasOwnProperty('@rid') ? `g.v(${utils.encode(val['@rid'].toString())})`
    : `[${
      Object.keys(val).map((key) =>
        `${utils.encode(key)}: ${gremlinEncode(val[key])}`
      ).join(', ')
    }]`
  )
  : utils.encode(val)

const gremlinTemplate = function (template) {
  const ret = []
  template.forEach((text, index) => {
    if (index) {
      ret.push(gremlinEncode(arguments[index]))
    }
    ret.push(text)
  })
  return ret.join('')
}

const sqlTemplate = function (template) {
  const params = {}
  const sb = [template[0]]
  for (var i = 1; i < arguments.length; i++) {
    params[`p${i}`] = arguments[i]
    sb.push(`:p${i}`)
    sb.push(template[i])
  }
  return {
    query: sb.join(''),
    params: params
  }
}

const orientdbProvider = (_config) => {
  const config = _.defaults({}, _config, {
    port: 2424,
    restPort: 2480
  })

  if (!config.host) throw new Error('orientdb requires host')
  if (!config.port) throw new Error('orientdb requires port')
  if (!config.username) throw new Error('orientdb requires username')
  if (!config.password) throw new Error('orientdb requires password')
  if (!config.database) throw new Error('orientdb requires database')

  return Promise.resolve().then(() => {
    var db = new ODatabase({
      host: config.host,
      port: config.port,
      username: config.username,
      password: config.password,
      name: config.database
    })

    const command = (language, commandText) => {
      const url = `http://${config.host}:${config.restPort}/command/${encodeURIComponent(config.database)}/${encodeURIComponent(language)}/${encodeURIComponent(commandText)}/2100000000/*:1`
      console.log(url)
      require('request').debug = true
      return rp.post(url, {
        auth: {
          user: config.username,
          pass: config.password,
          sendImmediately: true
        }
      })
      .then(JSON.parse)
      .then((x) => x.result)
    }

    const gremlin = function (query) {
      if (Array.isArray(query)) return gremlin(gremlinTemplate.apply(this, arguments))
      return command('gremlin', query)
    }

    const sqlQuery = (query, opts) => {
      return db.query(query, opts)
    }

    const sql = function (query, params) {
      if (Array.isArray(query)) return sql(sqlTemplate.apply(this, arguments))
      if (typeof query === 'object') {
        return sqlQuery(query.query, { params: query.params, limit: 2100000000 })
      }
      if (params) {
        return sqlQuery(query, {params, limit: 2100000000})
      }
      return sqlQuery(query, {limit: 2100000000})
    }

    return Object.assign(db,
      {gremlin, command, sql},
      {
        upsertEdge: (from, to, className, extra) =>
          db.select().from(className)
          .where(Object.assign(
            {
              out: from,
              in: to
            },
            extra || {}
          ))
          .all()
          .then((results) =>
            results.length ? Promise.resolve(results[0])
            : db.create('EDGE', className)
              .from(from)
              .to(to)
              .set(extra)
              .one()

          )
      }
    )
  })
}

module.exports = orientdbProvider
