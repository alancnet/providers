const _ = require('lodash')
const { ODatabase, utils } = require('orientjs')

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

const orientdbGremlin = (_config) => {
  const config = _.defaults({}, _config, {
    port: 2424
  })

  if (!config.host) throw new Error('orientdb requires host')
  if (!config.port) throw new Error('orientdb requires port')
  if (!config.username) throw new Error('orientdb requires username')
  if (!config.password) throw new Error('orientdb requires password')
  if (!config.database) throw new Error('orientdb requires database')

  const db = new ODatabase({
    host: config.host,
    port: config.port,
    username: config.username,
    password: config.password,
    name: config.database
  })

  function doQuery (query) {
    if (query instanceof Array) {
      return doQuery(gremlinTemplate.apply(null, arguments))
    } else if (typeof query === 'string') {
      const grem = query.split('\n')
        .map((line) => line.trim())
        .join(' ')

      const sql = `SELECT expand(gremlin(${utils.encode(grem)}))`
      console.log(sql)

      return db.query(sql)
      // .then((result) => {
      //   if (result.length > 1) throw new Error('Expected one result from gremlin query')
      //   return result[0].gremlin
      // })
    }
  }

  return Promise.resolve(doQuery)
}

module.exports = orientdbGremlin
