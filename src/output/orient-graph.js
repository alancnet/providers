const _ = require('lodash')
const { ODatabase } = require('orientjs')
const { Subject } = require('rxjs')

const CLASS = '@class'
const REF = '@ref'
const TYPE = '@type'
const RID = '@rid'
const UPSERT = '@upsert'
const EDGE = 'EDGE'
const VERTEX = 'VERTEX'

const isRid = (text) => /^#\d*:\d*$/.test(text)

const orientGraphOutput = (_config) => {
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

  const db = new ODatabase({
    host: config.host,
    port: config.port,
    username: config.username,
    password: config.password,
    name: config.database
  })

  // Verify connectivity to database
  return db.class.list().then(() => {
    const refs = {}
    const queue = []
    const done = new Subject()

    var processing = false
    var closed = false
    // Process elements one at a time without overlapping work.
    const checkQueue = () => {
      if (queue.length) {
        if (processing) return
        processing = true
        const item = queue.shift()
        processItem(item)
          .then(() => {
            done.next(item)
            processing = false
            checkQueue()
          })
          .catch((err) => {
            console.error(err)
            Object.keys(err).forEach((key) =>
              console.error(`${key}:`, err[key])
            )
            done.error(err)
            process.exit(1)
          })
      } else if (closed) {
        db.close()
        console.log('Closed')
      }
    }

    // Process a single item.
    const processItem = (_val) => {
      console.info('Processing', JSON.stringify(_val))
      // Create a mutable copy of the object
      var val = Object.assign({}, _val)
      var myClass = null
      var upsert = null
      if (val[CLASS]) {
        myClass = val[CLASS]
        delete val[CLASS]
      }

      // @ref: Saves this object for future reference by this ID.
      if (val[REF]) {
        /* TMP */ console.info(`Saving ref ${val[REF]}: `, val)
        refs[val[REF]] = val
        delete val[REF]
      }

      // @upsert: "a,b,c". Looks for an existing record by these fields.
      if (val[UPSERT]) {
        upsert = val[UPSERT].split(',')
        delete val[UPSERT]
      } else {
        // Auto-detect upsert
        if (val[TYPE] === EDGE) {
          // Normalize vocabulary (out == from, in == to)
          if (val.from && val.to && !val.out && !val.in) {
            // From-to vocabulary used. Convert to out-in
            val.out = val.from
            val.in = val.to
            delete val.from
            delete val.to
          }
          if (!val.out || !val.in) {
            throw new Error(`Attempted to insert edge without \`out\` and \`in\`: ${JSON.stringify(val)}`)
          }
          upsert = ['from', 'to']
        } else if (val.id) {
          upsert = ['id']
        } else {
          throw new Error(`Attempted to insert a record without an upsert or \`id\` field: ${JSON.stringify(val)}`)
        }
      }

      // Insert / Update for Vertices
      if (val[TYPE] === VERTEX) {
        delete val[TYPE]
        if (upsert) {
          /* TMP */ console.info('Upserting vertex')
          return db.select()
            .from(myClass || 'V')
            .where(_.pick(val, upsert))
            .all()
            .then((data) => {
              /* TMP */ console.info('Updating vertex')
              if (data.length > 1) throw new Error('Upsert query returned more than one record')
              if (data.length === 1) {
                // update
                val[RID] = data[0][RID]
                const update = _.omit(val, upsert, RID)
                if (Object.keys(update).length) {
                  /* TMP */ console.info('Updating vertex', JSON.stringify(update))
                  return db.update(data[0][RID])
                    .set(update)
                    .return('AFTER')
                    .one()
                } else {
                  /* TMP */ console.info('nothing left to update on vertex')
                  return Promise.resolve()
                }
              } else {
                // insert
                /* TMP */ console.info('Creating new vertex')
                return db.create(VERTEX, myClass || 'V')
                  .set(val)
                  .one()
                  .then((newVal) => Object.assign(val, newVal))
              }
            })
        } else {
          /* TMP */ console.info('Creating vertex blindly')
          return db.create(VERTEX, myClass || 'V')
            .set(val)
            .one()
            .then(record => Object.assign(val, record))
        }
      } else if (val[TYPE] === EDGE) {
        // Insert / Update for Edges
        delete val[TYPE]
        // Resolve references:

        if (!isRid(val.out)) {
          if (!refs[val.out]) throw new Error(`Unable to find out reference: ${val.out}`)
          val.out = refs[val.out][RID]
        }
        if (!val.out) throw new Error(`out reference has no @rid.`)

        if (!isRid(val.in)) {
          if (!refs[val.in]) throw new Error(`Unable to find in reference: ${val.in}`)
          val.in = refs[val.in][RID]
        }

        if (!val.in) throw new Error(`in reference has no @rid.`)

        if (upsert) {
          /* TMP */ console.info('Upserting edge')
          return db.select()
            .from(myClass || 'E')
            .where(_.pick(val, upsert, 'out', 'in'))
            .all()
            .then((data) => {
              if (data.length > 1) throw new Error('Upsert query returned more than one record')
              if (data.length === 1) {
                // update
                val[RID] = data[0][RID]
                const update = _.omit(val, upsert, RID, 'in', 'out')
                if (Object.keys(update).length) {
                  /* TMP */ console.info('Updating edge', JSON.stringify(update), Object.keys(update))
                  return db.update(data[0][RID])
                    .set(update)
                    .return('AFTER')
                    .one()
                    .catch((er) => {
                      console.error(er)
                      process.exit(1)
                    })
                } else {
                  /* TMP */ console.info('Nothing left to update on edge')
                  return Promise.resolve()
                }
              } else {
                /* TMP */ console.info('Creating new edge')
                // insert
                return db.create(EDGE, myClass || 'V')
                  .from(val.out)
                  .to(val.in)
                  .set(_.omit(val, 'out', 'in'))
                  .one()
                  .then((newVal) => Object.assign(val, newVal))
              }
            })
        } else {
          /* TMP */ console.info('Creating edge blindly')
          // insert
          return db.create(EDGE, myClass || 'V')
            .set(val)
            .one()
            .then((newVal) => Object.assign(val, newVal))
        }
      }
      throw new Error('Graph requires @type="VERTEX" or @type="EDGE"')
    } // ProcessItem

    return {
      next: (val) => {
        queue.push(Object.assign({}, val))
        checkQueue()
      },
      error: (err) => {
        console.error(err)
        process.exit(1)
      },
      complete: () => {
        console.log('Completed')
        closed = true
        checkQueue()
      },
      done
    }
  })
}

module.exports = orientGraphOutput
