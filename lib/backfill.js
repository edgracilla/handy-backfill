'use strict'

const _ = require('lodash')
const deserialize = require('fast-json-parse')
const serialize = require('fast-safe-stringify')

function Backfill (options) {
  if (!options.broker) throw new Error(`'broker-nats' not initialized. Please check your NATS url/config.`)

  this.broker = options.broker
  this.cacher = options.cacher
  this.resource = options.resource
  this.namespace = this.broker.namespace

  this.refmap = {}
  this.pk = options.pkey || '_id'
  this.fetchName = options.fetchName || 'read'

  const crawler = function (schema, refmap, root, arr = false) {
    Object.keys(schema).forEach(field => {
      let rmkey = root ? `${root}${arr ? '*' : '.'}${field}` : field
      let prop = schema[field]

      if (prop.ref) {
        refmap[rmkey] = { ref: prop.ref, array: false }
      } else {
        if (Array.isArray(prop)) {
          if (prop[0].ref) {
            refmap[rmkey] = { ref: prop[0].ref, array: true }
          } else if (prop[0].constructor.name === 'Schema') {
            crawler(prop[0].obj, refmap, rmkey, true)
          } else if (typeof prop[0] === 'object') {
            crawler(prop[0], refmap, rmkey, true)
          }
        } else if (prop.constructor.name === 'Schema') {
          crawler(prop.obj, refmap, rmkey)
        } else if (typeof prop === 'object') {
          crawler(prop, refmap, rmkey)
        }
      }
    })
  }

  crawler(options.schema, this.refmap)
}

Backfill.prototype.set = function (doc, options) {
  if (!doc) return Promise.resolve(null)

  const expand = options.expand || ''
  const key = `${this.namespace}:${this.resource}:${doc[this.pk]}`

  if (_.isObject(doc) && !_.isPlainObject(doc)) doc = doc.toObject()

  return this.cacher.set(key, serialize(doc))
    .then(() => expand ? this.expand(doc, expand) : doc)
}

Backfill.prototype.get = function (filter, options) {
  if (!filter[this.pk]) return Promise.resolve(null)

  const expand = options.expand || ''
  const key = `${this.namespace}:${this.resource}:${filter[this.pk]}`

  // TODO: filter select
  // const select = options.select || ''

  // if empty:
  //  if self (resource) return empty, caller outside should get it  from db
  //   else get from other ms

  return this.cacher.get(key)
    .then(doc => {
      let { err, value} = deserialize(doc)

      if (err) return null
      else return value && expand ? this.expand(value, expand) : value
    })
}

Backfill.prototype.unset = function (filter) {
  if (!filter[this.pk]) return Promise.resolve(null)
  return this.cacher.del(`${this.namespace}:${this.resource}:${filter[this.pk]}`)
}

Backfill.prototype.expand = function (doc, expand) {
  const expands = expand
    .replace(/\s/g, '')
    .split(/,/)

  // -- neutralizer
  const neutralized = []

  expands.sort().reverse().forEach(subj => {
    let match = false
    for (let i = 0; i < neutralized.length; i++) {
      let xtarSubj = subj.replace(/\*/g, '_')
      if ((new RegExp(`^${xtarSubj}\.|^${xtarSubj}$`)).test(neutralized[i].replace(/\*/g, '_'))) {
        match = true; break
      }
    }

    if (!match) neutralized.push(subj)
  })

  // -- mapper
  let emap = {}

  neutralized.forEach(key => {
    if (this.refmap[key]) { // local key (self model prop)
      emap[key] = Object.assign({ expand: [] }, this.refmap[key])
    } else {
      let chunks = key.split('.') // extract local key, then capture remaining foreign keys
      let lkey = chunks.shift()

      if (this.refmap[lkey]) {
        if (emap[lkey]) {
          emap[lkey].expand.push(chunks.join('.'))
        } else {
          emap[lkey] = Object.assign({ expand: [chunks.join('.')] }, this.refmap[lkey])
        }
      }
    }
  })

  // -- expander

  return Promise.each(Object.keys(emap), key => {
    const map = emap[key]
    const delegateExpand = map.expand.join()

    // TODO: handle aa*bb*cc*dd*{n}

    if (/\*/.test(key)) {
      let chunks = key.split('*')
      let arrField = chunks.shift()
      let itemProp = chunks.shift()

      return Promise.each(doc[arrField], item => {
        if (Array.isArray(item)) {
          // TODO: handle plane arr
          console.log('TODO: unhandled plane arr!')
        } else if (typeof item === 'object') {
          return Promise.map(doc[arrField], id => this.broker.call(`${map.ref}.${this.fetchName}`, item[itemProp], delegateExpand))
            .then(ret => { doc[arrField] = ret })
            .catch(err => console.warn(err))
        }
      })
    }

    if (!doc[key]) {
      return
    }

    if (map.array) {
      return Promise.map(doc[key], id => this.broker.call(`${map.ref}.${this.fetchName}`, id, delegateExpand))
        .then(ret => { doc[key] = ret })
        .catch(err => console.warn(err))
    } else {
      return this.broker.call(`${map.ref}.${this.fetchName}`, doc[key], delegateExpand)
        .then(ret => { doc[key] = ret })
        .catch(err => console.warn(err))
    }
  })
  .then(() => doc)
}

module.exports = Backfill
