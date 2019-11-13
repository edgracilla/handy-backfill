'use strict'

const _ = require('lodash')
const mongoose = require('mongoose')

function Backfill (options) {

  this.broker = options.broker
  this.cacher = options.cacher
  this.resource = options.resource

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
          } else if (prop[0] instanceof mongoose.Schema) {
            crawler(prop[0].obj, refmap, rmkey, true)
          } else if (typeof prop[0] === 'object') {
            crawler(prop[0], refmap, rmkey, true)
          }
        } else if (prop instanceof mongoose.Schema) {
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
  if (!doc) return null

  const key = `${this.resource}:${doc[this.pk]}`
  const expand = options.expand || ''

  return this.cacher.set(key, doc)
    .then(() => expand ? this.expand(doc, expand) : doc)

  // to cache
  // expand if expand
}

Backfill.prototype.get = function (filter, options) {
  if (!filter[this.pk]) return null

  // const key = `${this.resource}:${doc[this.pk]}`
  // const expand = options.expand || ''
  // const select = options.select || ''

  // generate universal standard key scheme
  // get from cache

  // if empty:
  //  if self (resource) return empty, caller outside should get it  from db
  //   else get from other ms

  // filter select

  return null
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
      if ((new RegExp(`^${subj}\.|^${subj}$`)).test(neutralized[i])) {
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

  if (_.isObject(doc) && !_.isPlainObject(doc)) doc = doc.toObject()

  return Promise.each(Object.keys(emap), key => {
    if (!doc[key]) return

    const map = emap[key]
    const delegateExpand = map.expand.join()
    // TODO: handle arr aa.bb*cc

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

Backfill.prototype.unset = function (doc) {

  // delete rsc:_id

  return doc
}

module.exports = Backfill
