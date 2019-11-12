'use strict'

const _ = require('lodash')
const mongoose = require('mongoose')

function Backfill (options) {

  this.broker = options.broker
  this.cacher = options.cacher
  this.schema = options.schema
  this.resource = options.resource

  this.expandables = {}
  this.pk = options.pkey || '_id'
  this.fetchName = options.fetchName || 'read'
  
  this.refmap = {}

  if (this.resource === 'tickets') {

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

    crawler(this.schema, this.refmap)
    // console.log(this.refmap)
  }
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

  if (_.isObject(doc) && !_.isPlainObject(doc)) doc = doc.toObject()

  
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
      emap[key] = Object.assign({}, this.refmap[key])
    } else {
      let chunks = key.split('.') // extract local key, then pass remaining foreign key
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

  console.log(emap)

  return null
}

Backfill.prototype.unset = function (doc) {

  // delete rsc:_id

  return doc
}

module.exports = Backfill
