'use strict'

const _ = require('lodash')
const Backfill = require('./backfill')

class CrudsAbstract {
  static _init (options) {
    this.backfill = new Backfill({
      broker: options.broker,
      cacher: options.redis,
      schema: this.schema.obj,
      resource: this.collection.name
    })
  }

  static _create (data, expand) {
    let model = new this()

    return model.set(data).save()
      .then(doc => this.backfill.set(doc, { expand }))
  }

  static _read (filter, options) {
    return this.backfill.get(filter, options)
      .then(doc => {
        if (doc) return doc
        return this.findOne(filter).exec()
          .then(doc => this.backfill.set(doc, options))
      })
  }

  static _update (filter, options ) {
    let expand = _.get(options, 'expand', '')
    let update = _.get(options, 'data', {})
    let soft = _.get(options, 'soft', false)

    let query = this.findOne(filter)

    // first  query to capture modified paths
    return query
      .exec()
      .then(doc => {
        if (!doc) return null

        let oldDoc = _.cloneDeep(doc)

        if (typeof soft === 'boolean') {
          if (soft) { // soft array update (append new items to existing array)
            _.each(Object.keys(update), key => {
              if (Array.isArray(doc[key])) {
                doc[key] = Array.from(new Set([...doc[key], ...update[key]]))
              } else {
                doc[key] = update[key]
              }
            })
          } else { // hard array update (overwrite array values with new ones)
            _.each(Object.keys(update), key => {
              doc[key] = update[key]
            })
          }
        } else if (typeof soft === 'object') {
          // mixed! some fields are soft some are not
          _.each(Object.keys(update), key => {
            if (Array.isArray(update[key]) && soft[key]) {
              doc[key] = Array.from(new Set([...doc[key], ...update[key]]))
            } else {
              doc[key] = update[key]
            }
          })
        }

        let changeLog = {}
        let modifieds = doc.modifiedPaths()

        let comparator = function (a, b) {
          return typeof a === 'object'
            ? !_.isEqual(a, b)
            : a !== b
        }

        return doc.save().then(updDoc => {
          if (modifieds.length) {
            _.each(modifieds, field => {
              let updd = updDoc[field]
              let oldd = oldDoc[field]

              if (Array.isArray(updd) && updd.length && !_.isPlainObject(updd[0])) {
                changeLog[field] = {
                  added: updd.filter(a => oldd.every(b => comparator(a, b))),
                  removed: oldd.filter(b => updd.every(a => comparator(b, a)))
                }
              } else {
                changeLog[field] = {
                  from: oldd,
                  to: updd
                }
              }
            })
          }

          return this.backfill.set(doc, { expand })
            .then(cachedDoc => {
              cachedDoc.modifieds = modifieds
              cachedDoc.changeLog = changeLog
              return cachedDoc
            })
        })
      })
  }

  static _delete (filter) {
    return this.findOne(filter)
      .exec()
      .then(doc => {
        if (!doc) return null

        return doc.remove()
          .then(() => this.backfill.unset(doc))
          .then(() => doc)
      })
  }

  static _search (filter, options) {
    let { sort, page, expand, listOnly, docsPerPage: limit } = options
    
    let query = this.find(filter)
    let cquery = this.find(filter)
    
    page = page || 1
    limit = limit || 50
    
    query.limit(limit)
    query.skip(limit * (page > 0 ? page - 1 : 0))

    if (!_.isEmpty(sort)) {
      query.collation({ locale: 'en' })
      query.sort(sort)
    }

    return query.exec()
      .then(docs => {
        return !expand 
          ? Promise.resolve(docs)
          : Promise.map(docs, doc => this.backfill.expand(doc.toObject(), expand)) // TODO: might cause sort problem since it is async
      })
      .then(docs => {
        if (listOnly) return docs

        return cquery.countDocuments()
          .then(count => {
            return {
              totalDocs: count,
              currentPage: page,
              docsPerPage: limit,
              totalPages: Math.ceil(count / limit),
              data: docs
            }
          })
      })
  }

  static _count (filter) {
    let cquery = this.find(filter)
    return cquery.countDocuments()
  }

  static _updateMany () {}
  static _deleteMany () {}
}

module.exports = CrudsAbstract
