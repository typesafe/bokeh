mongo = require "mongodb"
Db = mongo.Db
Server = mongo.Server

module.exports = class Mongo

  constructor: (@options) ->
    @startupQueue = []
    @db = new Db(@options.db || 'bokeh', new Server @options.host || 'localhost' , @options.port || 27017, {}, { safe: true });

  write: (key, data, callback) ->
    @_getCollection (collection) ->
      collection.save { _id: key, data: data }, callback

  read: (key, callback) ->
    @_getCollection (collection) ->
      collection.findOne { _id: key }, (err, doc) ->
        callback err, doc.data

  delete: (key, callback) ->
    @_getCollection (collection) ->
      collection.remove { _id: key }, callback

  keys: (callback) ->
    @_getCollection (collection) ->
      collection.find({}, { _id : 1 }).toArray (err, items) ->
        callback err, items.map (doc)->
          doc._id

  # Since there's no explicit `Store.open` to handle the store's connectivity, we use this function
  # to make sure db.open is called only once.
  _ensureOpen: (callback) ->
    thiz = this
    return callback(@db) if @open
    
    @startupQueue.push callback
    
    return if @opening

    @opening = true

    @db.open (err, db)->
      thiz.open = true
      thiz.opening = false
      thiz.startupQueue.forEach (callback)->
        callback(db)

  _getCollection: (callback) ->
    b = @options.bucket
    @_ensureOpen (db) ->
      db.collection b, (err, collection) ->
        throw err if err
        callback(collection)