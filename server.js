var EventEmitter = require('events').EventEmitter
var lpstream = require('length-prefixed-stream')
var eos = require('end-of-stream')
var duplexify = require('duplexify')
var messages = require('./messages')
var utils = require('./utils')

var CODERS = [
  messages.Get,
  messages.Put,
  messages.Delete,
  messages.Batch,
  messages.Iterator,
  messages.Subscribe,
  messages.Unsubscribe
]

module.exports = function (db, opts) {
  var readonly = !!(opts && opts.readonly)
  var decode = lpstream.decode()
  var encode = lpstream.encode()
  var stream = duplexify(decode, encode)

  if (db.isOpen()) ready()
  else db.open(ready)

  return stream

  function ready () {
    var down = db.db
    var iterators = []
    var handlers = {
      put: [],
      del: [],
      batch: []
    }

    eos(stream, function () {
      while (iterators.length) {
        var next = iterators.shift()
        if (next) next.end()
      }
    })

    decode.on('data', function (data) {
      if (!data.length) return
      var tag = data[0]
      if (tag >= CODERS.length) return

      var dec = CODERS[tag]
      try {
        var req = dec.decode(data, 1)
      } catch (err) {
        return
      }

      if (readonly) {
        switch (tag) {
          case 0: return onget(req)
          case 1: return onreadonly(req)
          case 2: return onreadonly(req)
          case 3: return onreadonly(req)
          case 4: return oniterator(req)
          case 5: return onsubscribe(req)
          case 6: return onunsubscribe(req)
        }
      } else {
        switch (tag) {
          case 0: return onget(req)
          case 1: return onput(req)
          case 2: return ondel(req)
          case 3: return onbatch(req)
          case 4: return oniterator(req)
          case 5: return onsubscribe(req)
          case 6: return onunsubscribe(req)
        }
      }
    })

    function callback (id, err, value) {
      var msg = {id: id, error: err && err.message, value: value}
      var buf = new Buffer(messages.Callback.encodingLength(msg) + 1)
      buf[0] = 0
      messages.Callback.encode(msg, buf, 1)
      encode.write(buf)
    }

    function emit (event, data) {
      var eHandlers = handlers[event]
      for (var i = 0; i < eHandlers.length; i++) {
        eHandlers[i](data)
      }
    }

    function onput (req) {
      down.put(req.key, req.value, function (err) {
        callback(req.id, err, null)
        if (!err) emit('put', { key: req.key, value: req.value })
      })
    }

    function onget (req) {
      down.get(req.key, function (err, value) {
        callback(req.id, err, value)
      })
    }

    function ondel (req) {
      down.del(req.key, function (err) {
        callback(req.id, err)
        if (!err) emit('del', { key: req.key })
      })
    }

    function onreadonly (req) {
      callback(req.id, new Error('Database is readonly'))
    }

    function onbatch (req) {
      down.batch(req.ops, function (err) {
        callback(req.id, err)
        if (!err) emit('batch', { ops: req.ops })
      })
    }

    function oniterator (req) {
      while (iterators.length < req.id) iterators.push(null)

      var prev = iterators[req.id]
      if (!prev) prev = iterators[req.id] = new Iterator(down, req, encode)

      if (!req.batch) {
        iterators[req.id] = null
        prev.end()
      } else {
        prev.batch = req.batch
        prev.next()
      }
    }

    function onsubscribe (req) {
      var tag = utils.getEventTag(req.event)
      var enc = CODERS[tag]
      var handler = function (data) {
        data.id = 0 // id doesn't matter
        var buf = new Buffer(enc.encodingLength(data) + 1)
        buf[0] = tag
        enc.encode(data, buf, 1)
        callback(req.id, null, buf)
      }

      handler._multilevelId = req.id
      handlers[req.event].push(handler)
    }

    function onunsubscribe (req) {
      // inefficient but in the end, pretty cheap
      for (var event in handlers) {
        handlers[event] = handlers[event].filter(function (handler) {
          return handler._multilevelId !== req.handler
        })
      }

      callback(req.id, null, null)
    }
  }
}

function Iterator (down, req, encode) {
  var self = this

  this.batch = req.batch || 0

  this._iterator = down.iterator(req.options)
  this._encode = encode
  this._send = send
  this._nexting = false
  this._first = true
  this._ended = false
  this._data = {
    id: req.id,
    error: null,
    key: null,
    value: null
  }

  function send (err, key, value) {
    self._nexting = false
    self._data.error = err && err.message
    self._data.key = key
    self._data.value = value
    self.batch--
    var buf = new Buffer(messages.IteratorData.encodingLength(self._data) + 1)
    buf[0] = 1
    messages.IteratorData.encode(self._data, buf, 1)
    encode.write(buf)
    self.next()
  }
}

Iterator.prototype.next = function () {
  if (this._nexting || this._ended) return
  if (!this._first && (!this.batch || this._data.error || (!this._data.key && !this._data.value))) return
  this._first = false
  this._nexting = true
  this._iterator.next(this._send)
}

Iterator.prototype.end = function () {
  this._ended = true
  this._iterator.end(noop)
}

function noop () {}
