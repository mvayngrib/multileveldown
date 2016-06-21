var levelup = require('levelup')
var duplexify = require('duplexify')
var leveldown = require('./leveldown')

module.exports = function (opts) {
  if (!opts) opts = {}

  var down
  opts.db = createLeveldown
  opts.onflush = onflush
  var db = levelup('multileveldown', opts)
  db.createRpcStream = db.connect = connect

  var eventCache = {
    put: [],
    del: [],
    batch: []
  }

  var on = db.on
  db.on = function (event, cb) {
    if (event in eventCache) {
      if (down) down.on(event, cb)
      else eventCache[event].push(cb)
    } else {
      return on.apply(this, arguments)
    }
  }

  var removeListener = db.removeListener
  db.removeListener = function (event, cb) {
    if (event in eventCache) {
      if (down) {
        down.removeListener(event, cb)
      } else {
        eventCache[event] = eventCache[event].filter(function (args) {
          return args[1] === cb
        })
      }
    } else {
      return removeListener.apply(this, arguments)
    }
  }

  return db

  function createLeveldown (path) {
    down = leveldown(path, opts)
    return down
  }

  function onflush () {
    db.emit('flush')
  }

  function connect (opts) {
    if (down) return down.createRpcStream(opts, null)

    var proxy = duplexify()
    db.open(function () {
      down.createRpcStream(opts, proxy)
      for (var event in eventCache) {
        for (var i = 0; i < eventCache[event].length; i++) {
          down.on(event, eventCache[event][i])
        }

        eventCache[event].length = 0
      }
    })

    return proxy
  }
}
