var tape = require('tape')
var memdown = require('memdown')
var concat = require('concat-stream')
var levelup = require('levelup')
var multileveldown = require('../')

tape('get', function (t) {
  var db = levelup('no-location', {db: memdown})
  var stream = multileveldown.server(db)
  var client = multileveldown.client()

  stream.pipe(client.createRpcStream()).pipe(stream)

  db.put('hello', 'world', function () {
    client.get('hello', function (err, value) {
      t.error(err, 'no err')
      t.same(value, 'world')
      t.end()
    })
  })
})

tape('put', function (t) {
  var db = levelup('no-location', {db: memdown})
  var stream = multileveldown.server(db)
  var client = multileveldown.client()

  stream.pipe(client.createRpcStream()).pipe(stream)

  client.put('hello', 'world', function (err) {
    t.error(err, 'no err')
    client.get('hello', function (err, value) {
      t.error(err, 'no err')
      t.same(value, 'world')
      t.end()
    })
  })
})

tape('readonly', function (t) {
  var db = levelup('no-location', {db: memdown})

  db.put('hello', 'verden')

  var stream = multileveldown.server(db, {readonly: true})
  var client = multileveldown.client()

  stream.pipe(client.createRpcStream()).pipe(stream)

  client.put('hello', 'world', function (err) {
    t.ok(err, 'put failed')
    client.get('hello', function (err, value) {
      t.error(err, 'no err')
      t.same(value, 'verden', 'old value')
      t.end()
    })
  })
})

tape('del', function (t) {
  var db = levelup('no-location', {db: memdown})
  var stream = multileveldown.server(db)
  var client = multileveldown.client()

  stream.pipe(client.createRpcStream()).pipe(stream)

  client.put('hello', 'world', function (err) {
    t.error(err, 'no err')
    client.del('hello', function (err) {
      t.error(err, 'no err')
      client.get('hello', function (err) {
        t.ok(err, 'had error')
        t.ok(err.notFound, 'not found err')
        t.end()
      })
    })
  })
})

tape('batch', function (t) {
  var db = levelup('no-location', {db: memdown})
  var stream = multileveldown.server(db)
  var client = multileveldown.client()

  stream.pipe(client.createRpcStream()).pipe(stream)

  client.batch([{type: 'put', key: 'hello', value: 'world'}, {type: 'put', key: 'hej', value: 'verden'}], function (err) {
    t.error(err, 'no err')
    client.get('hello', function (err, value) {
      t.error(err, 'no err')
      t.same(value, 'world')
      client.get('hej', function (err, value) {
        t.error(err, 'no err')
        t.same(value, 'verden')
        t.end()
      })
    })
  })
})

tape('read stream', function (t) {
  var db = levelup('no-location', {db: memdown})
  var stream = multileveldown.server(db)
  var client = multileveldown.client()

  stream.pipe(client.createRpcStream()).pipe(stream)

  client.batch([{type: 'put', key: 'hello', value: 'world'}, {type: 'put', key: 'hej', value: 'verden'}], function (err) {
    t.error(err, 'no err')
    var rs = client.createReadStream()
    rs.pipe(concat(function (datas) {
      t.same(datas.length, 2)
      t.same(datas[0], {key: 'hej', value: 'verden'})
      t.same(datas[1], {key: 'hello', value: 'world'})
      t.end()
    }))
  })
})

tape('read stream (gt)', function (t) {
  var db = levelup('no-location', {db: memdown})
  var stream = multileveldown.server(db)
  var client = multileveldown.client()

  stream.pipe(client.createRpcStream()).pipe(stream)

  client.batch([{type: 'put', key: 'hello', value: 'world'}, {type: 'put', key: 'hej', value: 'verden'}], function (err) {
    t.error(err, 'no err')
    var rs = client.createReadStream({gt: 'hej'})
    rs.pipe(concat(function (datas) {
      t.same(datas.length, 1)
      t.same(datas[0], {key: 'hello', value: 'world'})
      t.end()
    }))
  })
})

tape('events', function (t) {
  var db = levelup('no-location', {db: memdown, valueEncoding: 'binary'})
  var stream = multileveldown.server(db)
  var encOpts = {
    keyEncoding: 'utf8',
    valueEncoding: 'utf8'
  }

  var client1 = multileveldown.client(encOpts)
  var client2 = multileveldown.client(encOpts)

  stream.pipe(client1.createRpcStream()).pipe(stream)
  stream.pipe(client2.createRpcStream()).pipe(stream)

  // emits on op by another client
  client1.put('hello', 'world')
  client2.on('put', listener)
  var called = false

  function listener (value) {
    t.equal(value, 'world')
    client2.get('hello', function (err, value) {
      t.equal(called, false)
      called = true
      t.error(err, 'no err')
      t.same(value, 'world')
      client2.removeListener('put', listener)

      // emits on ops by same client
      client1.del('hello', function (err) {
        t.error(err)
      })

      client1.on('del', function (key) {
        t.equal(key, 'hello')

        var batch = [
          { type: 'put', key: 'hello', value: 'world' },
          { type: 'put', key: 'hello1', value: 'world1' }
        ]

        client1.batch(batch, function (err) {
          t.error(err, 'no err')
          t.end()
        })

        client2.on('batch', function (data) {
          t.same(data, batch)
          batch.forEach(function (op) {
            op.type = 'del'
          })

          db.batch(batch, function (err) {
            t.error(err, 'no err')
          })
        })
      })
    })
  }
})
