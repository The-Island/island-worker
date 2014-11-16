/*
 * index.js: Island Worker boilerplate
 *
 */

var http = require('http');
var Step = require('step');
var _ = require('underscore');
_.mixin(require('underscore.string'));
var db = require('mongish');
var zmq = require('zmq');

var start = exports.start = function (opts, cb) {
  cb = cb || function(){};

  // Router-Dealer pattern for two-way communication.
  if (opts.routerPort) {

    var frontPort = opts.routerPort;
    var backPort = 'ipc:///tmp/queue';
    
    // Create a router and dealer for client messages.
    var frontSock = zmq.socket('router');
    var backSock = zmq.socket('dealer');
    frontSock.identity = 'r' + process.pid;
    backSock.identity = 'd' + process.pid;

    // Router handling.
    frontSock.bindSync(frontPort);
    frontSock.on('message', function () {
      
      // Inform server.
      backSock.send(Array.prototype.slice.call(arguments));
    });

    // Dealer handling.
    backSock.bindSync(backPort);
    backSock.on('message', function (id, del, data) {

      // Send message receipt to appropriate client.
      frontSock.send(Array.prototype.slice.call(arguments));
    });

    // Create a queue for client messages.
    var queue = zmq.socket('rep');
    queue.identity = 'q' + process.pid;

    // Queue handling.
    queue.on('message', function (data) {
      data = JSON.parse(data.toString());
      var res = {__cb: data.__cb};

      function _cb() {

        // Inform dealer.
        queue.send(JSON.stringify(res));
      }
      if (opts.onSocketMessage) {
        opts.onSocketMessage(data.msg, function (err, msg) {
          if (err) {
            res.error = err;
          } else if (_.isObject(msg)) {
            res.msg = msg;
          } else {
            res.msg = {result: msg};
          }
          _cb();
        });
      } else {
        _cb()
      }
    });

    // Connect to router.
    queue.connect(backPort, function (err) {
      if (err) throw err;
    });
  }

  // XPub-XSub pattern for brokering messages.
  if (opts.pubPort && opts.subPort) {
    var hwm = 1000;
    var verbose = 0;

    // The xsub listener is where pubs connect to
    var subSock = zmq.socket('xsub');
    subSock.identity = 'subscriber' + process.pid;
    subSock.bindSync(opts.subPort);

    // The xpub listener is where subs connect to
    var pubSock = zmq.socket('xpub');
    pubSock.identity = 'publisher' + process.pid;
    pubSock.setsockopt(zmq.ZMQ_SNDHWM, hwm);
    // By default xpub only signals new subscriptions
    // Settings it to verbose = 1 , will signal on every new subscribe
    pubSock.setsockopt(zmq.ZMQ_XPUB_VERBOSE, verbose);
    pubSock.bindSync(opts.pubPort);

    // When we receive data on subSock, it means someone is publishing
    subSock.on('message', function(data) {
      // We just relay it to the pubSock, so subscribers can receive it
     pubSock.send(data);
    });

    // When Pubsock receives a message, it subscribe requests
    pubSock.on('message', function(data, bla) {
      // The data is a slow Buffer
      // The first byte is the subscribe (1) /unsubscribe flag (0)
      var type = data[0]===0 ? 'unsubscribe': 'subscribe';
      // The channel name is the rest of the buffer
      var channel = data.slice(1).toString();
      console.log(type + ':' + channel);
      // We send it to subSock, so it knows to what channels to listen to
      subSock.send(data);
    });
  }

  // Open a route for health check.
  http.createServer(function (req, res) {
    res.writeHead(200, {'Content-Type': 'text/plain'});
    res.end();
  }).listen(opts.port);

  Step(
    function () {

      // Open DB connection if URI is present in opts.
      if (opts.mongoURI) {
        new db.Connection(opts.mongoURI, {ensureIndexes: opts.indexDb}, this);
      } else {
        this();
      }
    },
    function (err, connection) {
      if (err) return this(err);

      // Init collections.
      if (!connection || _.size(opts.collections) === 0) {
        return this();
      }
      _.each(opts.collections, _.bind(function (c, name) {
        connection.add(name, c, this.parallel());
      }, this));
    },
    function (err) {
      if (err) return cb(err);
      cb();
      if (opts.onReady) {
        opts.onReady(db);
      }
    }
  );
}
