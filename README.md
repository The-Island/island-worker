###This is boilerplate for an Island worker.

Installation:

```npm install git+https://island-ebs:i51andghebs@github.com/Islandio/island-worker.git#develop``` (read-only)

Example usage:

```
// start.js

var worker = require('island-worker');

worker.start({
  port: 1337,
  socketPort: 3000,
  onReady: function () {
    console.log('i am ready, mkay')
  },
  onSocketMessage: function (msg, cb) {
	/* ... do work ... */
    cb(null, {success: 1});
  }
}, function (err) { if (err) throw err; });
```