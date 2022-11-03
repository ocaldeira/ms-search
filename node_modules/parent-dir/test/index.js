'use strict';

var assert = require('assert')
  , fs = require('fs')
  , path = require('path')
  , dir = __dirname
  , main = path.join(dir, '/../index.js')
  , ase = assert.strictEqual

describe('Parent.js', function() {
  it('works', function() {
    var copy = path.join(dir, '/module.js/node_modules/parent.js/index.js')


    var parent = require('../index')

    // console.log('parent: ', parent)
    // console.log('mocha: ', path.join(__dirname, '/../node_modules/mocha/bin'))

    
    ase(parent, path.join(__dirname, '/..'))

    fs.writeFileSync(copy, fs.readFileSync(main))
    delete require.cache[require.resolve('../index')]

    var module = require('./module.js/index')
    ase(module, __dirname)

    // delete require.cache[require.resolve('../index')]
    try { fs.unlinkSync(copy) } catch (e) {}
  })
})
