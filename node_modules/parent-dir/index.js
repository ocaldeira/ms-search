'use strict';

/*!
 * Module dependencies.
 */

var path = require('path')
  , debug = require('debug')('parent-dir')
  , nm = '/node_modules'

/**
 * Find the parent app directory by looking up the directory
 * tree until there is is no `package.json` file found or there is 
 * no `node_modules` directory found.
 *
 * This task is syncronous as it should only ever be ran once at 
 * app startup, then cached by `require`
 *
 * @returns {String} directory
 */

module.exports = (function() {
  debug('walking module parent tree')

  var dir = module
    , p

  // This is the simplest way, if included as a node module, the parent directory 
  // is generally the first dir to have a `node_modules` dir
  if (!!~dir.id.indexOf(nm)) {
    p = dir.id.split(nm)[0]

    debug('simple path: `%s`', p)
  }

  // We dont want to go all the way up the chain, in the case we are running
  // in a test context such as `mocha`, move up the chain if the current dir 
  // is within `node_modules`, or if not, that the parent directory isnt moving
  // *into* the `node_modules` dir.
  while (dir.parent 
    && (
      !!~dir.id.indexOf(nm) 
      || !~dir.parent.id.indexOf(nm)
    ) && (
      // Dont move down further into dirs
      !~path.dirname(dir.parent.id).indexOf(path.dirname(dir.id))
    )
  ) {
    
    debug('walking `%s`', dir.id)
    dir = dir.parent
  }

  // If the module path contains the simple path found above, dont use it
  var p2 = path.dirname(dir.id)
  if (!~p2.indexOf(p)) p = p2
  p = path.resolve(p)

  debug('found `%s`', p)
  return p
})()
