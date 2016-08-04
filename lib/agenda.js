/*  Code forked from https://github.com/rschmukler/agenda
 *
 */

var Job = require('./job.js'),
  humanInterval = require('human-interval'),
  utils = require('util'),
  Emitter = require('events').EventEmitter;
var urlParser = require('url');

var rethinkdbdash = require('rethinkdbdash');
var r;


var Agenda = module.exports = function (config, cb) {
  if (!(this instanceof Agenda)) {
    return new Agenda(config);
  }

  config = config ? config : {};
  this._name = config.name;
  this._processEvery = humanInterval(config.processEvery) || humanInterval('5 seconds');
  this._defaultConcurrency = config.defaultConcurrency || 5;
  this._maxConcurrency = config.maxConcurrency || 20;
  this._defaultLockLimit = config.defaultLockLimit || 0;
  this._lockLimit = config.lockLimit || 0;
  this._definitions = {};
  this._runningJobs = [];
  this._lockedJobs = [];
  this._jobQueue = [];
  this._defaultLockLifetime = config.defaultLockLifetime || 10 * 60 * 1000; //10 minute default lockLifetime
    
  this._isLockingOnTheFly = false;
  this._jobsToLock = [];

  if (config.rethinkdb) {
    this.rethink(config.rethinkdb, config.db ? config.db.table : undefined, cb);
  } else if (config.db) {
    this.database(config.db.address, config.db.table, config.db.options, cb);
  }
};

utils.inherits(Agenda, Emitter); // Job uses emit() to fire job events client can use.

// Configuration Methods

Agenda.prototype.rethink = function (rdb, table, cb) {
  table = table || 'agendaJobs';
  r = this._rdb = rdb;
  this.dbInit(table, cb);
  return this;
};

/** Connect to the spec'd RethinkDB server and database.
 *  Notes:
 *    - If `url` inludes auth details then `options` must specify: { 'uri_decode_auth': true }. This does Auth on the specified
 *      database, not the Admin database. If you are using Auth on the Admin DB and not on the Agenda DB, then you need to
 *      authenticate against the Admin DB and then pass the MongoDB instance in to the Constructor or use Agenda.mongo().
 *    - If your app already has a MongoDB connection then use that. ie. specify config.mongo in the Constructor or use Agenda.mongo().
 */
Agenda.prototype.database = function (url, table, options, cb) {

  if (!url.match(/^http:\/\/.*/)) {
    url = 'http://' + url;
  }

  var parsed = urlParser.parse(url);

  table = table || 'agendaJobs';
  var db = parsed.path.slice(1);
  options = options || {};
  var self = this;
  r = self._rdb = rethinkdbdash({
    host: parsed.hostname,
    port: parsed.port,
    db: db
  });
  self.dbInit(table, cb);

  return this;
};

/** Setup and initialize the collection used to manage Jobs.
 *  @param collection collection name or undefined for default 'agendaJobs'
 *  NF 20/04/2015
 */
Agenda.prototype.dbInit = function (table, cb) {

  var self = this;
  self._table = r.table(table);
  r.tableCreate(table).run()
    .catch(function (err) {
      if ((err) && (!err.message.match(/Table `.*` already exists/))) {
        self.emit('error', err);
      } else {
        self.emit('ready');
      }
    })
    .finally(function () {
      if (cb) cb(null, self._table);
    });

};

Agenda.prototype.name = function (name) {
  this._name = name;
  return this;
};

Agenda.prototype.processEvery = function (time) {
  this._processEvery = humanInterval(time);
  return this;
};

Agenda.prototype.maxConcurrency = function (num) {
  this._maxConcurrency = num;
  return this;
};

Agenda.prototype.defaultConcurrency = function (num) {
  this._defaultConcurrency = num;
  return this;
};

Agenda.prototype.lockLimit = function (num) {
  this._lockLimit = num;
  return this;
};


Agenda.prototype.defaultLockLimit = function (num) {
  this._defaultLockLimit = num;
  return this;
};

Agenda.prototype.defaultLockLifetime = function (ms) {
  this._defaultLockLifetime = ms;
  return this;
};

// Job Methods
Agenda.prototype.create = function (name, data) {
  var priority = this._definitions[name] ? this._definitions[name].priority : 0;
  var job = new Job({
    name: name,
    data: data,
    type: 'normal',
    priority: priority,
    agenda: this
  });
  return job;
};


/** Find all Jobs matching `query` and pass same back in cb().
 *  refactored. NF 21/04/2015
 */
Agenda.prototype.jobs = function (query, cb) {
  var self = this;
  this._table.filter(query).run(function (error, result) {
    var jobs;
    if (!error) {
      jobs = result.map(createJob.bind(null, self));
    }
    cb(error, jobs);
  });
};


Agenda.prototype.purge = function (cb) {
  var definedNames = Object.keys(this._definitions);
  this.cancel(function (job) {
    return r.expr(definedNames).contains(job('name')).not();
  }, cb);
};

Agenda.prototype.define = function (name, options, processor) {
  if (!processor) {
    processor = options;
    options = {};
  }
  this._definitions[name] = {
    fn: processor,
    concurrency: options.concurrency || this._defaultConcurrency,
    lockLimit: options.lockLimit || this._defaultLockLimit,
    priority: options.priority || 0,
    lockLifetime: options.lockLifetime || this._defaultLockLifetime,
    running: 0,
    locked: 0
  };
};

Agenda.prototype.every = function (interval, names, data, options, cb) {
  var self = this;

  if (cb === undefined && typeof data === 'function') {
    cb = data;
    data = undefined;
  } else if (cb === undefined && typeof options === 'function') {
    cb = options;
    options = undefined;
  }

  if (typeof names === 'string' || names instanceof String) {
    return createJob(interval, names, data, options, cb);
  } else if (Array.isArray(names)) {
    return createJobs(interval, names, data, options, cb);
  }

  function createJob(interval, name, data, options, cb) {
    var job = self.create(name, data);
    job.attrs.type = 'single';
    job.repeatEvery(interval, options);
    job.computeNextRunAt();
    job.save(cb);
    return job;
  }

  function createJobs(interval, names, data, options, cb) {
    var results = [];
    var pending = names.length;
    var errored = false;
    return names.map(function (name, i) {
      return createJob(interval, name, data, options, function (err, result) {
        if (err) {
          if (!errored) cb(err);
          errored = true;
          return;
        }
        results[i] = result;
        if (--pending === 0 && cb) cb(null, results);
      });
    });

  }
};

Agenda.prototype.schedule = function (when, names, data, cb) {
  var self = this;

  if (cb === undefined && typeof data === 'function') {
    cb = data;
    data = undefined;
  }

  if (typeof names === 'string' || names instanceof String) {
    return createJob(when, names, data, cb);
  } else if (Array.isArray(names)) {
    return createJobs(when, names, data, cb);
  }


  function createJob(when, name, data, cb) {
    var job = self.create(name, data);
    job.schedule(when);
    job.save(cb);
    return job;
  }

  function createJobs(when, names, data, cb) {
    var results = [];
    var pending = names.length;
    var errored = false;
    return names.map(function (name, i) {
      return createJob(when, name, data, function (err, result) {
        if (err) {
          if (!errored) cb(err);
          errored = true;
          return;
        }
        results[i] = result;
        if (--pending === 0 && cb) cb(null, results);
      });
    });
  }
};

Agenda.prototype.now = function (name, data, cb) {
  if (!cb && typeof data === 'function') {
    cb = data;
    data = undefined;
  }
  var job = this.create(name, data);
  job.schedule(new Date());
  job.save(cb);
  return job;
};


/** Cancels any jobs matching the passed mongodb query, and removes them from the database.
 *  @param query mongo db query
 *  @param cb callback( error, numRemoved )
 *
 *  @caller client code, Agenda.purge(), Job.remove()
 */
Agenda.prototype.cancel = function (query, cb) {
  this._table.filter(query).delete().run(function (error, result) {
    if (cb) {
      cb(error, result && result.deleted ? result.deleted : undefined);
    }
  });

};

Agenda.prototype.saveJob = function (job, cb) {
  var fn = cb,
    self = this;



  var props = job.toJSON();
  var id = job.attrs.id;
  var unique = job.attrs.unique;
  var uniqueOpts = job.attrs.uniqueOpts;

  delete props.id;
  delete props.unique;
  delete props.uniqueOpts;

  props.lastModifiedBy = this._name;

  var now = new Date(),
    protect = {},
    update = props;


  if (id) {
    this._table.filter({
      id: id
    }).update(update, {
      returnChanges: true
    }).run(processDbResult);
  } else if (props.type === 'single') {
    if (props.nextRunAt && props.nextRunAt <= now) {
      protect.nextRunAt = props.nextRunAt;
      delete props.nextRunAt;
    }
    // Try an upsert.
    self._table.filter({
      name: props.name,
      type: 'single'
    }).run().then(function (results) {
      if (results.length) {

        self._table.filter({
          name: props.name,
          type: 'single'
        }).update(update, {
          returnChanges: true
        }).run(processDbResult);
      } else {
        self._table.insert(Object.assign(update, protect), {
          returnChanges: true
        }).run(processDbResult);
      }
    });
  } else if (unique) {
    var query = job.attrs.unique;
    query.name = props.name;
    if (uniqueOpts && uniqueOpts.insertOnly) {
      //insert only if doesnt exist 
      self._table.filter(query).run().then(function (results) {
        if (results.length === 0) {
          self._table.insert(update, {
            returnChanges: true
          }).run(processDbResult);
        } else {
          processDbResult(null, { changes: [{ 'new_val': results }] });
        }
      });
    } else {
      self._table.filter(query).run().then(function (results) {
        if (results.length) {
          self._table.filter(query).update(update, {
            returnChanges: true
          }).run(processDbResult);
        } else {
          self._table.insert(update, {
            returnChanges: true
          }).run(processDbResult);
        }
      });

    }
  } else {
    this._table.insert(props, {
      returnChanges: true
    }).run(processDbResult); // NF updated 22/04/2015
  }

  function processDbResult(err, result) {
    if (err) {
      if (fn) {
        return fn(err);
      } else {
        throw err;
      }
    } else if (result) {
      var res = result.changes ? result.changes : result;
      if (res) {
        if (Array.isArray(res) && res.length) {
          res = res[0]['new_val'];

          job.attrs.id = res.id;
          job.attrs.nextRunAt = res.nextRunAt;

          if (job.attrs.nextRunAt && job.attrs.nextRunAt < self._nextScanAt) {
            processJobs.call(self, job);
          }
        }


      }
    }

    if (fn) {
      fn(null, job);
    }
  }
};

// Job Flow Methods

Agenda.prototype.start = function () {
  if (!this._processInterval) {
    this._processInterval = setInterval(processJobs.bind(this), this._processEvery);
    process.nextTick(processJobs.bind(this));
  }
};

Agenda.prototype.stop = function (cb) {
  cb = cb || function () { };
  clearInterval(this._processInterval);
  this._processInterval = undefined;
  this._unlockJobs(cb);
};

/**
 * Find and lock jobs
 * @param {String} jobName
 * @param {Function} cb
 * @protected
 *  @caller jobQueueFilling() only
 */
Agenda.prototype._findAndLockNextJob = function (jobName, definition, cb) {
  var self = this,
    now = new Date(),
    lockDeadline = new Date(Date.now().valueOf() - definition.lockLifetime);

  // Don't try and access Rethink Db if we've lost connection to it. Also see clibu_automation.js db.on.close code. NF 29/04/2015
  // Trying to resolve crash on Dev PC when it resumes from sleep.
  if (r.getPoolMaster().getLength() === 0) {
    cb(new Error('No RethinkDB Connection'));
  } else {

    this._table
      .filter(function (job) {
        return job('name').eq(jobName)
          .and(job('disabled').ne(true)).default(true)
          .and(job('nextRunAt').ne(null))
          .and(job('nextRunAt').le(Date(self._nextScanAt))).default(true)
          .and(job('lockedAt').eq(null).default(true).or(job('lockedAt').le(lockDeadline).default(true)));
      })
      .limit(1)
      .update({
        lockedAt: now
      }, {
          returnChanges: true
        }).run(function (error, result) {
          var jobs;
          if (!error && result.changes && result.changes[0] && result.changes[0]['new_val']) {
            jobs = createJob(self, result.changes[0]['new_val']);
          }
          cb(error, jobs);
        });
  }
};


/**
 * Create Job object from data
 * @param {Object} agenda
 * @param {Object} jobData
 * @return {Job}
 * @private
 */
function createJob(agenda, jobData) {
  jobData.agenda = agenda;
  return new Job(jobData);
}

// Refactored to Agenda method. NF 22/04/2015
// @caller Agenda.stop() only. Could be moved into stop(). NF
Agenda.prototype._unlockJobs = function (done) {


  var jobIds = this._lockedJobs.map(function (job) {
    return job.attrs.id;
  });
  this._table.filter(function (doc) {
    return r.expr(jobIds).contains(doc('id'));
  }).update({
    lockedAt: null
  }).run(done);

};


function processJobs(extraJob) {
  if (!this._processInterval) {
    return;
  }

  var definitions = this._definitions,
    jobName,
    jobQueue = this._jobQueue,
    self = this;


  if (!extraJob) {
    for (jobName in definitions) {
      jobQueueFilling(jobName);
    }
  } else if (definitions[extraJob.attrs.name]) {
    self._jobsToLock.push(extraJob);
    lockOnTheFly();
  }


  function shouldLock(name) {
    var shouldLock = true;
    var jobDefinition = definitions[name];

    if (self._lockLimit && self._lockLimit <= self._lockedJobs.length) {
      shouldLock = false;
    }

    if (jobDefinition.lockLimit && jobDefinition.lockLimit <= jobDefinition.locked) {
      shouldLock = false;
    }
    return shouldLock;
  }

  function enqueueJobs(jobs) {
    if (!Array.isArray(jobs)) {
      jobs = [jobs];
    }

    jobs.forEach(function (job) {
      var jobIndex = 0;

      jobQueue.forEach(function (queuedJob) {
        if (queuedJob.attrs.priority < job.attrs.priority) {
          jobIndex++;
        }
      });

      jobQueue.splice(jobIndex, 0, job);
    });
  }

  function lockOnTheFly() {
    if (self._isLockingOnTheFly) {
      return;
    }

    if (!self._jobsToLock.length) {
      self._isLockingOnTheFly = false;
      return;
    }

    self._isLockingOnTheFly = true;

    var now = new Date();
    var job = self._jobsToLock.pop();
        
    // If locking limits have been hit, stop locking on the fly.
    // Jobs that were waiting to be locked will be picked up during a 
    // future locking interval.
    if (!shouldLock(job.attrs.name)) {
      self._jobsToLock = [];
      self._isLockingOnTheFly = false;
      return;
    }

    self._table.filter({
      id: extraJob.attrs.id,
      lockedAt: null,
      disabled: {
        ne: true
      }
    }, {
        default: false
      }).update({
        lockedAt: now
      }, {
          returnChanges: true
        }).run(function (err, resp) {
          if (resp.changes && resp.changes[0]['new_val']) {
            var job = createJob(self, resp.changes[0]['new_val']);

            self._lockedJobs.push(job);
            definitions[job.attrs.name].locked++;

            enqueueJobs(job);
            jobProcessing();
          }
          self._isLockingOnTheFly = false;
          lockOnTheFly();
        });
  }

  function jobQueueFilling(name) {
    if (!shouldLock(name)) {
      return;
    }

    var now = new Date();
    self._nextScanAt = new Date(now.valueOf() + self._processEvery);
    self._findAndLockNextJob(name, definitions[name], function (err, job) {
      if (err) {
        throw err;
      }

      if (job) {
        self._lockedJobs.push(job);
        definitions[job.attrs.name].locked++;

        enqueueJobs(job);
        jobQueueFilling(name);
        jobProcessing();
      }
    });
  }

  function jobProcessing() {

    if (!jobQueue.length) {
      return;
    }

    var now = new Date();

    var job = jobQueue.pop(),
      name = job.attrs.name,
      jobDefinition = definitions[name];

    if (job.attrs.nextRunAt < now) {
      runOrRetry();
    } else {
      setTimeout(runOrRetry, job.attrs.nextRunAt - now);
    }

    function runOrRetry() {
      if (self._processInterval) {
        if (jobDefinition.concurrency > jobDefinition.running &&
          self._runningJobs.length < self._maxConcurrency) {

          var lockDeadline = new Date(Date.now() - jobDefinition.lockLifetime);
          if (job.attrs.lockedAt < lockDeadline) {
            // Drop expired job
            self._lockedJobs.splice(self._lockedJobs.indexOf(job), 1);
            jobDefinition.locked--;
            jobProcessing();
            return;
          }

          self._runningJobs.push(job);
          jobDefinition.running++;
          job.run(processJobResult);
          jobProcessing();
        } else {
          // Put on top to run ASAP
          jobQueue.push(job);
        }
      }
    }
  }

  function processJobResult(err, job) {
    var name = job.attrs.name;

    self._runningJobs.splice(self._runningJobs.indexOf(job), 1);
    definitions[name].running--;

    //Assigning the last completed job for a specific job.
    definitions[name].locked--;

    jobProcessing();
  }
}
