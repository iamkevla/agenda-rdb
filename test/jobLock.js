/* globals before, describe, it, beforeEach, after, afterEach */
var expect = require('expect.js'),
    path = require('path'),
    cp = require('child_process'),
    Agenda = require(path.join('..', 'index.js')),
    Job = require(path.join('..', 'lib', 'job.js'));

var r = require('./fixtures/connection');



// create agenda instances
var jobs = null;

function clearJobs(done) {
    r.table('agendaJobs').delete().run(done);
}

// Slow timeouts for travis
var jobTimeout = process.env.TRAVIS ? 1500 : 300;


var jobType = 'do work';
var jobProcessor = function(job) {};


function failOnError(err) {
    if (err) {
        throw err;
    }
}


describe('jobLock', function() {


    before(function(done) {

        jobs = new Agenda({
            rethinkdb: r
        }, function(err) {


            setTimeout(function() {
                clearJobs(function() {
                    jobs.define('someJob', jobProcessor);
                    jobs.define('send email', jobProcessor);
                    jobs.define('some job', jobProcessor);
                    jobs.define(jobType, jobProcessor);
                    done();
                });
            }, 50);

        });
    });


  describe('job lock', function() {
      beforeEach(clearJobs);
      it('runs job after a lock has expired', function(done) {
          var startCounter = 0;

          jobs.define('lock job', {
              lockLifetime: 50
          }, function(job, cb) {
              startCounter++;

              if (startCounter !== 1) {
                  expect(startCounter).to.be(2);
                  jobs.stop(done);
              }
          });

          expect(jobs._definitions['lock job'].lockLifetime).to.be(50);

          jobs.defaultConcurrency(100);
          jobs.processEvery(10);
          jobs.every('0.02 seconds', 'lock job');
          jobs.stop();
          jobs.start();
      });
  });

});
