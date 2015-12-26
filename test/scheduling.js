/* globals before, describe, it, beforeEach, after, afterEach */
var expect = require('expect.js'),
    path = require('path'),
    Agenda = require(path.join('..', 'index.js'));

var r = require('./fixtures/connection');



// create agenda instances
var jobs = null;

function clearJobs(done) {
    r.table('agendaJobs').delete().run(done);
}

// Slow timeouts for travis
var jobTimeout = process.env.TRAVIS ? 15000 : 300;


var jobType = 'do work';
var jobProcessor = function(job) {};


function failOnError(err) {
    if (err) {
        throw err;
    }
}


describe('scheduling.js', function() {

    this.timeout(11000);
    before(function(done) {

        jobs = new Agenda({
            rethinkdb: r,
            db: {
                table: 'agendaJobs'
            }
        }, function(err) {


            setTimeout(function() {
                clearJobs(done);
            }, 50);

        });
    });
  after(clearJobs);


    describe('Scheduling Concurrent Jobs', function() {


        it('do not run more than once in a scheduled time interval', function(done) {
            var jobs = new Agenda({
                defaultConcurrency: 1,
                rethinkdb: r,
                db: {
                    table: 'agendaJobs'
                }
            });
            var jobRunInterval = 400;
            var jobRunTime = 200;
            var processEvery = 300;
            var successTime = 1200;
            var lastRunAt;

            function jobProcessor(job, done) {
                if (lastRunAt) {
                    var timeSinceLastRun = new Date() - lastRunAt;

                    if (timeSinceLastRun < jobRunInterval - 50 || timeSinceLastRun > jobRunInterval + 400) {
                        throw 'INVALID Job Execution Time ' + timeSinceLastRun;
                    }
                }

                lastRunAt = new Date();

                setTimeout(done, jobRunTime);
            }


            jobs.define('concjob', {
                concurrency: 1
            }, jobProcessor);
            var interval = setInterval(function() {
                clearInterval(interval);
                jobs.stop(done);
            }, successTime);

            jobs.on('fail', function(err) {
                clearInterval(interval);
                jobs.stop(function() {
                    done(err);
                });
            });

            jobs.on('ready', function() {
                jobs.every(jobRunInterval, 'concjob');
                jobs.processEvery(processEvery);
                jobs.start();
            });
        });
    });

});
