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
var jobTimeout = process.env.TRAVIS ? 15000 : 300;


var jobType = 'do work';
var jobProcessor = function(job) {};


function failOnError(err) {
    if (err) {
        throw err;
    }
}


describe('scheduling.js', function() {


    before(function(done) {

        jobs = new Agenda({
            rethinkdb: r,
            db: {
                table: 'agendaJobs'
            }
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


    describe('Scheduling Concurrent Jobs', function() {

        beforeEach(function(done) {
            clearJobs(done);
        });
        after(clearJobs);

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
            var successTime = 5000;
            var lastRunAt;

            function jobProcessor(job, done) {
                if (lastRunAt) {
                    var timeSinceLastRun = new Date() - lastRunAt;

                    if (timeSinceLastRun < jobRunInterval - 50 || timeSinceLastRun > jobRunInterval + 300) {
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

            this.timeout(11000);

            jobs.on('ready', function() {
                jobs.every(jobRunInterval, 'concjob');
                jobs.processEvery(processEvery);
                jobs.start();
            });
        });
    });

});
