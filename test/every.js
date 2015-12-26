/* globals before, describe, it, beforeEach, after, afterEach */
var expect = require('expect.js'),
    path = require('path'),
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


describe('every.js', function() {


    before(function(done) {

        jobs = new Agenda({
            rethinkdb: r,
            db: {
                table: 'agendaJobs'
            }
        }, function(err) {

          setTimeout(function() {
                clearJobs(function() {
                    jobs.define('send email', jobProcessor);
                    jobs.define('some job', jobProcessor);
                    done();
                });
            }, 50);

        });
    });


    describe('every', function() {
        describe('with a job name specified', function() {

            it('returns a job', function() {
                expect(jobs.every('5 minutes', 'send email')).to.be.a(Job);
            });

            it('sets the repeatEvery', function() {
                expect(jobs.every('5 seconds', 'send email').attrs.repeatInterval).to.be('5 seconds');
            });

            it('sets the agenda', function() {
                jobs.every('5 seconds', 'send email', function(err, Job) {
                  expect(Job.agenda).to.be(jobs);
                });
            });

            it('should update a job that was previously scheduled with `every`', function(done) {
                jobs.every(10, 'shouldBeSingleJob', function() {
                  setTimeout(function() {
                    jobs.every(20, 'shouldBeSingleJob');
                  }, 10);
                });

                // Give the saves a little time to propagate
                setTimeout(function() {
                    jobs.jobs({
                        name: 'shouldBeSingleJob'
                    }, function(err, res) {
                        expect(res).to.have.length(1);
                        done();
                    });
                }, jobTimeout);

            });
            after(clearJobs);
        });

        describe('with array of names specified', function() {
          it('returns array of jobs', function() {
            jobs.every('5 minutes', ['send email', 'some job'], function(err, Job) {
              expect(Job).to.be.an('array');
            });
          });
        });
    });

});
