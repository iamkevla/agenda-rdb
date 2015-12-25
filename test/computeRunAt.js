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


describe('computeRunAt', function() {


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



    describe('computeNextRunAt', function() {
        var job;

        beforeEach(function() {
            job = new Job();
        });

        it('returns the job', function() {
            expect(job.computeNextRunAt()).to.be(job);
        });

        it('sets to undefined if no repeat at', function() {
            job.attrs.repeatAt = null;
            job.computeNextRunAt();
            expect(job.attrs.nextRunAt).to.be(undefined);
        });

        it('it understands repeatAt times', function() {
            var d = new Date();
            d.setHours(23);
            d.setMinutes(59);
            d.setSeconds(0);
            job.attrs.repeatAt = '11:59pm';
            job.computeNextRunAt();
            expect(job.attrs.nextRunAt.getHours()).to.be(d.getHours());
            expect(job.attrs.nextRunAt.getMinutes()).to.be(d.getMinutes());
        });

        it('sets to undefined if no repeat interval', function() {
            job.attrs.repeatInterval = null;
            job.computeNextRunAt();
            expect(job.attrs.nextRunAt).to.be(undefined);
        });

        it('it understands human intervals', function() {
            var now = new Date();
            job.attrs.lastRunAt = now;
            job.repeatEvery('2 minutes');
            job.computeNextRunAt();
            expect(job.attrs.nextRunAt).to.be(now.valueOf() + 120000);
        });

        it('understands cron intervals', function() {
            var now = new Date();
            now.setMinutes(1);
            now.setMilliseconds(0);
            now.setSeconds(0);
            job.attrs.lastRunAt = now;
            job.repeatEvery('*/2 * * * *');
            job.computeNextRunAt();
            expect(job.attrs.nextRunAt.valueOf()).to.be(now.valueOf() + 60000);
        });

        describe('when repeat at time is invalid', function() {
            beforeEach(function() {
                try {
                    job.attrs.repeatAt = 'foo';
                    job.computeNextRunAt();
                } catch (e) {}
            });

            it('sets nextRunAt to undefined', function() {
                expect(job.attrs.nextRunAt).to.be(undefined);
            });

            it('fails the job', function() {
                expect(job.attrs.failReason).to.equal('failed to calculate repeatAt time due to invalid format');
            });
        });

        describe('when repeat interval is invalid', function() {
            beforeEach(function() {
                try {
                    job.attrs.repeatInterval = 'asd';
                    job.computeNextRunAt();
                } catch (e) {}
            });

            it('sets nextRunAt to undefined', function() {
                expect(job.attrs.nextRunAt).to.be(undefined);
            });

            it('fails the job', function() {
                expect(job.attrs.failReason).to.equal('failed to calculate nextRunAt due to invalid repeat interval');
            });
        });

    });
});
