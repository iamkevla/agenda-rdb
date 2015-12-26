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



function failOnError(err) {
    if (err) {
        throw err;
    }
}


describe('startStop', function() {


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

    describe('start/stop', function() {
        it('starts/stops the job queue', function(done) {
            var stopped = false;
            jobs.define('jobQueueTest', function jobQueueTest(job, cb) {
                jobs.stop(function() {
                    if (stopped === false) {
                        stopped = true;
                        done();
                    }
                });
                cb();
            });

            jobs.every('1 second', 'jobQueueTest');
            jobs.processEvery('1 second');
            jobs.start();

        });

        it('does not run disabled jobs', function(done) {
            var ran = false;
            jobs.define('disabledJob', function() {
                ran = true;
            });
            var job = jobs.create('disabledJob').disable().schedule('now');
            job.save(function(err) {
                if (err) return done(err);
                jobs.start();
                setTimeout(function() {
                    expect(ran).to.be(false);
                    jobs.stop(done);
                }, jobTimeout);
            });
        });

        it('clears locks on stop', function(done) {

            jobs.define('longRunningJob', function(job, cb) {
                //Job never finishes
            });
            jobs.every('10 seconds', 'longRunningJob');
            jobs.processEvery('1 second');
            jobs.start();

            setTimeout(function() {

                jobs.stop(function(err, res) {
                    jobs._table.filter({
                        name: 'longRunningJob'
                    }).run().then(function(job) {

                        expect(job.lockedAt).to.be(undefined);
                        done();
                    });
                });
            }, jobTimeout);
        });

        describe('events', function() {
            beforeEach(clearJobs);
            it('emits start event', function(done) {
                var job = new Job({
                    agenda: jobs,
                    name: 'jobQueueTest'
                });
                jobs.once('start', function(j) {
                    expect(j).to.be(job);
                    done();
                });
                job.run();
            });
            it('emits start:job name event', function(done) {
                var job = new Job({
                    agenda: jobs,
                    name: 'jobQueueTest'
                });
                jobs.once('start:jobQueueTest', function(j) {
                    expect(j).to.be(job);
                    done();
                });
                job.run();
            });


            it('emits complete event', function(done) {

                var job = new Job({
                    agenda: jobs,
                    name: 'jobQueueTest'
                });

                setTimeout(function() {
                    job.run(function(err, j) {
                        expect(job.attrs.id.toString()).to.be(j.attrs.id.toString());
                        done();
                    });
                }, 0);

            });

            it('emits complete:job name event', function(done) {

                jobs.once('complete:jobQueueTest', function(j) {
                    expect(job.attrs.id.toString()).to.be(j.attrs.id.toString());
                    done();
                });

                var job = new Job({
                    agenda: jobs,
                    name: 'jobQueueTest'
                });

                job.run();
            });
            it('emits success event', function(done) {
                var job = new Job({
                    agenda: jobs,
                    name: 'jobQueueTest'
                });
                jobs.once('success', function(j) {
                    expect(j).to.be.ok();
                    done();
                });
                job.run();
            });
            it('emits success:job name event', function(done) {
                var job = new Job({
                    agenda: jobs,
                    name: 'jobQueueTest'
                });
                jobs.once('success:jobQueueTest', function(j) {
                    expect(j).to.be.ok();
                    done();
                });
                job.run();
            });
            it('emits fail event', function(done) {
                var job = new Job({
                    agenda: jobs,
                    name: 'failBoat'
                });
                jobs.once('fail', function(err, j) {
                    expect(err.message).to.be('Undefined job');
                    expect(j).to.be(job);
                    done();
                });
                job.run();
            });
            it('emits fail:job name event', function(done) {
                var job = new Job({
                    agenda: jobs,
                    name: 'failBoat'
                });
                jobs.once('fail:failBoat', function(err, j) {
                    expect(err.message).to.be('Undefined job');
                    expect(j).to.be(job);
                    done();
                });
                job.run();
            });


        });

    });

});
