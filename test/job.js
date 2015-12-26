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


describe('job.js', function() {


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

    after(clearJobs);


    describe('Job', function() {
        describe('repeatAt', function() {
            var job = new Job();
            it('sets the repeat at', function() {
                job.repeatAt('3:30pm');
                expect(job.attrs.repeatAt).to.be('3:30pm');
            });
            it('returns the job', function() {
                expect(job.repeatAt('3:30pm')).to.be(job);
            });
        });

        describe('unique', function() {
            var job = new Job();
            it('sets the unique property', function() {
                job.unique({
                    data: {
                        type: 'active',
                        userId: '123'
                    }
                });
                expect(JSON.stringify(job.attrs.unique)).to.be(JSON.stringify({
                    data: {
                        type: 'active',
                        userId: '123'
                    }
                }));
            });
            it('returns the job', function() {
                expect(job.unique({
                    data: {
                        type: 'active',
                        userId: '123'
                    }
                })).to.be(job);
            });
        });

        describe('repeatEvery', function() {
            var job = new Job();
            it('sets the repeat interval', function() {
                job.repeatEvery(5000);
                expect(job.attrs.repeatInterval).to.be(5000);
            });
            it('returns the job', function() {
                expect(job.repeatEvery('one second')).to.be(job);
            });
        });

        describe('schedule', function() {
            var job;
            beforeEach(function() {
                job = new Job();
            });
            it('sets the next run time', function() {
                job.schedule('in 5 minutes');
                expect(job.attrs.nextRunAt).to.be.a(Date);
            });
            it('sets the next run time Date object', function() {
                var when = new Date(Date.now() + 1000 * 60 * 3);
                job.schedule(when);
                expect(job.attrs.nextRunAt).to.be.a(Date);
                expect(job.attrs.nextRunAt.getTime()).to.eql(when.getTime());
            });
            it('returns the job', function() {
                expect(job.schedule('tomorrow at noon')).to.be(job);
            });
        });

        describe('priority', function() {
            var job;
            beforeEach(function() {
                job = new Job();
            });
            it('sets the priority to a number', function() {
                job.priority(10);
                expect(job.attrs.priority).to.be(10);
            });
            it('returns the job', function() {
                expect(job.priority(50)).to.be(job);
            });
            it('parses written priorities', function() {
                job.priority('high');
                expect(job.attrs.priority).to.be(10);
            });
        });



        describe('remove', function() {
            it('removes the job', function(done) {
                var job = new Job({
                    agenda: jobs,
                    name: 'removed job'
                });
                job.save(function(err) {
                    if (err) return done(err);
                    job.remove(function(err) {
                        if (err) return done(err);
                        r.table('agendaJobs').filter({
                            id: job.attrs.id
                        }).run(function(err, j) {
                            expect(j).to.have.length(0);
                            done();
                        });
                    });
                });
            });
        });

        describe('run', function() {
            var job;

            before(function() {
                jobs.define('testRun', function(job, done) {
                    setTimeout(function() {
                        done();
                    }, 100);
                });
            });

            beforeEach(function() {
                job = new Job({
                    agenda: jobs,
                    name: 'testRun'
                });
            });

            it('updates lastRunAt', function(done) {
                var now = new Date();
                setTimeout(function() {
                    job.run(function() {
                        expect(job.attrs.lastRunAt.valueOf()).to.be.greaterThan(now.valueOf());
                        done();
                    });
                }, 5);
            });

            it('fails if job is undefined', function(done) {
                job = new Job({
                    agenda: jobs,
                    name: 'not defined'
                });
                job.run(function() {
                    expect(job.attrs.failedAt).to.be.ok();
                    expect(job.attrs.failReason).to.be('Undefined job');
                    done();
                });
            });
            it('updates nextRunAt', function(done) {
                var now = new Date();
                job.repeatEvery('10 minutes');
                setTimeout(function() {
                    job.run(function() {
                        expect(job.attrs.nextRunAt.valueOf()).to.be.greaterThan(now.valueOf() + 59999);
                        done();
                    });
                }, 5);
            });
            it('handles errors', function(done) {
                job.attrs.name = 'failBoat';
                jobs.define('failBoat', function(job, cb) {
                    throw (new Error('Zomg fail'));
                });
                job.run(function(err) {
                    expect(err).to.be.ok();
                    done();
                });
            });
            it('handles errors with q promises', function(done) {
                job.attrs.name = 'failBoat2';
                jobs.define('failBoat2', function(job, cb) {
                    var Q = require('q');
                    Q.delay(100).then(function() {
                        throw (new Error('Zomg fail'));
                    }).fail(cb).done();
                });
                job.run(function(err) {
                    expect(err).to.be.ok();
                    done();
                });
            });

            it('doesn\'t allow a stale job to be saved', function(done) {
                var flag = false;
                job.attrs.name = 'failBoat3';
                job.save(function(err) {
                    if (err) return done(err);
                    jobs.define('failBoat3', function(job, cb) {
                        // Explicitly find the job again,
                        // so we have a new job object
                        jobs.jobs({
                            name: 'failBoat3'
                        }, function(err, j) {
                            if (err) return done(err);
                            j[0].remove(function(err) {
                                if (err) return done(err);
                                cb();
                            });
                        });
                    });

                    job.run(function(err) {
                        // Expect the deleted job to not exist in the database
                        jobs.jobs({
                            name: 'failBoat3'
                        }, function(err, j) {
                            if (err) return done(err);
                            expect(j).to.have.length(0);
                            done();
                        });
                    });
                });
            });

        });

        describe('touch', function(done) {
            it('extends the lock lifetime', function(done) {
                var lockedAt = new Date();
                var job = new Job({
                    agenda: jobs,
                    name: 'some job',
                    lockedAt: lockedAt
                });
                job.save = function(cb) {
                    cb();
                };
                setTimeout(function() {
                    job.touch(function() {
                        expect(job.attrs.lockedAt).to.be.greaterThan(lockedAt);
                        done();
                    });
                }, 2);
            });
        });

        describe('fail', function() {
            var job = new Job();
            it('takes a string', function() {
                job.fail('test');
                expect(job.attrs.failReason).to.be('test');
            });
            it('takes an error object', function() {
                job.fail(new Error('test'));
                expect(job.attrs.failReason).to.be('test');
            });
            it('sets the failedAt time', function() {
                job.fail('test');
                expect(job.attrs.failedAt).to.be.a(Date);
            });
        });

        describe('enable', function() {
            it('sets disabled to false on the job', function() {
                var job = new Job({
                    disabled: true
                });
                job.enable();
                expect(job.attrs.disabled).to.be(false);
            });

            it('returns the job', function() {
                var job = new Job({
                    disabled: true
                });
                expect(job.enable()).to.be(job);
            });
        });

        describe('disable', function() {
            it('sets disabled to true on the job', function() {
                var job = new Job();
                job.disable();
                expect(job.attrs.disabled).to.be(true);
            });
            it('returns the job', function() {
                var job = new Job();
                expect(job.disable()).to.be(job);
            });
            after(clearJobs);
        });

        describe('save', function() {
            it('calls saveJob on the agenda', function(done) {
                var oldSaveJob = jobs.saveJob;
                jobs.saveJob = function() {
                    jobs.saveJob = oldSaveJob;
                    done();
                };
                var job = jobs.create('some job', {
                    wee: 1
                });
                job.save();
            });

            it('doesnt save the job if its been removed', function(done) {
                var job = jobs.create('another job');
                // Save, then remove, then try and save again.
                // The second save should fail.
                job.save(function(err, j) {
                    j.remove(function() {
                        j.save(function(err, res) {
                            jobs.jobs({
                                name: 'another job'
                            }, function(err, res) {
                                expect(res).to.have.length(0);
                                done();
                            });
                        });
                    });
                });
            });

            it('returns the job', function() {
                var job = jobs.create('some job', {
                    wee: 1
                });
                expect(job.save()).to.be(job);
            });
        });
    });
});
