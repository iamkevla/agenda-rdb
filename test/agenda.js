/* globals before, describe, it, beforeEach, after, afterEach */
var rethinkHost = process.env.RETHINKDB_HOST || 'localhost',
    rethinkPort = process.env.RETHINKDB_PORT || '28015',
    rethinkCfg = 'http://' + rethinkHost + ':' + rethinkPort + '/test';

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

describe('agenda', function() {


    before(function(done) {

        jobs = new Agenda({
            db: {
                address: rethinkCfg
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


    describe('Agenda', function() {
        it('sets a default processEvery', function() {
            expect(jobs._processEvery).to.be(5000);
        });

        describe('configuration methods', function() {
            it('sets the _db directly when passed as an option', function() {
                var agenda = new Agenda({
                    rethinkdb: r,
                    db: {
                        table: 'agendaJobs'
                    }
                });
                expect(typeof agenda).to.equal('object');
            });
        });

        describe('configuration methods', function() {

            describe('rethink', function() {
                it('sets the _db directly', function() {
                    var agenda = new Agenda();
                    agenda.rethink(r, 'agendaJobs');
                    expect(typeof agenda).to.equal('object');
                });

                it('returns itself', function() {
                    var agenda = new Agenda();
                    expect(agenda.rethink(r)).to.be(agenda);
                });
            });

            describe('name', function() {
                it('sets the agenda name', function() {
                    jobs.name('test queue');
                    expect(jobs._name).to.be('test queue');
                });
                it('returns itself', function() {
                    expect(jobs.name('test queue')).to.be(jobs);
                });
            });

            describe('processEvery', function() {
                it('sets the processEvery time', function() {
                    jobs.processEvery('3 minutes');
                    expect(jobs._processEvery).to.be(180000);
                });
                it('returns itself', function() {
                    expect(jobs.processEvery('3 minutes')).to.be(jobs);
                });
            });
            describe('maxConcurrency', function() {
                it('sets the maxConcurrency', function() {
                    jobs.maxConcurrency(10);
                    expect(jobs._maxConcurrency).to.be(10);
                });
                it('returns itself', function() {
                    expect(jobs.maxConcurrency(10)).to.be(jobs);
                });
            });
            describe('defaultConcurrency', function() {
                it('sets the defaultConcurrency', function() {
                    jobs.defaultConcurrency(1);
                    expect(jobs._defaultConcurrency).to.be(1);
                });
                it('returns itself', function() {
                    expect(jobs.defaultConcurrency(5)).to.be(jobs);
                });
            });
            describe('defaultLockLifetime', function() {
                it('returns itself', function() {
                    expect(jobs.defaultLockLifetime(1000)).to.be(jobs);
                });
                it('sets the default lock lifetime', function() {
                    jobs.defaultLockLifetime(9999);
                    expect(jobs._defaultLockLifetime).to.be(9999);
                });
                it('is inherited by jobs', function() {
                    jobs.defaultLockLifetime(7777);
                    jobs.define('testDefaultLockLifetime', function(job, done) {});
                    expect(jobs._definitions.testDefaultLockLifetime.lockLifetime).to.be(7777);
                });
            });
        });

        describe('job methods', function() {

            describe('create', function() {
                var job;
                beforeEach(function() {
                    job = jobs.create('sendEmail', {
                        to: 'some guy'
                    });
                });

                it('returns a job', function() {
                    expect(job).to.be.a(Job);
                });
                it('sets the name', function() {
                    expect(job.attrs.name).to.be('sendEmail');
                });
                it('sets the type', function() {
                    expect(job.attrs.type).to.be('normal');
                });
                it('sets the agenda', function() {
                    expect(job.agenda).to.be(jobs);
                });
                it('sets the data', function() {
                    expect(job.attrs.data).to.have.property('to', 'some guy');
                });
            });

            describe('define', function() {

                it('stores the definition for the job', function() {
                    expect(jobs._definitions.someJob).to.have.property('fn', jobProcessor);
                });

                it('sets the default concurrency for the job', function() {
                    expect(jobs._definitions.someJob).to.have.property('concurrency', 5);
                });

                it('sets the default priority for the job', function() {
                    expect(jobs._definitions.someJob).to.have.property('priority', 0);
                });
                it('takes concurrency option for the job', function() {
                    jobs.define('highPriority', {
                        priority: 10
                    }, jobProcessor);
                    expect(jobs._definitions.highPriority).to.have.property('priority', 10);
                });
            });



            describe('schedule', function() {
                describe('with a job name specified', function() {
                    it('returns a job', function() {
                        expect(jobs.schedule('in 5 minutes', 'send email')).to.be.a(Job);
                    });
                    it('sets the schedule', function() {
                        var fiveish = (new Date()).valueOf() + 250000;
                        expect(jobs.schedule('in 5 minutes', 'send email').attrs.nextRunAt.valueOf()).to.be.greaterThan(fiveish);
                    });
                });
                describe('with array of names specified', function() {
                    it('returns array of jobs', function() {
                        expect(jobs.schedule('5 minutes', ['send email', 'some job'])).to.be.an('array');
                    });
                });
                after(clearJobs);
            });

            describe('unique', function() {

                describe('should demonstrate unique contraint', function(done) {

                    it('should modify one job when unique matches', function(done) {
                        jobs.create('unique job', {
                            type: 'active',
                            userId: '123',
                            'other': true
                        }).unique({
                            data: {
                                type: 'active',
                                userId: '123'
                            }
                        }).schedule('now').save(function(err, job1) {
                            jobs.create('unique job', {
                                type: 'active',
                                userId: '123',
                                'other': false
                            }).unique({
                                data: {
                                    type: 'active',
                                    userId: '123'
                                }
                            }).schedule('now').save(function(err, job2) {
                                expect(job1.attrs.nextRunAt.toISOString()).not.to.equal(job2.attrs.nextRunAt.toISOString());
                                r.table('agendaJobs').filter({
                                    name: 'unique job'
                                }).run(function(err, j) {
                                    expect(j).to.have.length(1);
                                    done();
                                });
                            });
                        });
                    });

                    it('should not modify job when unique matches and insertOnly is set to true', function(done) {
                        jobs.create('unique job', {
                            type: 'active',
                            userId: '123',
                            'other': true
                        }).unique({
                            data: {
                                type: 'active',
                                userId: '123'
                            }
                        }, {
                            insertOnly: true
                        }).schedule('now').save(function(err, job1) {
                            jobs.create('unique job', {
                                type: 'active',
                                userId: '123',
                                'other': false
                            }).unique({
                                data: {
                                    type: 'active',
                                    userId: '123'
                                }
                            }, {
                                insertOnly: true
                            }).schedule('now').save(function(err, job2) {
                                //expect(job1.attrs.nextRunAt.toISOString()).to.equal(job2.attrs.nextRunAt.toISOString());
                                r.table('agendaJobs').filter({
                                    name: 'unique job'
                                }).run(function(err, j) {
                                    expect(j).to.have.length(1);
                                    done();
                                });
                            });
                        });
                    });

                    after(clearJobs);

                });

                describe('should demonstrate non-unique contraint', function(done) {

                    it('should create two jobs when unique doesn\t match', function(done) {
                        var time = new Date(Date.now() + 1000 * 60 * 3);
                        var time2 = new Date(Date.now() + 1000 * 60 * 4);

                        jobs.create('unique job', {
                            type: 'active',
                            userId: '123',
                            'other': true
                        }).unique({
                            'data.type': 'active',
                            'data.userId': '123',
                            nextRunAt: time
                        }).schedule(time).save(function(err, job) {
                            jobs.create('unique job', {
                                type: 'active',
                                userId: '123',
                                'other': false
                            }).unique({
                                'data.type': 'active',
                                'data.userId': '123',
                                nextRunAt: time2
                            }).schedule(time).save(function(err, job) {
                                r.table('agendaJobs').filter({
                                    name: 'unique job'
                                }).run(function(err, j) {
                                    expect(j).to.have.length(2);
                                    done();
                                });
                            });
                        });

                    });
                    after(clearJobs);

                });

            });

            describe('now', function() {
                it('returns a job', function() {
                    expect(jobs.now('send email')).to.be.a(Job);
                });
                it('sets the schedule', function() {
                    var now = new Date();
                    expect(jobs.now('send email').attrs.nextRunAt.valueOf()).to.be.greaterThan(now.valueOf() - 1);
                });

                it('runs the job immediately', function(done) {
                    jobs.define('immediateJob', function(job) {
                        expect(job.isRunning()).to.be(true);
                        setTimeout(function() {
                            jobs.stop(done);
                        }, 500);
                    });
                    jobs.now('immediateJob');
                    jobs.start();

                });

                after(clearJobs);
            });

            describe('jobs', function() {
                it('returns jobs', function(done) {
                    var job = jobs.create('test');
                    job.save(function() {
                        jobs.jobs({}, function(err, c) {
                            expect(c.length).to.not.be(0);
                            expect(c[0]).to.be.a(Job);
                            clearJobs(done);
                        });
                    });
                });
            });

            describe('purge', function() {
                it('removes all jobs without definitions', function(done) {
                    var job = jobs.create('no definition');
                    jobs.stop(function() {
                        job.save(function() {
                            jobs.jobs({
                                name: 'no definition'
                            }, function(err, j) {
                                if (err) return done(err);
                                expect(j).to.have.length(1);
                                jobs.purge(function(err) {
                                    if (err) return done(err);
                                    jobs.jobs({
                                        name: 'no definition'
                                    }, function(err, j) {
                                        if (err) return done(err);
                                        expect(j).to.have.length(0);
                                        done();
                                    });
                                });
                            });
                        });
                    });
                });
            });

            describe('saveJob', function() {
                it('persists job to the database', function(done) {
                    var job = jobs.create('someJob', {});
                    job.save(function(err, job) {
                        expect(job.attrs.id).to.be.ok();
                        clearJobs(done);
                    });
                });
            });
        });

        describe('cancel', function() {
            beforeEach(function(done) {
                var remaining = 3;
                var checkDone = function(err) {
                    if (err) return done(err);
                    remaining--;
                    if (!remaining) {
                        done();
                    }
                };
                jobs.create('jobA').save(checkDone);
                jobs.create('jobA', 'someData').save(checkDone);
                jobs.create('jobB').save(checkDone);
            });

            afterEach(function(done) {
                clearJobs(done);
            });

            it('should cancel a job', function(done) {

                jobs.jobs({
                    name: 'jobA'
                }, function(err, j) {
                    if (err) return done(err);
                    expect(j).to.have.length(2);
                    jobs.cancel({
                        name: 'jobA'
                    }, function(err) {
                        if (err) return done(err);
                        jobs.jobs({
                            name: 'jobA'
                        }, function(err, j) {
                            if (err) return done(err);
                            expect(j).to.have.length(0);
                            done();
                        });
                    });
                });
            });

            it('should cancel multiple jobs', function(done) {
                jobs.jobs(function(job) {
                    return r.expr(['jobA', 'jobB']).contains(job('name'));
                }, function(err, j) {
                    if (err) return done(err);
                    expect(j).to.have.length(3);
                    jobs.cancel(function(job) {
                        return r.expr(['jobA', 'jobB']).contains(job('name'));
                    }, function(err) {
                        if (err) return done(err);
                        jobs.jobs(function(job) {
                            return r.expr(['jobA', 'jobB']).contains(job('name'));
                        }, function(err, j) {
                            if (err) return done(err);
                            expect(j).to.have.length(0);
                            done();
                        });
                    });
                });
            });

            it('should cancel jobs only if the data matches', function(done) {
                jobs.jobs({
                    name: 'jobA',
                    data: 'someData'
                }, function(err, j) {
                    if (err) return done(err);
                    expect(j).to.have.length(1);
                    jobs.cancel({
                        name: 'jobA',
                        data: 'someData'
                    }, function(err) {
                        if (err) return done(err);
                        jobs.jobs({
                            name: 'jobA',
                            data: 'someData'
                        }, function(err, j) {
                            if (err) return done(err);
                            expect(j).to.have.length(0);
                            jobs.jobs({
                                name: 'jobA'
                            }, function(err, j) {
                                if (err) return done(err);
                                expect(j).to.have.length(1);
                                done();
                            });
                        });
                    });
                });
            });
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
                db: {
                    address: rethinkCfg
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
            after(clearJobs);
        });


    });

});
