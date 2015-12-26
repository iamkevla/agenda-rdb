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
var jobTimeout = process.env.TRAVIS ? 1500 : 300;


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
                        }, 50);
                    });
                    jobs.now('immediateJob', function() {
                      jobs.start();
                    });


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

});
