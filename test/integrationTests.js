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





describe('Integration Tests', function() {

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

    describe('Integration Tests', function() {

        describe('.every()', function() {
          this.timeout(15000);
            it('Should not rerun completed jobs after restart', function(done) {
                var i = 0;

                var serviceError = function(e) {
                    done(e);
                };
                var receiveMessage = function(msg) {
                    if (msg === 'ran') {
                        expect(i).to.be(0);
                        i += 1;
                        startService();
                    } else if (msg === 'notRan') {
                        expect(i).to.be(0);
                        done();
                    } else return done(new Error('Unexpected response returned!'));
                };

                var startService = function() {
                    var serverPath = path.join(__dirname, 'fixtures', 'agenda-instance.js');
                    var n = cp.fork(serverPath, [rethinkCfg, 'daily']);

                    n.on('message', receiveMessage);
                    n.on('error', serviceError);
                };

                startService();
            });

            it('Should properly run jobs when defined via an array', function(done) {
                var ran1 = false,
                    ran2 = true,
                    doneCalled = false,
                    n;

                var serviceError = function(e) {
                    done(e);
                };

                var receiveMessage = function(msg) {
                    if (msg === 'test1-ran') {
                        ran1 = true;
                        if (!!ran1 && !!ran2 && !doneCalled) {
                            doneCalled = true;
                            done();
                            return n.send('exit');
                        }
                    } else if (msg === 'test2-ran') {
                        ran2 = true;
                        if (!!ran1 && !!ran2 && !doneCalled) {
                            doneCalled = true;
                            done();
                            return n.send('exit');
                        }
                    } else return done(new Error('Jobs did not run!'));
                };

                var startService = function() {

                    var serverPath = path.join(__dirname, 'fixtures', 'agenda-instance.js');
                    n = cp.fork(serverPath, [rethinkCfg, 'daily-array']);

                    n.on('message', receiveMessage);
                    n.on('error', serviceError);
                };

                startService();
            });

            it('should not run if job is disabled', function(done) {
                var counter = 0;
                jobs.define('everyDisabledTest', function(job, cb) {
                    counter++;
                    cb();
                });

                var job = jobs.every('1 second', 'everyDisabledTest');

              //need to let the id of job to be saved by using event loop
                setTimeout(function() {

                  job.disable();
                  job.save(function() {
                      jobs.start();

                      setTimeout(function() {
                          jobs.jobs({
                              name: 'everyDisabledTest'
                          }, function(err, res) {
                              expect(counter).to.be(0);
                              jobs.stop(done);
                          });
                      }, jobTimeout);

                  });

                }, 50);

            });

        });

        describe('schedule()', function() {

            it('Should not run jobs scheduled in the future', function(done) {
                var i = 0;

                var serviceError = function(e) {
                    done(e);
                };
                var receiveMessage = function(msg) {
                    if (msg === 'notRan') {
                        if (i < 5) return done();

                        i += 1;
                        startService();
                    } else return done(new Error('Job scheduled in future was ran!'));
                };

                var startService = function() {
                    var serverPath = path.join(__dirname, 'fixtures', 'agenda-instance.js');
                    var n = cp.fork(serverPath, [rethinkCfg, 'define-future-job']);

                    n.on('message', receiveMessage);
                    n.on('error', serviceError);
                };

                startService();
            });

            it('Should run past due jobs when process starts', function(done) {

                var serviceError = function(e) {
                    done(e);
                };
                var receiveMessage = function(msg) {
                    if (msg === 'ran') {
                        done();
                    } else return done(new Error('Past due job did not run!'));
                };

                var startService = function() {
                    var serverPath = path.join(__dirname, 'fixtures', 'agenda-instance.js');
                    var n = cp.fork(serverPath, [rethinkCfg, 'define-past-due-job']);

                    n.on('message', receiveMessage);
                    n.on('error', serviceError);
                };

                startService();
            });

            it('Should schedule using array of names', function(done) {
                var ran1 = false,
                    ran2 = false,
                    doneCalled = false;

                var serviceError = function(e) {
                    done(e);
                };
                var receiveMessage = function(msg) {

                    if (msg === 'test1-ran') {
                        ran1 = true;
                        if (!!ran1 && !!ran2 && !doneCalled) {
                            doneCalled = true;
                            done();
                            return n.send('exit');
                        }
                    } else if (msg === 'test2-ran') {
                        ran2 = true;
                        if (!!ran1 && !!ran2 && !doneCalled) {
                            doneCalled = true;
                            done();
                            return n.send('exit');
                        }
                    } else return done(new Error('Jobs did not run!'));
                };


                var serverPath = path.join(__dirname, 'fixtures', 'agenda-instance.js');
                var n = cp.fork(serverPath, [rethinkCfg, 'schedule-array']);

                n.on('message', receiveMessage);
                n.on('error', serviceError);
            });


        });

        describe('now()', function() {

            it('Should immediately run the job', function(done) {
                var serviceError = function(e) {
                    done(e);
                };
                var receiveMessage = function(msg) {
                    if (msg === 'ran') {
                        return done();
                    } else return done(new Error('Job did not immediately run!'));
                };

                var serverPath = path.join(__dirname, 'fixtures', 'agenda-instance.js');
                var n = cp.fork(serverPath, [rethinkCfg, 'now']);

                n.on('message', receiveMessage);
                n.on('error', serviceError);

            });


        });
    });

});
