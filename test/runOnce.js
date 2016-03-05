/* globals before, describe, it, beforeEach, after, afterEach */
var expect = require('expect.js'),
  path = require('path'),
  Agenda = require(path.join('..', 'index.js'));

var r = require('./fixtures/connection');


function clearJobs(done) {
  r.table('agendaJobs').delete().run(done);
}

// Slow timeouts for travis
var jobTimeout = process.env.TRAVIS ? 1500 : 300;


describe('Once', function () {
  
  var jobs;
  
  before(function(done){
    jobs = new Agenda({
      rethinkdb: r
    }, done);
  });


  describe(' run just once', function () {
    this.timeout(20000);

    before(function(done){clearJobs(done);});

    it(' should run the job only once', function (done) {
      var startCounter = 0;

      jobs.define('runonce', {}, function (job, cb) {
        startCounter++;
        cb();
      });

      jobs.lockLimit(1);
      jobs.defaultLockLimit(4000);
      jobs.every('00 30 08 * * 2-6', 'runonce');
      jobs.start();

      setTimeout(function () {
        jobs.jobs({ name: 'runonce' }, function (err, job) {
          job[0].run();
        });
      }, 10);

      setTimeout(function () {
        jobs.stop(function () {
          expect(startCounter).to.be(1);
          done();
        });
      }, 18000);

    });
  });

  describe(' now just once', function () {
    this.timeout(20000);

    before(function(done){clearJobs(done);});

    it(' should schedule the job only once', function (done) {
      var startCounter = 0;

      jobs.define('nowonce', {}, function (job, cb) {
        startCounter++;
        cb();
      });
      
      jobs.lockLimit(1);
      jobs.defaultLockLimit(4000);
      jobs.every('00 30 08 * * 2-6', 'nowonce');
      jobs.start();

      setTimeout(function () {
        jobs.now('nowonce', '');
      }, 10);

      setTimeout(function () {
        jobs.stop(function () {
          expect(startCounter).to.be(1);
          done();
        });
      }, 18000);

    });

  });

  describe.only(' schedule just once', function () {
    this.timeout(20000);

    before(function(done){clearJobs(done);});

    it(' should schedule the job only once', function (done) {
      var startCounter = 0;

      jobs.define('scheduleonce', {}, function (job, cb) {
        startCounter++;
        cb();
      });
      
      jobs.lockLimit(1);
      jobs.defaultLockLimit(20000);
      jobs.every('00 30 08 * * 2-6', 'scheduleonce');
      jobs.start();

      setTimeout(function () {
        jobs.schedule(new Date(), 'scheduleonce', '');
      }, 10);

      setTimeout(function () {
        jobs.stop(function () {
          expect(startCounter).to.be(1);
          done();
        });

      }, 18000);

    });


  });


});
