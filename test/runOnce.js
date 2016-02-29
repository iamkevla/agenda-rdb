/* globals before, describe, it, beforeEach, after, afterEach */
var expect = require('expect.js'),
  path = require('path'),
  Agenda = require(path.join('..', 'index.js'));

var r = require('./fixtures/connection');


// create agenda instances
var jobs = new Agenda({
  rethinkdb: r,
  db: {
    table: 'agendaJobs'
  }
});

function clearJobs(done) {
  r.table('agendaJobs').delete().run(done);
}

// Slow timeouts for travis
var jobTimeout = process.env.TRAVIS ? 1500 : 300;


describe('Once', function () {


  describe(' run just once', function () {
    this.timeout(30000);

    beforeEach(clearJobs);

    it(' should run the job only once', function (done) {
      var startCounter = 0;

      jobs.define('runonce', {
        lockLifetime: 50
      }, function (job, cb) {
        startCounter++;
      });

      jobs.every('00 30 08 * * 2-6', 'runonce');
      jobs.start();

      setTimeout(function () {
        jobs.jobs({ name: 'runonce' }, function (err, job) {
          job[0].run();
        })
      }, 10);

      setTimeout(function () {
        jobs.stop(function () {
          expect(startCounter).to.be(1);
          done();
        });
      }, 28000);

    });
  });

  describe(' now just once', function () {
    this.timeout(30000);

    beforeEach(clearJobs);

    it(' should schedule the job only once', function (done) {
      var startCounter = 0;

      jobs.define('nowonce', {
        lockLifetime: 50
      }, function (job, cb) {
        startCounter++;
      });

      jobs.maxConcurrency(1);
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
      }, 28000);

    });

  });

  describe(' schedule just once', function () {
    this.timeout(40000);

    beforeEach(clearJobs);

    it(' should schedule the job only once', function (done) {
      var startCounter = 0;

      jobs.define('scheduleonce', {
        lockLifetime: 50
      }, function (job, cb) {
        startCounter++;
      });

      jobs.maxConcurrency(1);
      jobs.every('00 30 08 * * 2-6', 'scheduleonce');
      jobs.start();

      setTimeout(function () {
        jobs.schedule('in 10 seconds', 'scheduleonce', '');
      }, 10);

      setTimeout(function () {
        jobs.stop(function () {
          expect(startCounter).to.be(1);
          done();
        });

      }, 39000);

    });


  });


});
