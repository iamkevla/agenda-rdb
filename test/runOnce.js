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
    this.timeout(20000);

    beforeEach(clearJobs);

    it(' should run the job only once', function (done) {
      var startCounter = 0;

      jobs.define('lock job', {
        lockLifetime: 50
      }, function (job, cb) {
        startCounter++;
      });

      jobs.every('00 30 08 * * 2-6', 'lock job');
      jobs.start();

      setTimeout(function () {
        jobs.jobs({name: 'lock job'}, function(err, job) {
          job[0].run();
        })
      }, 10);

      setTimeout(function () {
        expect(startCounter).to.be(1);
        jobs.stop(done);
      }, 10000);

    });
  });
  
  describe(' now just once', function () {
    this.timeout(20000);

    beforeEach(clearJobs);

    it(' should schedule the job only once', function (done) {
      var startCounter = 0;

      jobs.define('lock job', {
        lockLifetime: 50
      }, function (job, cb) {
        startCounter++;
      });
      
      jobs.maxConcurrency(1);
      jobs.every('00 30 08 * * 2-6', 'lock job');
      jobs.start();

      setTimeout(function () {
        jobs.now('lock job', '');
      }, 10);

      setTimeout(function () {
        expect(startCounter).to.be(1);
        jobs.stop(done);
      }, 15000);

    });
    
  });
    
    describe.skip(' schedule just once', function () {
      this.timeout(20000);

      beforeEach(clearJobs);

      it(' should schedule the job only once', function (done) {
        var startCounter = 0;

        jobs.define('lock job', {
          lockLifetime: 50
        }, function (job, cb) {
          startCounter++;
        });
        
        jobs.maxConcurrency(1);
        jobs.every('00 30 08 * * 2-6', 'lock job');
        jobs.start();

        setTimeout(function () {
          jobs.schedule('in 2 seconds', 'lock job', '');
        }, 10);

        setTimeout(function () {
          expect(startCounter).to.be(1);
          jobs.stop(done);
        }, 15000);

    });
    
    
  });
  

});
