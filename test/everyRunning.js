/* globals before, describe, it, beforeEach, after, afterEach */
var expect = require('expect.js'),
    path = require('path'),
    Agenda = require(path.join('..', 'index.js'));

var r = require('./fixtures/connection');


// create agenda instances
var jobs = null;

function clearJobs(done) {
    r.table('agendaJobs').delete().run(done);
}

// Slow timeouts for travis
var jobTimeout = process.env.TRAVIS ? 15000 : 300;



describe('everyRunning', function() {


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

    //after(clearJobs);

    describe('every running', function() {

        before(function(done) {
            jobs.defaultConcurrency(1);
            jobs.processEvery(5);
            jobs.stop(done);
        });

        it('should run the same job multiple times', function(done) {
            var counter = 0;

            jobs.define('everyRunTest1', function(job, cb) {
                if (counter < 2) {
                    counter++;
                }
                cb();
            });


            jobs.every(10, 'everyRunTest1');

            setTimeout(function() {

              jobs.start();

              setTimeout(function() {
                  jobs.jobs({
                      name: 'everyRunTest1'
                  }, function(err, res) {
                      expect(counter).to.be(2);
                      jobs.stop(done);
                  });
              }, jobTimeout);

            });

        });

        it('should reuse the same job on multiple runs', function(done) {
            var counter = 0;

            jobs.define('everyRunTest2', function(job, cb) {
                if (counter < 2) {
                    counter++;
                }
                cb();
            });
            jobs.every(10, 'everyRunTest2');

            // use the event loop to make sure Jobs is saved
            setTimeout(function() {

              jobs.start();

              setTimeout(function() {
                  jobs.jobs({
                      name: 'everyRunTest2'
                  }, function(err, res) {
                      expect(res).to.have.length(1);
                      jobs.stop(done);
                  });
              }, jobTimeout);

            }, 0);
        });

    });

});
