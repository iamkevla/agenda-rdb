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
var jobs = new Agenda({
  db: {
    address: rethinkCfg
  }
});

function clearJobs(done) {
  r.table('agendaJobs').delete().run(done);
}

// Slow timeouts for travis
var jobTimeout = process.env.TRAVIS ? 1500 : 300;


var jobType = 'do work';
var jobProcessor = function (job) { };


function failOnError(err) {
  if (err) {
    throw err;
  }
}


describe('unique', function () {


  beforeEach(function (done) {

    clearJobs(function () {
      jobs.define('someJob', jobProcessor);
      jobs.define('send email', jobProcessor);
      jobs.define('some job', jobProcessor);
      jobs.define(jobType, jobProcessor);
      done();
    });

  });


  describe('unique', function () {

    describe('should demonstrate unique constraint', function () {

      it('should modify one job when unique matches', function (done) {
        jobs.create('unique job', {
          type: 'active',
          userId: '123',
          'other': true
        }).unique({
          data: {
            type: 'active',
            userId: '123'
          }
        }).schedule('now').save(function (err, job1) {
          jobs.create('unique job', {
            type: 'active',
            userId: '123',
            'other': false
          }).unique({
            data: {
              type: 'active',
              userId: '123'
            }
          }).schedule('now').save(function (err, job2) { 
            expect(job1.attrs.nextRunAt.toISOString()).not.to.equal(job2.attrs.nextRunAt.toISOString());
            r.table('agendaJobs').filter({
              name: 'unique job'
            }).run(function (err, j) {
              expect(j).to.have.length(1);
              done();
            });
          });
        });
      });

      it('should not modify job when unique matches and insertOnly is set to true', function (done) {
        
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
          }).schedule('now').save(function (err, job1) {
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
            }).schedule('now').save(function (err, job2) {
              //expect(job1.attrs.nextRunAt.toISOString()).to.equal(job2.nextRunAt.toISOString());
              r.table('agendaJobs').filter({
                name: 'unique job'
              }).run(function (err, j) {
                expect(j).to.have.length(1);
                done();
              });
            });
          });
      });

    });

    describe('should demonstrate non-unique contraint', function () {

      it('should create two jobs when unique doesn\t match', function (done) {
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
        }).schedule(time).save(function (err, job) {
          jobs.create('unique job', {
            type: 'active',
            userId: '123',
            'other': false
          }).unique({
            'data.type': 'active',
            'data.userId': '123',
            nextRunAt: time2
          }).schedule(time).save(function (err, job) {
            r.table('agendaJobs').filter({
              name: 'unique job'
            }).run(function (err, j) {
              expect(j).to.have.length(2);
              done();
            });
          });
        });

      });
      //after(clearJobs);

    });

  });

});
