
var _ = require('underscore')
var http = require('http')
var request = require('request')
var async = require('async')
var Storm = 'http://localhost:9999'


function safeParse(json) {
  var parsed
  try {
    parsed = JSON.parse(json)
  } catch(e) {
    parsed = json
  }
  return parsed
}

function restructure(data){
  var payload = {}
  _.each(data.delete, function(job){
    //job.deleted = job.deleted == "true"
      payload[job.job_id] = {
        "success" : job.deleted
      }
      if(job.error) {payload[job.job_id]["messages"] = [job.error]}
      else{{payload[job.job_id]["messages"] = []}}
  })
  return payload
}

// getActive function returns all jobs with job status of ACTIVE
exports.getPing = function(req, res) {
  console.log('getPing function')
  request(Storm+'/api/ping', function(error, response, body){
    if(error){
      console.log(error)
      res.send(error)
    }else{
      console.log(body)
      res.send(response.statusCode, body)
    }
  })
}

// getActive function returns all jobs with job status of ACTIVE
exports.getActive = function(req, res) {
  console.log('getActive function')
  request(Storm+'/api/jobs/status/processing', function(error, response, body){
    if(error){
      console.log(error)
      res.send(error)
    }else{
      body = safeParse(body)
      res.send(response.statusCode, body)
    }
  })
}

// getStatus function returns the job status of supplied job_id
exports.getStatus = function(req, res) {
  console.log('getStatus function')
  var job_id = req.params.job_id
  request(Storm+'/api/job/'+job_id+'/status', function(error, response, body){
    if(error){
      console.log(error)
      res.send(error)
    }else{
      body = safeParse(body)
      res.send(response.statusCode, body)
    }
  })
}

// getHistory function returns history and metrics info of supplied job_id
exports.getHistory = function(req, res) {
  console.log('getHistory function')
  var job_id = req.params.job_id
  request(Storm+'/api/job/'+job_id+'/history', function(error, response, body){
    if(error){
      console.log(error)
      res.send(error)
    }else{
      body = safeParse(body)
      console.log(body)
      res.send(response.statusCode, body)
    }
  })
}

// getErrors function returns all error data of supplied job_id
exports.getErrors = function(req, res) {
  console.log('getSeeds function')
  var job_id = req.params.job_id
  request(Storm+'/api/job/'+job_id+'/errors/', function(error, response, body){
    if(error){
      console.log(error)
      res.send(error)
    }else{
      body = JSON.parse(body)
      res.send(200, body)
    }
  })
}

// cancelJob function sends a cancel request for supplied job_id
exports.cancelJob = function(req, res) {
  console.log('cancelJob function')
  var job_list = req.body
  var job_id = {"jobs":[]}
  console.log('deleteJob function')
  _.each(job_list, function(job) {
    job_id.jobs.push({"job_id":job})
  })
  request({
      headers: {
        'Accept': 'application/json',
        'Content-type': 'application/json'
      },
      uri: Storm+'/api/jobs/cancel',
      body: JSON.stringify(job_id),
      method: 'PUT'
    }, function(error, response, body){
      if(error){
      console.log(error)
      res.send(error)
    }else{
      body = safeParse(body)
      var jobs = {}
      _.each(body, function(job){
        jobs[job.job_id] = job
      })
      res.send(response.statusCode, jobs)
    }
  })
}
/*
// deleteJob function deletes a job from storm framework and db
exports.deleteJob = function(req, res) {
  var job_list = req.body
  var job_id = {"jobs":[]}
  console.log('deleteJob function')
  _.each(job_list, function(job) {
    //console.log(job)
    job_id.jobs.push({"job_id":job})

  })
  request({
      headers: {
        'Accept': 'application/json',
        'Content-type': 'application/json'
      },
      uri: Storm+'/api/jobs/',
      method: 'POST',
      body: JSON.stringify(job_id)
    }, function(error, response, body){
      if(error){
      console.log(error)
      res.send(error)
    }else{
      body = safeParse(body)
      var jobs = {}
      _.each(body, function(job){
        jobs[job.job_id] = job
      })
      res.send(response.statusCode, jobs)
    }
  })
}
*/
// getAdapters function returns all adapters and status
exports.getAdapters = function(req, res) {
  console.log('getAdapters function')
  request(Storm+'/api/adapter', function(error, response, body){
    if(error){
      console.log('Error: '+error)
      res.send(error)
    }else{
      body = safeParse(body)
      res.send(response.statusCode, body)
    }
  })
}

// getAdapter function returns adapter information for supplied adapter_id
exports.getAdapter = function(req, res) {
  console.log('getAdapter function')
  var adapter_id = req.params.adapter_id
  request(Storm+'/api/adapter/'+adapter_id, function(error, response, body){
    if(error){
      console.log(error)
      res.send(error)
    }else{
      body = safeParse(body)
      res.send(response.statusCode, body)
    }
  })
}

// createJob function creates a new job using supplied data in body of request
exports.createJob = function(req, res) {
  console.log('createJob function')
  console.log(req.body)
  request({
      body: JSON.stringify(req.body),
      headers: {
        'Accept': 'application/json',
        'Content-type': 'application/json'
      },
      uri: Storm+'/api/job',
      method: 'POST'
    }, function(error, response, body){
      if(error){
      console.log(error)
      res.send(error)
    }else{
      body = safeParse(body)
      res.send(response.statusCode, body)
    }
  })
}

// addToJob function adds additional information to job for supplied job_id
exports.addToJob = function(req, res) {
  console.log('addToJob function')
  var job_id = req.params.job_id
  checkUser(req, res)
  request({
      body: JSON.stringify(req.body),
      headers: {
        'Accept': 'application/json',
        'Content-type': 'application/json'
      },
      uri: Storm+'/api/job/'+job_id+'/insert',
      method: 'POST'
    }, function(error, response, body){
      if(error){
      console.log(error)
      res.send(error)
    }else{
      body = safeParse(body)
      res.send(response.statusCode, body)
    }
  })
}

exports.activate_adapters = function(req, res) {
  console.log('addToJob function')
  var job_id = req.params.job_id
  checkUser(req, res)
  request({
      body: JSON.stringify(req.body),
      headers: {
        'Accept': 'application/json',
        'Content-type': 'application/json'
      },
      uri: Storm+'/api/job/'+job_id+'/execute_adapters_on_nodes',
      method: 'POST'
    }, function(error, response, body){
      if(error){
      console.log(error)
      res.send(error)
    }else{
      body = safeParse(body)
      res.send(response.statusCode, body)
    }
  })
}


exports.deleteIt = function(req, res) {
  console.log('cancel and delete job')
  var final_response = {}
  var job_list = req.body
  var job_id = {"jobs":[]}
  _.each(job_list, function(job) {
    //console.log(job)
    job_id.jobs.push({"job_id":job})

  })
  async.series([
    function(callback) {
      request({
          headers: {
            'Accept': 'application/json',
            'Content-type': 'application/json'
          },
          uri: Storm+'/api/jobs/cancel',
          body: JSON.stringify(job_id),
          method: 'PUT'
        }, function(error, response, body){
          if(error){
          console.log(error)
          callback(error)
        }else{
          //console.log(body)
          body = safeParse(body)
          var jobs = {}
          _.each(body, function(job){
            jobs[job.job_id] = job
          })
          final_response.cancel = body
          callback()
        }
      })
    },
    function(callback) {
      request({
          headers: {
            'Accept': 'application/json',
            'Content-type': 'application/json'
          },
          uri: Storm+'/api/jobs/',
          method: 'POST',
          body: JSON.stringify(job_id)
        }, function(error, response, body){
          if(error){
          console.log(error)
          callback(error)
        }else{
          //console.log(body)
          body = safeParse(body)
          var jobs = {}
          _.each(body, function(job){
            jobs[job.job_id] = job
          })
          final_response.delete = body
          callback()
        }
      })
    }], function(err){
      if(err) res.send(err)
      console.log(final_response)
      final_response = restructure(final_response)
      res.send(final_response)
    }
  )
}

exports.resetJob = function(req, res) {
  console.log('resetJob function')
  var job_list = req.body
  var job_id = {"jobs":[]}
  console.log('deleteJob function')
  _.each(job_list, function(job) {
    job_id.jobs.push({"job_id":job})
  })
  request({
      headers: {
        'Accept': 'application/json',
        'Content-type': 'application/json'
      },
      uri: Storm+'/api/jobs/cancel',
      body: JSON.stringify(job_id),
      method: 'PUT'
    }, function(error, response, body){
      if(error){
      console.log(error)
      res.send(error)
    }else{
      body = safeParse(body)
      var jobs = {}
      _.each(body, function(job){
        jobs[job.job_id] = job
      })
      res.send(response.statusCode, jobs)
    }
  })
}
