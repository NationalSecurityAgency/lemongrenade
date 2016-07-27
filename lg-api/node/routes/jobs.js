
var _ = require('underscore')
var http = require('http')
var request = require('request')
var querystring = require('querystring')
var async = require('async')

var Lemongraph = 'http://localhost:8001'
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



// getJobs function returns an information list of all jobs in the db
exports.getJobs = function(req, res) {
  console.log('getJobs function')
  var query = '?'+querystring.stringify(req.query)
  console.log(query)
  request({
    uri: Lemongraph+'/graph'+query,
    method: 'GET'
  }, function(error, response, body){
    if(error){
      console.log(error)
      res.send(error)
    } else {
      body = safeParse(body)
      if(querystring.stringify(req.query)){
        var jobList = []
        _.each(body, function(job){
          if(job.graph && job.meta){
            var entry = {
              job_id: job.graph,
              config: job.meta,
              size: job.size
            }
            jobList.push(entry)
          }
        })
        if(jobList.length) {
          res.send(jobList)
        }else{
          res.send(body)
        }
      } else {
      var jobList = []
      _.each(body, function(job){
        var entry = {
          job_id: job.graph,
          config: job.meta,
          size: job.size
        }
        jobList.push(entry)
      })
      res.send(jobList)
      }
    }
  })
}

// getJob function returns job information by supplied job_id
exports.getJob = function(req, res) {
  console.log('getJob function')
  var job_id = req.params.job_id
  async.parallel([
    function(callback) {
      var chunks = []
      request(Lemongraph+'/graph/'+job_id, function(error, response, body){
        console.log('ERROR: '+error)
        console.log(response.statusCode)
      }).on('error', function(err){
        console.log('so much error')
        res.send(err)
      }).on('response', function(chunk){
        console.log('got the data')
        res.set(chunk.headers)
        res.removeHeader("Content-Length")
        chunk.on('data', function(data) {
          console.log('end of data')
          chunks.push(data)
        })
      }).on('end', function(end){
        console.log('end function')
        var info  = Buffer.concat(chunks)
        info = safeParse(info.toString())
        console.log(info)
        var job = {
          "job_id": info.graph,
          "config": info.meta,
          "nodes": info.nodes,
          "edges": info.edges,
          "maxID": info.maxID,
          "size": info.size
        }
        callback(null, job)
      })
    },
    function(callback) {
      request(Storm+'/api/job/'+job_id+'/status', function(error, response, body){
        if(error){
          console.log(error)
          res.send(error)
        }else{
          body = safeParse(body)
          callback(null, body)
        }
      })

    },
    function(callback) {
      console.log('getSeeds function')
      var job_id = req.params.job_id
      request(Storm+'/api/job/'+job_id+'/errors/', function(error, response, body){
        if(error){
          console.log(error)
          res.send(error)
        }else{
          body = JSON.parse(body)
          callback(null,body)
        }
      })

    }], function(err, results){
      if(err) res.send(err)
      //console.log(results)
      results = _.extend(results[0],results[1],results[2])
      res.send(results)
    }
  )
}

var util = require('util');
var util_options = {
  showHidden: true,
  depth: null
};

// addUser function adds user information to existing job
exports.addUser = function(req, res) {
  var job_id = req.params.job_id
  var userList = new Object()
  _.each(req.body, function(v,k){
    userList[k] = v
  })

  userList = {"roles":userList}
  request({
    uri: Lemongraph+'/graph/'+job_id+'/meta',
	body : JSON.stringify(userList),
	headers: {
	  'Accept': 'application/json',
	  'Content-type': 'application/json'
	},
    method: 'PUT'
  }, function(error, response, body){
    if(error){
	  console.log(`[addUser] error: ${util.inspect(error, util_options)}`);
      res.send(error)
    }else{
      body = safeParse(body)
      res.send(JSON.stringify(body))
    }
  })
}

// getSeeds function returns all job seed data of supplied job_id
exports.getSeeds = function(req, res) {
  console.log('getSeeds function')
  var job_id = req.params.job_id
  request(Lemongraph+'/graph/'+job_id+'/seeds/', function(error, response, body){
    if(error){
      console.log(error)
      res.send(error)
    }else{
      body = JSON.parse(body)
      res.send(200, body)
    }
  })
}

// getInfo function returns job metadata by supplied job_id
exports.getInfo = function(req, res) {
  console.log('getInfo function')
  var job_id = req.params.job_id
  async.parallel([
    function(callback) {
      request(Lemongraph+'/graph/'+job_id+'/status/', function(error, response, body){
        if(error){
          console.log(error)
          res.send(error)
        }else{
          body = safeParse(body)
          //console.log(body)
          //res.set(response.headers)
          var job = {
            "job_id": body.graph,
            "config": body.meta,
            "maxID": body.maxID,
            "size": body.size
          }
          callback(null, job)
        }
      })
    },
    function(callback) {
      request(Storm+'/api/job/'+job_id+'/status', function(error, response, body){
        if(error){
          console.log(error)
          res.send(error)
        }else{
          body = safeParse(body)
          callback(null, body)
        }
      })

    },
    function(callback) {
      console.log('getSeeds function')
      var job_id = req.params.job_id
      request(Storm+'/api/job/'+job_id+'/errors/', function(error, response, body){
        if(error){
          console.log(error)
          res.send(error)
        }else{
          body = JSON.parse(body)
          callback(null,body)
        }
      })

    }], function(err, results){
      if(err) res.send(err)
      results = _.extend(results[0],results[1],results[2])
      res.send(results)
    }
  )
}

// getNode function returns node data by node_id
exports.getNode = function(req, res) {
  console.log('getNode function')
  var job_id = req.params.job_id
  var node_id = req.params.node_id
  request(Lemongraph+'/graph/'+job_id+'/node/'+node_id, function(error, response, body){
    if(error){
      console.log(error)
      res.send(error)
    }else{
      body = JSON.parse(body)
      res.send(200, body)
    }
  })

}

// getEdge function returns node data by edge_id
exports.getEdge = function(req, res) {
  console.log('getEdge function')
  var job_id = req.params.job_id
  var edge_id = req.params.edge_id
  request(Lemongraph+'/graph/'+job_id+'/edge/'+edge_id, function(error, response, body){
    if(error){
      console.log(error)
      res.send(error)
    }else{
      body = JSON.parse(body)
      res.send(200, body)
    }
  })
}
