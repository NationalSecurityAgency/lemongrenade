// Lemongrenade RESTAPI command interface
// --------------------------------------
// ex. http://localhost:8080/jobs/
//
//

var express    = require('express');
var app        = express();
var bodyParser = require('body-parser');
var amqp       = require('amqp');
var jobs       = require('./routes/jobs');
var storm      = require('./routes/storm');
//var html       = require('html');

// Set Port
var port = process.env.PORT || 5050;

// Set html render
//app.set('view engine', 'html');

//app.get('/', function(req,res) {
//    res.sendfile(__dirname+'/views/index.html')
//});

app.use(bodyParser.json())

// Endpoint setup
app.get('/jobs/?*', jobs.getJobs);
app.get('/job/:job_id/', jobs.getJob);
app.get('/seeds/:job_id/', jobs.getSeeds);
app.get('/info/:job_id/', jobs.getInfo);
app.get('/job/:job_id/node/:node_id/', jobs.getNode);
app.get('/job/:job_id/edge/:edge_id/', jobs.getEdge);
app.put('/user/:job_id/add', jobs.addUser);

app.get('/ping/', storm.getPing);
app.get('/active/', storm.getActive);
app.get('/status/:job_id/', storm.getStatus);
app.get('/history/:job_id/', storm.getHistory);
app.get('/errors/:job_id/', storm.getErrors);
app.get('/adapters/', storm.getAdapters);
app.get('/adapter/:adapter_id/', storm.getAdapter);
//app.post('/delete', storm.deleteJob);
app.post('/create', storm.createJob);
app.post('/add/:job_id', storm.addToJob);
app.post('/useadapter/:job_id', storm.activate_adapters);
app.put('/cancel', storm.cancelJob);
app.post('/delete', storm.deleteIt);
app.post('/reset', storm.resetJob)

module.exports = app;
app.listen(port);
console.log('Lemongrenade API started on port: '+port);
