var lemonviewApp = angular.module('lemonviewApp', ['ngRoute','ngResource', 'chart.js'])

lemonviewApp.config(function($routeProvider) {
    $routeProvider

    .when('/', {
	templateUrl : 'home.html',
	controller  : 'mainController'
    })
    .when('/jobs', {
	templateUrl : 'jobs.html',
	controller  : 'mainController'
    })
    .when('/job/:jobid', {
	templateUrl : 'job.html',
	controller  : 'jobController'
    })
    .when('/joberrors/:jobid', {
	templateUrl : 'joberrors.html',
	controller  : 'jobController'
    })

    .when('/jobmetrics/:jobid', {
	templateUrl : 'view/jobmetric.html',
	controller  : 'jobMetricController'
    })
    .when('/adapters', {
	templateUrl : 'adapters.html',
	controller  : 'adapterController'
    })
    .when('/metrics', {
	templateUrl : 'metrics.html',
	controller  : 'metricsController'
    })
});

lemonviewApp.factory('dataService', function($http) {
    return {
	getJobsLast: function() {
	    return $http.get('/api/jobs/last/50').then(function(result) {
		return result.data;
            });
        },
	getAdapters: function() {
	    return $http.get('/api/adapter').then(function(result) {
		console.log(angular.toJson(result.data))
		return result.data;
            });
        },
	getAdaptersNames: function() {
	    return $http.get('/api/adapter/names').then(function(result) {
		//console.log(angular.toJson(result.data))
		return result.data;
            });
        },
	getMetrics: function() {
	    return $http.get('/data/metrics.json').then(function(result) {
		//console.log(angular.toJson(result.data))
		return result.data;
            });
        },
	getJob: function(job) {
	    return $http.get('/api/job/'+job+'/full').then(function(result) {
		//console.log(angular.toJson(result.data))
		return result.data;
            });
        },
	getJobMetrics: function(job) {
	    return $http.get('/api/job/'+job+'/metrics').then(function(result) {
		//console.log(angular.toJson("LVL1"+angular.toJson(result.data)));
		return result.data;
            });
        },

	getConfig: function() {
	    return $http.get('/config').then(function(result) {
		return result.data;
            });
        }
    }
});

lemonviewApp.filter('ageFilter', function() {
    return function(inputraw) {
	var input = Math.floor(inputraw);
	function z(n) { return ( n < 10 ? '0': '') + n;}
	var seconds = input % 60;
	var minutes = Math.floor(input % 3600 / 60);
	var hours   = Math.floor(input / 3600);
	return (z(hours) + ':' + z(minutes) + ':' + z(seconds));
    }
});


lemonviewApp.factory('lemongrenadeRestService', function($http, $resource) {
    return {

	retryJob: function(id) {
	    call_url = "/api/job/"+id+"/retry";
	    var dataObj = {};
	    return $http({
		method: 'PUT',
                headers: {
                    'Content-Type':'application/json; charset=utf-8',
                    'Accept':'application/json; charset=utf-8',
                    'Accept-Charset':'application/json; charset=utf-8'

                },
		withCredentials: true,
		url: call_url,
                dataType:"json",
		data: { data: angular.toJson(dataObj)}
	    })
	    .success(function(data, status, header, config) {
		//alert("Job Retry Started");
	    })
	    .error(function(data, status, header, config) {
		alert("Error retrying job! "+data.error);
	    });
	},

	retryJobByTaskId: function(id,taskId) {
	    call_url = "/api/job/"+id+"/retry/"+taskId;
	    var dataObj = {};
	    return $http({
		method: 'PUT',
                headers: {
                    'Content-Type':'application/json; charset=utf-8',
                    'Accept':'application/json; charset=utf-8',
                    'Accept-Charset':'application/json; charset=utf-8'

                },
		withCredentials: true,
		url: call_url,
                dataType:"json",
		data: { data: angular.toJson(dataObj)}
	    })
	    .success(function(data, status, header, config) {
		//alert("Job Retry Started");
	    })
	    .error(function(data, status, header, config) {
		alert("Error retrying job! "+data.error);
	    });
	},

	deleteJob: function(id) {
	    url = "/api/job/"+id;
	    return $http({
		method: 'DELETE',
		withCredentials: true,
		url: server
	    })
	    .success(function(data, status, header, config) {
		alert("Job Deleted!");
	    })
	    .error(function(data, status, header, config) {
		alert("Error deleting job! "+data.error);
	    });
	},

	deleteJobBulk: function(body) {
	    url = "/api/jobs";
	    return $http({
		method: 'DELETE',
		withCredentials: true,
		headers: {
		    "Content-Type": "application/json"
		},
		url: url,
		data: body
	    })
	    .success(function(data, status, header, config) {
		alert("Jobs Deleted! "+ angular.toJson(data));
	    })
	    .error(function(data, status, header, config) {
		alert("Error deleting job! "+angular.toJson(data));
	    });
	},

	stopJob: function(server, id) {
	    return $http({
		method: 'PUT',
		withCredentials: true,
		url: server
	    })
	    .success(function(data, status, header, config) {
		alert("Job STOPPED! "+angular.toJson(data));
	    })
	    .error(function(data, status, header, config) {
		alert("Error stopping job! Status:"+status+" Message:"+angular.toJson(data));
	    });
	},

	stopJobBulk: function(body) {
	    url = "/api/jobs/cancel";
	    return $http({
		method: 'PUT',
		withCredentials: true,
		headers: {
		    "Content-Type": "application/json"
		},
		url: url,
		data: body
	    })
	    .success(function(data, status, header, config) {
		alert("Jobs Deleted! "+ angular.toJson(data));
	    })
	    .error(function(data, status, header, config) {
		alert("Error deleting job! "+angular.toJson(data));
	    });
	},

	getEntireGraph: function(server,id) {
	    var call_url = ""+server+"/graph/"+id+"/";
	    return $http({
		method: 'GET',
		withCredentials: true,
		url: call_url
	    })
	    .success(function(data, status, header, config) {
		
	    })
	    .error(function(data, status, header, config) {
		// todo?
	    });
	},

	getGraphNode: function(server,id,node) {
	    var call_url = ""+server+"/graph/"+id+"/"+node+"/";
	    return $http({
		method: 'GET',
		withCredentials: true,
		url: call_url
	    })
	    .success(function(data, status, header, config) {
		
	    })
	    .error(function(data, status, header, config) {
		// todo?
	    });
	},

	getGraphNodeDepth: function(server,id,node,depth) {
	    var call_url = ""+server+"/graph/"+id+"/"+node+"/"+depth+"/";
	    return $http({
		method: 'GET',
		withCredentials: true,
		url: call_url
	    })
	    .success(function(data, status, header, config) {
		
	    })
	    .error(function(data, status, header, config) {
		// todo?
	    });
	},

	getAdapters: function(server) {
	    var call_url = ""+server+"/adapters/";
	    return $http({
		method: 'GET',
		withCredentials: true,
		url: call_url
	    })
	    .success(function(data, status, header, config) {
		
	    })
	    .error(function(data, status, header, config) {
		// todo?
	    });
	},

	getMetrics: function(server) {
	    alert("GET METRICS");
	    var call_url = ""+server+"/metrics.json";
	    return $http({
		method: 'GET',
		withCredentials: true,
		url: call_url
	    })
	    .success(function(data, status, header, config) {
		//console.log("SUCCESS");
		
	    })
	    .error(function(data, status, header, config) {
		//console.log("FAIL");
		// todo?
	    });
	},

	getJobInfo: function(server, id) {
	    var call_url = ""+server+"/info/"+id+"/";
	    return $http({
		method: 'GET',
		withCredentials: true,
		url: call_url
	    })
	    .success(function(data, status, header, config) {
		
	    })
	    .error(function(data, status, header, config) {
		alert("ERROR"+angular.toJson(data));
		// todo?
	    });
	},

	getGraph: function(server, id) {
	    var call_url = ""+server+"/graph/"+id+"/?pattern=";
	    $http({
		method: 'GET',
		withCredentials: true,
		url: call_url
	    })
	    .success(function(data, status, header, config) {
		//alert("Job Deleted");
		//alert("GRAPH"+angular.toJson(data));
	    })
	    .error(function(data, status, header, config) {
		alert("Error getting graph "+id);
	    });
	},

	getInfo: function(server, id) {
	    var call_url = ""+server+"/info/"+id+"/";
	    $http({
		method: 'GET',
		withCredentials: true,
		url: call_url
	    })
	    .success(function(data, status, header, config) {
		//alert("Info"+angular.toJson(data));
	    })
	    .error(function(data, status, header, config) {
		alert("Error getting graph "+id);
	    });
	},

	getJobs: function(server) {
	    var call_url = ""+server+"/jobs/";
	    return $http({
		method: 'GET',
		withCredentials: true,
		url: call_url
	    })
	    .success(function(data, status, header, config) {
		//alert("jobs:"+angular.toJson(data));
	    })
	    .error(function(data, status, header, config) {
		alert("Error getting jobs ");
	    });
	},

	createJob: function(server, job) {
	    alert("CREATE JOB 2")
	    var apromise = this.getAdapters(server);
	    apromise.then(function(result) {
                alert("Got adapters "+angular.toJson(result));
	    },
            function(result) {
                alert("Got adapters fail"+angular.toJson(result));
	    }); 

	    var call_url = ""+server+"/job/create";
	    var actions  = [];
	    var seeds    = [];

            alert("create 1"+angular.toJson(job.adapters));
	    angular.forEach(job.adapters, function(avalue, adapter){
		alert("WTF :"+avalue+" " +adapter);
		actions.push(action);
	    });

            console.log("ACTIONS:"+angular.toJson(actions));

	    var dataObj  = {
		"description"   : job.description,
		"depth"  : job.depth,
		"status" : "NEW",
		"seed"   : job.seeddata,
		"adapters": actions
	    };

	    console.log(angular.toJson(dataObj));

	    return $http({
		method: 'PUT',
                headers: {
                    'Content-Type':'application/json; charset=utf-8',
                    'Accept':'application/json; charset=utf-8',
                    'Accept-Charset':'application/json; charset=utf-8'

                },
		withCredentials: true,
		url: call_url,
                dataType:"json",
		data: { data: angular.toJson(dataObj)}
	    })
	    .success(function(data, status, header, config) {
		//alert("Job Created");
	    })
	    .error(function(data, status, header, config) {
		alert("Error Creating  job!");
	    });

	}
    };
});

lemonviewApp.controller('adapterController', function($scope, $timeout, $window, $q, $log, dataService, lemongrenadeRestService) {
    $scope.message = 'main message';
    $scope.showInactiveAdapter = false;
    $scope.showJobsActive      = true;
    $scope.showJobsQueued      = true;
    $scope.showJobsFinished    = true;

    $scope.data = [];

    $scope.isDataEmpty = function() {
	var size = Object.keys($scope.data).length;
	if (size == 0) return true;
	return false;
    };

    var poller = function() {
	dataService.getAdapters().then(function(data) {
	    $scope.data = data;
	    $timeout(poller, 5000); 
	    $scope.loading = false;
	}, function errorError(response) {
	    $timeout(poller, 5000); 
	    $scope.loading = false;  /* we only show 'loading' page on first load */
	});
    };
    poller();


});

lemonviewApp.controller('metricsController', function($scope, $timeout, $window, $q, $log, dataService, lemongrenadeRestService) {
    $scope.message = 'Metrics';
    $scope.adapters = [];
    $scope.data_top_ten_users_by_job_submitted = {};
    $scope.labels = [ "1", "2", "3", "4","5","6","7","8","9","10","11","12", "13","14","15","16","17","18","19","20","21","22","23", "24", "25", "26", "27", "28","29", "30"];
    $scope.labels_hour = [ "1", "2", "3", "4","5","6","7","8","9","10","11","12", "13","14","15","16","17","18","19","20","21","22","23", "24"];
    $scope.series = ['Seriea A'];
    $scope.series_graph_activity = ['Min','Max','Avg'];
    $scope.series_graph_tasks_spawned = ['Min','Max','Avg'];
    $scope.data_graphactivity = [ [0] ];
    $scope.data_job_count_by_hour = [[0]];

    $scope.onClick = function(points,out) {
	console.log(points,out);
    };

    $scope.datasetOverride = [{ yAxisId: 'y-axis-1' }];
    $scope.options = {
	scales: {
	    yAxes: [
		{ id:'y-axis-1',
		  type: 'linear',
		  display: true,
		  position:'left'
		  }]
	    }
    };

    // Adapters


    // Dynamically build adapter_avg_runtime_day
    $scope.adapterAvgRunTimeDayList = [];
    var adapterAvgRuntimeBuild = function(data) {
	for(var i=0; i< data.adapter_avg_runtime_day.length; i++) {
	    //console.log( "WT1:"+data.adapter_tasks_per_day[i].adapter);
	    //console.log( "WT2:"+data.adapter_tasks_per_day[i].data);
	    $scope.chartConfig = {
		name: data.adapter_avg_runtime_day[i].adapter,
		options: {
		    chart: { 
			type: 'line'
		    }
		},
		data: [data.adapter_avg_runtime_day[i].data]
	     }
	     $scope.adapterAvgRunTimeDayList.push($scope.chartConfig);
        }
    };

    // Adapter Errors PEr Day
    $scope.adapterErrorsPerDayList = [];
    var adapterErrorsPerDayBuild = function(data) {
	for(var i=0; i< data.adapter_errors_per_day.length; i++) {
	    $scope.chartConfig = {
		name: data.adapter_errors_per_day[i].adapter,
		options: {
		    chart: { 
			type: 'line'
		    }
		},
		data: [data.adapter_errors_per_day[i].data]
	     }
	     $scope.adapterErrorsPerDayList.push($scope.chartConfig);
        }
    };

    // Adapter tasks per day
    $scope.adapterTasksPerDayList = [];
    var adapterTasksPerDayBuild = function(data) {
	for(var i=0; i< data.adapter_tasks_per_day.length; i++) {
	    $scope.chartConfig = {
		name: data.adapter_tasks_per_day[i].adapter,
		options: {
		    chart: { 
			type: 'line'
		    }
		},
		data: [data.adapter_tasks_per_day[i].data]
	     }
	     $scope.adapterTasksPerDayList.push($scope.chartConfig);
        }
    };


    // Adapter tasks SPAWNED per day
    $scope.adapterTasksSpawnedPerDayList = [];
    var adapterTasksSpawnedPerDayBuild = function(data) {
	for(var i=0; i< data.adapter_tasks_spawned_per_day.length; i++) {
	    $scope.chartConfig = {
		name: data.adapter_tasks_spawned_per_day[i].adapter,
		options: {
		    chart: { 
			type: 'line'
		    }
		},
		data: [data.adapter_tasks_spawned_per_day[i].avg, 
		       data.adapter_tasks_spawned_per_day[i].min,
		       data.adapter_tasks_spawned_per_day[i].max ]
	     }

	     $scope.adapterTasksSpawnedPerDayList.push($scope.chartConfig);
        }
    };

/*
    var adapterAvgRuntimeBuild = function() {
	for(var i=0; i< a.length; i++) {
	    console.log("A"+i);
	    $scope.chartConfig = {
		options: {
		    chart: { 
			type: 'line'
		    }
		},
		data: [a[i]],
	     }
	     $scope.adapterChartList.push($scope.chartConfig);
        }
    };

*/
    var poller = function() {
	dataService.getMetrics().then(function(data) {
	    $scope.data = data;
	    adapterAvgRuntimeBuild(data);
	    adapterErrorsPerDayBuild(data);
	    adapterTasksPerDayBuild(data);
	    adapterTasksSpawnedPerDayBuild(data);
	    $scope.adapters = data.adapters;
	    $scope.data_jobs_per_day= [data.jobs_per_day];
	    $scope.data_tasks_per_day= [data.task_total_per_day];
	    $scope.data_errors_per_day= [data.errors_per_day];
	    $scope.data_job_count_by_hour = [data.job_count_by_hour];
	    $scope.data_top_ten_users_by_job_submitted = data.top_users_by_job_submitted
	    $scope.graph_activity_per_day = [ data.graph_activity_min, data.graph_activity_max, data.graph_activity_avg]
	    $scope.loading = false;
	}, function errorError(response) {
	    $scope.loading = false;  
	});
      };
     poller();
});

lemonviewApp.controller('jobMetricController', function($scope, $timeout, $window, $q, $log, dataService, lemongrenadeRestService,  $routeParams) {
    $scope.jobid   = $routeParams.jobid;
    $scope.message = 'Job Metrics';
    $scope.data_graph_changes_per_task = {};
    $scope.labels = [ "1", "2", "3", "4","5","6","7","8","9","10","11","12", "13","14","15","16","17","18","19","20","21","22","23", "24", "25", "26", "27", "28","29", "30"];
    $scope.pie_adapter_labels = [];
    $scope.series_graph_activity = ['Min','Max','Avg'];
    $scope.onClick = function(points,out) {
	console.log(points,out);
    };

    $scope.datasetOverride = [{ yAxisId: 'y-axis-1' }];
    $scope.options = {
	scales: {
	    yAxes: [
		{ id:'y-axis-1',
		  type: 'linear',
		  display: true,
		  position:'left'
		  }]
	    }
    };

    var poller = function() {

	dataService.getJobMetrics($scope.jobid).then(function(data) {
	    $scope.data = data;
	    $scope.data_graph_changes_per_task = [data.graph_changes_per_task];
	    $scope.data_pie_adapter = data.adapter_pie;
	    $scope.pie_adapter_labels = data.adapter_pie_labels;

	    var count = 0;
	    $scope.labels = [];
	    for (var job in data.graph_changes_per_task) {
		$scope.labels[count] = count+1;
		count++;
	    }
	    $scope.loading = false;
	}, function errorError(response) {
	    $scope.loading = false;  
	});
      };
     poller();


});

lemonviewApp.filter('orderObjectByDate', function() {
    return function(items, field, reverse) {
	var filtered = [];
	angular.forEach(items, function(item) {
	    filtered.push(item);
	});
	filtered.sort(function (a,b) {
	    var dateA = new Date(a[field]);
	    var dateB = new Date(b[field]);
	    return (dateA > dateB ? 1 : -1);
	});
	if(reverse) filtered.reverse();
	return filtered;
    };
});

lemonviewApp.controller('mainController', function($scope, $timeout, $window, $q, $log, dataService, lemongrenadeRestService) {
    $scope.message = 'main message';

    $scope.data = [];
    $scope.loading = true;
    $scope.showJobModal = false;
    $scope.showJobModalId = 0;

    $scope.showJobModalText       = '';
    $scope.showDeleteAllJobsModal = false;
    $scope.showStopAllJobsModal   = false;
    $scope.showRetryJobModal      = false;
    $scope.showRetryJobModalId   = 0;
    $scope.showDeleteJobModal     = false;
    $scope.showDeleteJobModalId   = 0;
    $scope.showStopJobModal       = false;
    $scope.showStopJobModalId     = 0;
    $scope.showCreateJobModal     = false;
    $scope.deleteInProgress       = false;
    $scope.retryInProgress        = false;
    $scope.adapterCount = 0;
    $scope.newjob = [];
    $scope.newjob['depth'] = 3;


    /** Looks for a user name with owner status in :  { \"roles\": {\"alice\" : { \"owner\": true, \"reader\": true} }} "); **/
    $scope.findUser = function(tmpobj) {
	if (tmpobj == undefined) { return "unknown"; }
	if (tmpobj.hasOwnProperty('roles')) {
	    for (var key in tmpobj.roles) {
		var user = tmpobj.roles[key];
		if (user.hasOwnProperty('owner')) {
		    if (user.owner == true) {
			return key;
		    }
		}
	    }
	}
	return "unknown";
    };

    $scope.dateDiff = function(dateA, dateB) {
	var a = new Date(dateA);
	var b = new Date(dateB);
	return Math.floor((b-a) / 1000)
    };

    $scope.getAdapterCount = function(obj) {
	var count = 0;
	angular.forEach(obj, function(adapter){
	    //console.log(adapter);
	    count++;
	});
	return count;
    };

    $scope.toggleDeleteJobModal = function(id) {
	$scope.showDeleteJobModalId = id;
	$scope.showDeleteJobModal = !$scope.showDeleteJobModal;
    };

    $scope.toggleRetryJobModal = function(id) {
	$scope.showRetryJobModalId = id;
	$scope.showRetryJobModal = !$scope.showRetryJobModal;
    };

    $scope.toggleStopJobModal = function(id) {
	$scope.showStopJobModalId = id;
	$scope.showStopJobModal = !$scope.showStopJobModal;
    };

    $scope.toggleDeleteAllJobsModal = function() {
	$scope.showDeleteAllJobsModal = !$scope.showDeleteAllJobsModal;
    };

    $scope.toggleStopAllJobsModal = function() {
	$scope.showStopAllJobsModal = !$scope.showStopAllJobsModal;
    };

    $scope.toggleCreateJobModal = function() {
	$scope.showCreateJobModal = !$scope.showCreateJobModal;
    };

    $scope.toggleJobModal = function(id,text) {
	$scope.showJobModalId = id;
	$scope.showJobModalText = text;
	$scope.showJobModal = !$scope.showJobModal;
    };

    $scope.deleteJob = function(id) {
	$scope.deleteInProgress = true;
	var deletePromise = lemongrenadeRestService.deleteJob(id);
	deletePromise.then(function(result) {
	    $scope.deleteInProgress = false;
        }, function(result) {
	    $scope.deleteInProgress = false;
	});
    };

    $scope.retryJob = function(id) {
	$scope.retryInProgress = true;
	var retryPromise = lemongrenadeRestService.retryJob(id);
	retryPromise.then(function(result) {
	    $scope.retryInProgress = false;
        }, function(result) {
	    $scope.retryInProgress = false;
	});
    };


    $scope.stopJob = function(id) {
	$scope.stopInProgress = true;
	var url = "/api/job/"+id+"/cancel";
	var stopPromise = lemongrenadeRestService.stopJob(url,id);
	stopPromise.then(function(result) {
	    $scope.stopInProgress = false;
        }, function(result) {
	    $scope.stopInProgress = false;
	});
    };

    $scope.deleteAllJobs = function() {
	$scope.deleteInProgress = true;
	$scope.jobsToDelete = [];
	for (var jobid in $scope.data) {
	    var job = {
		"job_id": jobid
	    };
	    $scope.jobsToDelete.push(job);
	}
	$scope.jobs = {
	    "jobs" : $scope.jobsToDelete
	};
	var deletePromise = lemongrenadeRestService.deleteJobBulk(angular.toJson($scope.jobs));
	deletePromise.then(function(result) {
	    $scope.deleteInProgress = false;
        }, function(result) {
	    $scope.deleteInProgress = false;
	});
    };

    $scope.stopAllJobs = function() {
	$scope.deleteInProgress = true;
	$scope.jobsToStop = [];
	for (var jobid in $scope.data) {
	    var job = {
		"job_id": jobid
	    };
	    $scope.jobsToStop.push(job);
	}
	$scope.jobs = {
	    "jobs" : $scope.jobsToStop
	};
	var deletePromise = lemongrenadeRestService.stopJobBulk(angular.toJson($scope.jobs));
	deletePromise.then(function(result) {
	    $scope.deleteInProgress = false;
        }, function(result) {
	    $scope.deleteInProgress = false;
	});
    };



    $scope.rerunJob = function(id) {
	// If in local store, use that instead of rebuilding from lg data
	var jobInfo = []
	for (var job in $scope.data.jobs) {
	    //console.log(angular.toJson($scope.data.jobs[job]));
	    var jobid = $scope.data.jobs[job]['job_id'];
	    if (jobid == id) {
		jobInfo = $scope.data.jobs[job];
		break;
	    }
	}
	if (jobInfo.length == 0) {
	    alert("Unable to find job information!");
	}
	var j = {};
	j['description']    = jobInfo['description'];
	j['seed'] = jobInfo['seed'].split("\n");
	j['depth']    = jobInfo['depth'];
	j['adapters'] = {};
	
	// need to re-assemeble the actions for input based on what lemongrenade knows
	var actions  = jobInfo['actions'];
	for (var aindex in actions) {
	    var action = actions[aindex];
	    var func    = action['defaults'];
	    for (var findex in func) {
		var functionlist = {};
		for (var f2 in func[findex]) {
		    console.log(f2);
		    functionlist[f2] = true;
		}
		j['adapters'][findex] = { "functions" :  functionlist }
	    }
	}
	$scope.newjob = [];
	$scope.newjob.description   = j['description']
	$scope.newjob.depth         = j['depth']
	$scope.newjob.adapters      = j['adapters']
	$scope.showCreateJobModal   = true;
	// This was the old way, now we re-open the createJObWindow
	//var address  = $scope.data.config.registry.address;
	//var port     = $scope.data.config.registry.port;
	//var port     = 8050;
	//var protocol = $scope.data.config.registry.protocol;
	//var server   = protocol+"://"+address+":"+port;
	//lemongrenadeRestService.createJob(server,j);
    };

    $scope.getGraph = function(id) {
	var address  = $scope.data.config.registry.address;
	var port     = $scope.data.config.registry.port;
	var port     = 8050;
	var protocol = $scope.data.config.registry.protocol;
	var server   = protocol+"://"+address+":"+port;
	lemongrenadeRestService.getInfo(server,id);
	lemongrenadeRestService.getGraph(server,id);
    };

    $scope.openGraphWindow = function(id) {
	var address  = $scope.data.config.registry.address;
	var port     = $scope.data.config.registry.port;
	var port     = 8050;
	var protocol = $scope.data.config.registry.protocol;
	var server   = protocol+"://"+address+":"+port;
	var url = server+"/?id="+id;
	
	$window.open(url, 'Graph '+id, 'width=1000,height=800');
    };

    $scope.createJob = function(j) {
	$scope.titlerequired          = false;
	$scope.depthrequired          = false;
	$scope.missingadapter         = false;
	$scope.adapterrequired        = false;
	var valid = true;
	if (angular.isUndefined(j.description)) {
	    $scope.descriptionrequired = true;
	    valid = false;
	}
	if (angular.isUndefined(j.seeddata)) {
	    $scope.seeddatarequired = true;
	    valid = false;
	}
	if (angular.isUndefined(j.depth) || !isFinite(j.depth)) {
	    $scope.depthrequired = true;
	    valid = false;
	}
	if (angular.isUndefined(j.adapters)) {
	    $scope.missingadapter = true;
	    valid = false;
	} else {
	    alert(angular.toJson(j.adapters));
	    var acount = Object.keys(j.adapters).length;
	    if (acount <= 0) {
		$scope.missingadapter = true;
		valid = false;
	    }
	}
	if (valid) {
	    var address  = $scope.data.config.registry.address;
	    var port     = $scope.data.config.registry.port;
	    var port     = 8050;
	    var protocol = $scope.data.config.registry.protocol;
	    var server   = protocol+"://"+address+":"+port;
	    console.log("server"+server+"  job:"+angular.toJson(j.adapters));
	    lemongrenadeRestService.createJob(server,j);
	    $('modal').hide();
	    $scope.showCreateJobModal = !$scope.showCreateJobModal;
       }
    };


    $scope.isDataEmpty = function() {
	var size = Object.keys($scope.data).length;
	if (size == 0) return true;
	return false;
    };

    var poller = function() {
	dataService.getAdaptersNames().then(function(data) {
	    $scope.adapters = data;
	    $scope.adapterCount = $scope.getAdapterCount(data);
	}, function errorError(response) {
	});

	dataService.getJobsLast().then(function(data) {
	    $scope.data = data;
	    $timeout(poller, 5000); 
	    $scope.loading = false;
	}, function errorError(response) {
	    $timeout(poller, 5000); 
	    $scope.loading = false;  /* we only show 'loading' page on first load */
	});
    };
    poller();
});

lemonviewApp.directive('modal', function() {
    return {
	template: 
	'<div class="modal fade">' +
	    '<div class="modal-dialog">' +
	        '<div class="modal-content">' +
	            '<div class="modal-header">' +
	            '<button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times;</button>' +
	            '<h4 class="modal-title">{{title}}</h4>' +
	            '</div>' +
	            '<div class="modal-body" ng-transclude></div>' +
	        '</div>' +
	    '</div>' +
	'</div>',
	restrict: 'E',
	transclude: true,
	replace: true,
	scope: true,
	link: function postLink(scope, element, attrs) {
	    scope.title = attrs.title;
	    scope.$watch(attrs.visible, function(value) {
		if (value == true)
		    $(element).modal('show');
		else 
		    $(element).modal('hide');
	    });

	    $(element).on('shown.bs.modal', function() {
		scope.$apply(function() {
		    scope.$parent[attrs.visible] = true;
		});
	    });

	    $(element).on('hidden.bs.modal', function() {
		scope.$apply(function() {
		    scope.$parent[attrs.visible] = false;
		});
	    });
	}
    };
});

lemonviewApp.controller('infoController', function($scope, $timeout, $log, dataService) {
    $scope.configdata = [];
    $scope.connected = false;
    
    var infopoller = function() {
	dataService.getConfig()
	.then(function(data) {
	    $scope.configdata = data;
	    $scope.connected  = true;
	    $timeout(infopoller, 5000); 
	}, function Error(response) {
	    $scope.connected = false;
	    $timeout(infopoller, 5000); 
	});
    };
    infopoller();
});

lemonviewApp.directive('bsPopover', function() {
    return function(scope, element, attrs) {
	element.find("a[rel=popover]").popover({ placement: 'bottom', html: 'true'});
    };
});

lemonviewApp.controller('jobController', function($scope, $timeout, $window, $q, $log, dataService, lemongrenadeRestService, $routeParams) {

    /** Looks for a user name with owner status in :  { \"roles\": {\"alice\" : { \"owner\": true, \"reader\": true} }} "); **/
    $scope.findUser = function(tmpobj) {
	if (tmpobj == undefined) { return "unknown"; }
	if (tmpobj.hasOwnProperty('roles')) {
	    for (var key in tmpobj.roles) {
		var user = tmpobj.roles[key];
		if (user.hasOwnProperty('owner')) {
		    if (user.owner == true) {
			return key;
		    }
		}
	    }
	}
	return "unknown";
    }

    $scope.message = 'main message';
    $scope.job    = [];
    $scope.loading = true;
    $scope.jobid   = $routeParams.jobid;


//scope.toggleRetryJobByTaskModal(job,id)
//showRetryJobByTaskModal
//showRetryJobModalId,showRetryJobByTaskModalTaskId



    $scope.showRetryJobByTaskModal   = false;
    $scope.showRetryJobByModalId     = 0 ;
    $scope.showRetryJobByModalTaskId = 0;
    $scope.toggleRetryJobByTaskModal = function(id,taskId) {
	$scope.showRetryJobModalId = id;
	$scope.showRetryJobModalTaskId = taskId;
	$scope.showRetryJobByTaskModal = !$scope.showRetryJobByTaskModal;
    };

    $scope.retryJobByTaskId = function(id,taskId) {
	$scope.retryInProgress = true;
	var retryPromise = lemongrenadeRestService.retryJobByTaskId(id,taskId);
	retryPromise.then(function(result) {
	    $scope.retryInProgress = false;
        }, function(result) {
	    $scope.retryInProgress = false;
	});
	$scope.showRetryJobByTaskModal = !$scope.showRetryJobByTaskModal;
    };



    var jobpoller = function() {
	dataService.getJob($scope.jobid)
	.then(function(data) {
	    $scope.job = data
	    $scope.user = $scope.findUser(data.job_config);
	    $scope.tasks = data.tasks
	    $scope.loading  = false;
	    $timeout(jobpoller, 25000); 
	}, function Error(response) {
	    $scope.loading  = true;
	    $timeout(jobpoller, 25000); 
	});
    };
    jobpoller();
});

