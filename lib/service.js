var freebase = require('freebase');
var freebase_client = freebase.client;
var elasticsearch = require('elasticsearch');
var async = require('async');
var logger = require('./logger');

module.exports = {
	initialize:function(config, done){

		try{

			var fbport = config.freebase_port?config.freebase_port:8000;
		 	var fbhost = config.freebase_host?config.freebase_host:'localhost';
		 	var fbsecret = config.freebase_secret?config.freebase_secret:'test_secret';
		 	var elastichosts = config.elastic_hosts?config.elastic_hosts:['localhost:9200'];
		 	
		 	var elasticClient = elasticsearch.Client({
			  hosts: elastichosts
			});

			elasticClient.cluster.health(function (err, resp) {
			  if (err) {
			    done(err.message);
			  } else {

			    logger.initialize({elasticClient:elasticClient, logToConsole:true});
			    logger.log('info', 'elastic client connected', null);

			    freebase_client.newClient({host:fbhost, port:fbport, secret:fbsecret}, function(e, fbclient){

			    	if (!e){

			    		fbclient.onAll(function(e, message){

			    			console.log('incoming');
			    			console.log(message);
			    			console.log(message.data.data);

			    			if (message.params.index && message.params.type){

			    				if (message.action == 'PUT'){

			    					var data = message.data.data;

			    					if (message.data.data instanceof Array)
			    						data = {array:message.data.data};

				    				elasticClient.index({
									  index: message.params.index,
									  type: message.params.type,
									  id: message.data._id,
									  body: data
									}, function (err, resp) {
									  	if (err)
									  		logger.log('error', 'Error indexing: ' + err, message);
									  	else
									  		logger.log('success', 'Indexed' , message.data._id);
									});

				    			}
				    			else if (message.action == 'DELETE' && message.data.removed > 0){

				    				if (message.params.child_id){

				    					elasticClient.delete({
											  index: message.params.index,
											  type: message.params.type,
											  id: message.params.child_id
											}, function (err, resp) {

												if (err)
											  		logger.log('error', 'Error deleting: ' + err, message);
											  	else
											  		logger.log('success', 'Deleted' , resp);
											});

				    				}else{

				    					async.eachSeries(message.data.data, function( item, callback) {

					    					elasticClient.delete({
											  index: message.params.index,
											  type: message.params.type,
											  id: item._id
											}, function (err, resp) {

												if (err)
											  		logger.log('error', 'Error deleting: ' + err, item._id);
											  	else
											  		logger.log('success', 'Deleted' , resp);

												callback(err);
											});

										}, function(err){
										    // if any of the file processing produced an error, err would equal that error
										   if (err)
										  		logger.log('error', 'Error in delete: ' + err, message);
										  	else
										  		logger.log('success', 'Delete successful' , {deleted:message.data.data.length});
										});

				    				}

				    			}
			    			}
			    		});

						logger.log('info', 'Freebase elastic listening to instance ' + fbhost + ':' + fbport);

			    		done();

			    	}else{
			    		done('Failed loading freebase client: ' + e);
			    	}

			    });

			  }
			});


		}catch(e){
			done(e);
		}
	}
}