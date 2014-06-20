module.exports = {
	initialize:function(params){

		var _this = this;

		if (!params.elasticClient)
			throw 'Need elasticClient parameter';

		for (var propertyName in params)
			_this[propertyName] = params[propertyName];

		if (_this.index == null)
			_this.index = 'freebase-elastic-logs';

	},
	log:function(type, message, data, done){

		var _this = this;

		var timestamp = new Date();

		if (data == null)
			data = {};

		_this.elasticClient.index({ index: _this.index,
							  type: type,
							  id: require('shortid').generate(),
							  body: {
							  	timestamp:timestamp,
							  	message:message,
							  	data:JSON.stringify(data)
							  }
							}, function (err, resp) {
								if (done){
									done(err, resp);
								}else if (err)
							  		console.log('Error logging: ' + err);
							});

		if (_this.logToConsole){
			console.log(timestamp);
			console.log(type);
			console.log(message);
			console.log(data);
		}
			

	}
}