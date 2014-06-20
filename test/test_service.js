var service = require('../lib/service');


service.initialize({freebase_secret:'freebase-ui-secret'}, function(e){

	if (!e)
		console.log('elastic listener up');
	else
		console.log('elastic listener not up: ' + e);

});