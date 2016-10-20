$(function(){
var url='http://www.adsflow.cc/bids/pid00000'
    $.get(url, function(data){
	var dataSet =data.pidscore.map(function(dataPoint){
		return [dataPoint.uid, dataPoint.pid, dataPoint.time, dataPoint.price]
	})
	 
	$(document).ready(function() {
	    $('#example').DataTable( {
		data: dataSet,
		columns: [
		    { title: "user ID"},
		    { title: "product ID" },
		    { title: "time" },
		    { title: "winning price" }
		]
	    } );
	} );

    } );
})
