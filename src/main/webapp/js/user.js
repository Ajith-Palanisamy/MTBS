
var contentStates=[];

$(document).ready(function(){
	
	getUserList();
	
	
	var user_id,user_name;
	var screen_id,movie_id,movie_name;
	var show_id;
	var selectedSeats =[];
	
	//refreshSelectedSeatsTable();
	
	
	function pushState()
	{
		 contentStates.push($("#body-content").html())
		 console.log("content pushed.."+contentStates.length);
	}
	
	 $(document).on('click','a',function()
	 {
		pushState();
	 }
	 );
	
	
	function getUserList() {
    $.ajax({
      url: './webapi/user',
      type: 'GET',
      success: function(data) {
        $('#user-list').empty();
  		//console.log(data);
        data.forEach(function(item) {
          var listItem = $('<li>');
          var linkItem=$('<a>')
            .text(item.name)
            .attr("class","userLink")
            .attr("href","javascript:void(0)")
            .attr('user',JSON.stringify(item));
           
          listItem.append(linkItem);
          $('#user-list').append(listItem);
          
        });
      }
    });
  }
  
  
  
  $(document).on('click','.userLink',function(event)
  {
	event.preventDefault();
	var user=JSON.parse($(this).attr('user'));
	
	user_id=user.user_id;
	user_name=$(this).text();
	sessionStorage.setItem('user_id',user_id);
	sessionStorage.setItem('user_name',user_name);
	
	$('#users').hide();
	$('#latest-movies').show();
	$('#upcoming-movies').show();
	$('#your-bookings-link-div').show();
	
	
	 $.ajax({
      url: './webapi/movie/latest',
      type: 'GET',
      success: function(data) {
        $('#latest-movie-list').empty();
  		console.log(data);
        data.forEach(function(item) {
          var listItem = $('<li>');
           var linkItem=$('<a>')
            .text(item.name)
            .attr("class","movieLink")
            .attr("href","javascript:void(0)")
            .attr('selected_movie', JSON.stringify(item));
           
            listItem.append(linkItem);
            $('#latest-movie-list').append(listItem);
        });
      }
    });
    
    $.ajax({
      url: './webapi/movie/upcoming',
      type: 'GET',
      success: function(data) {
        $('#upcoming-movie-list').empty();
  		console.log(data);
        data.forEach(function(item) {
          var listItem = $('<li>')
            .text(item.name)
            .attr('selected_movie', JSON.stringify(item));
            
            $('#upcoming-movie-list').append(listItem);
        });
      }
    }); 
    
    
    $.ajax({
      url: './webapi/user/'+user_id+'/bookings',
      type: 'GET',
      success: function(data) {
       
  		console.log(data);
        
      }
    }); 
    
       
 });
 
 
	$(document).on('click','.movieLink',function(event)
	  {
		event.preventDefault();
		
		$('#latest-movies').hide();
		$('#upcoming-movies').hide();
		$('#your-bookings-link-div').hide();
		
		$('#shows-div').show();
		
		var selected_movie=JSON.parse($(this).attr("selected_movie"));
		
		sessionStorage.setItem('movie_id',selected_movie.movie_id);
		sessionStorage.setItem('movie_name',selected_movie.name);
		movie_id=sessionStorage.getItem("movie_id");
		movie_name=sessionStorage.getItem("movie_name");
		
		$('#shows-div h2').text(movie_name);
		
		$.ajax({
			url: './webapi/movie/'+movie_id+'/show',
		    type: 'GET',
		    success: function(data) {
			//console.log("shows"+data);
		    var table = $("#shows-table");
           // table.find("tr:not(:first)").remove();
            
            $.each(data, function(index, object) {
                var row = $('<tr>');
               
                row.append($("<td>").text(object.show_date));
                row.append($("<td>").text(object.start_time));
                row.append($("<td>").text(object.language));
                row.append($("<td>").text(object.screen_name));
                row.append($("<td>").text(object.theater_name+','+object.district));
                //row.append($("<td>").text(object.end_time));
                console.log(object.theater_name);
                var l=$("<a>")
                .attr("show_id",object.show_id)
                .attr("theater",object.theater_name)
                .attr("screen_id",object.screen_id)
                .attr("href","javascript:void(0)")
                .attr("class","bookTicket")
                .text("Book Tickets");
                var td=$("<td>").append(l);
                row.append(td);
                table.append(row);
                
            });
          }
		});
	});
	
	$(document).on('click','.bookTicket',function(event)
	{
		event.preventDefault();
		movie_name=sessionStorage.getItem("movie_name");
		
		$('#shows-div').hide();
		$('#seating-container').show();
		$('#seating-container h1').text(movie_name);
		
		var show_id=$(this).attr('show_id');
		sessionStorage.setItem('show_id',$(this).attr("show_id"));
		
		getOffers();
		//getTheaterSeatingStatus();
		
	});
	
	
	function getTheaterSeatingStatus(){
		show_id=sessionStorage.getItem('show_id');
		
		
		$.ajax({
		  url: './webapi/show/'+show_id+'/seatingArrangement',
	      method: 'GET',
	      success: function(response) 
	      {
			console.log(response);
			displaySeatings(response);
		  },
		  error:function(err)
		  {
			console.log("error in getting seat arrangement for the show "+err);
			alert("error in getting seat arrangement for the show "+err);
		  }
		});
	}
	
	
	function displaySeatings(response)
	{
		
	    var seats=response.seats;
	    var booked=response.booked;
	    show_id=sessionStorage.getItem('show_id');
	    var columns=response.columns;
	    var rows=parseInt(seats.length/columns);
	       	
    	var seatingLayout = $('#result-seating-layout');
	    seatingLayout.empty();
	    
	    seatingLayout.append('<div class="char"></div>');
	    for(var i=1;i<=columns;i++)
	    {
			seatingLayout.append('<div class="char">'+i+'</div>');
		}
		seatingLayout.append('<br>');
		var k=0;
	    for (var i = 1; i <= rows; i++) {
			var char=String.fromCharCode(i + 64);
			seatingLayout.append('<div class="char">'+char+'</div>');
	      for (var j = 1; j <= columns; j++) {
	        var seat=seats[k];
	        
	        var seatDiv=$('<div>')
	        .addClass('seat')
	        .append("&nbsp;")
	        .data("seatNumber",char+''+j)
	        .data("seatType",seat.seat_type);
	     
	      
	        if(seat.seat_type==="P")
	        {
				seatDiv.addClass('path');
	        }
	        else if(booked.includes(char+''+j))
	        {
				seatDiv.addClass('booked');
			}
	        else
	        {
				seatDiv.addClass(seat.seat_type.toLowerCase());
				seatDiv.addClass('open');	
				
			}
	        
	         if(seat.seat_type.toLowerCase()==='vip')
	         {
				seatDiv.data("seatPrize",response.vip_prize);
			 }
	         else if(seat.seat_type.toLowerCase()==='premium')
	         {
				seatDiv.data("seatPrize",response.premium_prize);
			 }
			 else
	         {
				seatDiv.data("seatPrize",response.normal_prize);
			 }
	        
	        seatingLayout.append(seatDiv);
	        k++;
	      }
	      seatingLayout.append('<br>');
	      
	      $('#vip-prize').text("Rs : "+response.vip_prize+"/- Cancellation Fee : "+response.vip_cancel+"%");
	      $('#premium-prize').text("Rs : "+response.premium_prize+"/-  Cancellation Fee : "+response.premium_cancel+"%");
	      $('#normal-prize').text("Rs : "+response.normal_prize+"/-  Cancellation Fee : "+response.normal_cancel+"%");
	      
    }
	}
	
	function getOffers()
	{
		//
		
		$.ajax({
		  url: './webapi/offer/valid',
	      method: 'GET',
	      success: function(response) 
	      {
					 //alert(response);
			         var dataArray = JSON.parse(response);
			         if(dataArray.length>0)
			         {
							$('#offers').show();
							$('#offer-list').empty();
		             $.each(dataArray,function(index,object)
		             {
							
							 var listItem = $('<li>')
							 				.html('<b>'+object.offer_name+':</b>Book minimum '+object.no_of_tickets+' tickets and get '+object.discount+'% offer on each.');
            				 
							 $('#offer-list').append(listItem);
						
			         });
			         }
		  },
		  error:function(err)
		  {
			console.log("error in getting offers "+err);
			alert("error in getting offers "+err);
		  }
		});
		
		getTheaterSeatingStatus();
	}
	
	$(document).on('click', '.open', function() 
	{
		
        var size=$('.seat.selected').length;
        if(size<10)
        {
			$(this).toggleClass('selected');
         }
        else if(size==10 && $(this).hasClass('selected'))
        {
			$(this).removeClass('selected');
        }
        else if(size==10)
        {
			alert("You can only book 10 tickets in single booking..");
			return;
        }
	    refreshSelectedSeatsTable();
    });
    
    function refreshSelectedSeatsTable()
    {	
		selectedSeats =[];
    	$('.seat.selected').each(function() {
	      var seatNumber = $(this).data('seatNumber');
	      var seatPrize = $(this).data('seatPrize');
	      var seatType=$(this).data('seatType');
	      
	      selectedSeats.push({ number :seatNumber, prize: seatPrize, type : seatType});
        });
        
        console.log(selectedSeats);
        
		    var total=0;
		    var table = $("#selected-seats-table");
            table.find("tr:not(:first)").remove();
            var row;
            var i=1;
            if(selectedSeats.length==0)
            {
				row=$("<tr>");
				row.append($("<td colspan='4'>").text("No seats added."));
				table.append(row);
            }
            else
            {
				$.each(selectedSeats, function(index, object) {
                row = $("<tr>");
                row.append($("<td>").text(i));
                row.append($("<td>").text(object.number));
                row.append($("<td>").text(object.type));
                row.append($("<td>").text(object.prize));
                table.append(row);
                //row.append($("<td>").text(object.end_time));
                total=total+object.prize;
                i++;
                });
                row = $("<tr>");
                row.append($("<td colspan='3'>").text("Total amount : "));
                row.append($('<td id="total">').text(total));
                table.append(row);
            }
            
	}
   
   $(document).on('click', '#confirm-button', function(event) 
   {
    	event.preventDefault();
    	
    	selectedSeats =[];
    	$('.seat.selected').each(function() {
	      var seatNumber = $(this).data('seatNumber');
	      var seatType=$(this).data('seatType');
	      var seatPrize=$(this).data('seatPrize');
	      selectedSeats.push({ number :seatNumber,type : seatType,prize : seatPrize});
        });
        
        if(selectedSeats.length==0)
        {
			alert("No seats selected..");
			return;
        }
        
    	var data={};
    	data.selectedSeats=selectedSeats;
    	data.user_id=sessionStorage.getItem("user_id");
    	data.show_id=sessionStorage.getItem("show_id");
    	data.amount=$('#total').text();
    	console.log(data);
    	
	    $.ajax({
		  url: './webapi/show/'+show_id+'/bookTicket',
	      method: 'POST',
	      data:JSON.stringify(data),
	      success: function(response) 
	      {
			console.log(response);
			alert(response);
			selectedSeats =[];
			var table = $("#selected-seats-table");
            table.find("tr:not(:first)").remove();
            var row;
				row=$("<tr>");
				row.append($("<td colspan='4'>").text("No seats added."));
				table.append(row);
			getTheaterSeatingStatus();
			
           
		  },
		  error:function(err)
		  {
			console.log("error in booking tickets for the show "+err);
			alert("error in in booking tickets for the show "+err);
		  }
		});
   });
   
   $(document).on('click','#your-bookings-link',function(event)
   {
	 event.preventDefault();
	 $('#latest-movies').hide();
	 $('#upcoming-movies').hide();
	 $('#your-bookings-link-div').hide();
	
	 $('#your-bookings').show();
	 $('#cancelled-bookings').show();
	 var user_id=sessionStorage.getItem("user_id");
	 
	 refreshBookings();
	
   });
   
   function refreshBookings()
   {
		  var user_id=sessionStorage.getItem("user_id");
		  $.ajax({
		  url: './webapi/user/'+user_id+'/bookings',
	      method: 'GET',
	      success: function(response) 
	      {
			console.log(response);
			displayBookings(response);
		  },
		  error:function(err)
		  {
			console.log("error in getting seat arrangement for the show "+err);
			alert("error in getting seat arrangement for the show "+err);
		  }
		});
	}
		
	function displayBookings(response)
	{
		
	        var total=0;
		    var table1 = $("#your-bookings-table");
		    var table2 = $("#cancelled-bookings-table");
            table1.find("tr:not(:first)").remove();
            table2.find("tr:not(:first)").remove();
            var row;
            
            $.each(response,function(index,object)
            {
				row = $("<tr>");
                row.append($("<td>").text(object.movie_name));
                row.append($("<td>").text(object.show_date));
                row.append($("<td>").text(object.show_time));
                row.append($("<td>").text(object.language));
                row.append($("<td>").text(object.screen_name));
                row.append($("<td>").text(object.theater));
                row.append($("<td>").text(object.seats));
				if(object.status=='Booked'|| object.status=='Cancellation unavailable')//Cancellation unavailable for past successfully booked
				{
					row.append($("<td>").text(object.offer_name));
                    row.append($("<td>").text(object.discount));
                    
                    
                    
	                var l=$('<button>')
	                .attr("tickets",JSON.stringify(object))
	                .attr("class","viewTicketsLink")
	                .text("View Tickets");
	                var td=$("<td>").append(l);
	                row.append(td);
	                table1.append(row);
				}
				else
				{
					var prizes=object.prizes.split(',');
					var refunds=object.refunds.split(',');
					var total=0,refund=0;
					for(var i=0;i<prizes.length;i++)
					{
		                total=total+parseInt(prizes[i]);
		                refund=refund+parseInt(refunds[i]);
					}
					row.append($("<td>").text(total));
                	row.append($("<td>").text(refund));
               	    row.append($("<td>").text(object.status));
               	    table2.append(row);
				}
				
            });
           
      }
      
      $(document).on('click','.viewTicketsLink',function()
      {
			
			var tickets=JSON.parse($(this).attr("tickets"));
			
			$('#view-tickets-div').show();
			$('#view-tickets-div h2').text(tickets.movie_name);
			var table=$('#view-tickets-table');
			table.find("tr:not(:first)").remove();
			var seats=tickets.seats.split(',');
			var prizes=tickets.prizes.split(',');
			var types=tickets.types.split(',');
			
			var total=0;
			for(var i=0;i<seats.length;i++)
			{
				row = $("<tr>");
				row.append($("<td>").text(i+1));
                row.append($("<td>").text(seats[i]));
                row.append($("<td>").text(types[i]));
                row.append($("<td>").text(prizes[i]));
                table.append(row);
                total=total+parseInt(prizes[i]);
              
			}
			row = $("<tr>");
			row.append($('<td colspan="3">').text("Total"));
            row.append($('<td>').text(total));
			table.append(row);
			
			tickets.total=total; 
			 
			row = $("<tr>");
			var closeButton = $('<button>')
            .text('close')
            .click(clickedCloseButton);
            
			var cancelButton = $('<button>')
            .text('Cancel Booking')
            .click(cancelBooking)
            .attr('tickets',JSON.stringify(tickets));
            
             row.append($('<td colspan="2">').append(closeButton));
             if(tickets.status==='Booked')
             row.append($('<td colspan="2">').append(cancelButton));
			 table.append(row);
			
	  });
      
      function clickedCloseButton()
      {
		$('#view-tickets-div').hide();
      }
      
      function cancelBooking()
      {
	    tickets=JSON.parse($(this).attr('tickets'));
	    show_id=tickets.show_id;
	    booking_id=tickets.booking_id;
	    
	    $.ajax({
		  //url: './webapi/show/'+show_id+'/cancellationPercentage',
		  url: './webapi/booking/'+booking_id+'/cancellationPercentage',
	      method: 'GET',
	      success: function(response) 
	      {
			console.log(response);
			calculateCancellationFee(tickets,response);
		  },
		  error:function(err)
		  {
			console.log("error in getting cancellation percentage for the show "+err);
			alert("error in getting cancellation percentage for the show "+err);
		  }

		
	    });
      }
      
      function calculateCancellationFee(tickets,response)
      {
		    var seats=tickets.seats.split(',');
			var prizes=tickets.prizes.split(',');
			var types=tickets.types.split(',');
			var vip,premium,normal;
			
			vip=(parseInt(response.vip_cancel)*0.01);
			premium=(parseInt(response.premium_cancel)*0.01);
			normal=(parseInt(response.normal_cancel)*0.01);
			var total_fee=0;
			var r1=0,r2=0,r3=0;
			var cnt1=0,cnt2=0,cnt3=0;
			
			//alert("prizes[] : "+prizes);
			//alert("types[] : "+types);
			//alert("vip % "+vip);
			//alert("premium % "+premium);
			//alert("normal % "+normal);
			for(var i=0;i<seats.length;i++)
			{
				var prize=parseInt(prizes[i]);
				if(types[i]==='VIP')
				{
				  r1=Math.round(prize*vip);
				  cnt1++;
				
				}
				else if(types[i]==='Premium')
				{
				  r2=Math.round(prize*premium);
				  cnt2++;
				}
				else if(types[i]==='Normal')
				{
				  r3=Math.round(prize*normal);
				  cnt3++;
				}
			}
			
			var f1=(r1*cnt1);
			
			var f2=(r2*cnt2);
			
			var f3=(r3*cnt3);
			total_fee=(f1+f2+f3);
			
		    var final_refund=(parseInt(tickets.total))-total_fee;
			var msg='Cancellation Fee :\n VIP - '+response.vip_cancel+'%\nPremium - '+response.premium_cancel+'%\nNormal - '+response.normal_cancel+'%\nTotal Amount - '+tickets.total+'\nCancellation Fee - '+total_fee+'\nRefund - '+final_refund;
			if(confirm(msg))
			{   
				user_id=sessionStorage.getItem("user_id");
				booking_id=tickets.booking_id;
				var data=
				{
					vip:r1,
					premium:r2,
					normal:r3,
					user_wallet:final_refund,
					theater_wallet:total_fee
				}
				console.log('cancelation'+data);
				//alert(data);
				$.ajax({
				  url: './webapi/user/'+user_id+'/bookings/'+booking_id+'/cancel',
			      method: 'POST',
			      data:JSON.stringify(data),
			      success: function(response) 
			      {
					console.log(response);
					alert("Tickets cancelled!!");
					$('#view-tickets-div').hide();
					refreshBookings();
				  },
				  error:function(err)
				  {
					console.log("error in cancellation of tickets "+err);
					alert("error in cancellation of tickets "+err);
				  }
				});
			}
      }
      
      $(document).on('click','#backButton',function() {
	   //alert("Back button pressed");
	   
	   console.log(contentStates);
       if (contentStates.length > 0) {
       var previousState = contentStates.pop();
       $("#body-content").html(previousState);
       console.log('content poped..'+contentStates.length);
      }
  });
  
});