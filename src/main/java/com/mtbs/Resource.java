package com.mtbs;

import java.util.LinkedList;
import java.util.List;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.mtbs.Movie;
import com.mtbs.Screen;
import com.mtbs.User;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Path("/")
public class Resource 
{
	DAO dao=new DAO();
	
	//UserResource
	
	@GET
	@Path("user")
	@Produces({MediaType.APPLICATION_JSON})
	public List<User> getUsers()
	{
		return dao.getUsers();
	}
	
	@GET
	@Path("user/manager")
	@Produces({MediaType.APPLICATION_JSON})
	public String getManagers()
	{
		return dao.getManagers().toString();
	}
	
	@GET
	@Path("user/{user_id}/bookings")
	@Produces({MediaType.APPLICATION_JSON})
	public String getBookings(@PathParam("user_id") int user_id)
	{
		return dao.getBookings(user_id);
	}
	
	@GET
	@Path("user/{user_id}/theater")
	@Produces({MediaType.APPLICATION_JSON})
	public String getTheaters(@PathParam("user_id") int user_id)
	{
		return dao.getTheaters(user_id).toString();
	}
	
	@POST
	@Path("user/{user_id}/bookings/{booking_id}/cancel")
	public String cancelBooking(@PathParam("user_id") int user_id,@PathParam("booking_id") int booking_id,String jsonObject)
	{
		JSONObject jsonData=null;
		try {
			jsonData = (JSONObject)new JSONParser().parse(jsonObject);
		} catch (ParseException e) 
		{
			System.out.println("Exception in cancelBooking() "+e);
		}
		return dao.cancelBooking(booking_id,user_id,jsonData);
	}
	
	
	
	//MovieResource
	
	@GET
	@Path("movie")
	@Produces({MediaType.APPLICATION_JSON})
	public List<Movie> getMovies()
	{
		
		return dao.getAllMovies();
	}
	
	@GET
	@Path("movie/{movie_id}")
	@Produces({MediaType.APPLICATION_JSON})
	public Movie getMovie(@PathParam("movie_id") int movie_id)
	{
		System.out.println("getMovie Called..");
		return dao.getMovie(movie_id);
	}
	
	
	@POST
	@Path("movie")
	@Produces({MediaType.APPLICATION_JSON})
	@Consumes({MediaType.APPLICATION_FORM_URLENCODED})
	public Movie addMovie(@FormParam("name") String name,@FormParam("certificate") String certificate,@FormParam("director") String director,
			@FormParam("description") String description,@FormParam("duration") String duration,@FormParam("image") String image,
			@FormParam("language1") String language1,@FormParam("language2") String language2,@FormParam("language3") String language3)
	{
		System.out.println("Languages : "+language1+" "+language2+" "+language3);
		List<Integer> languages=new LinkedList<Integer>();
		if(language1!=null)
		{
			languages.add(Integer.parseInt(language1));
		}
		if(language2!=null)
		{
			languages.add(Integer.parseInt(language2));
		}
		if(language3!=null)
		{
			languages.add(Integer.parseInt(language3));
		}
		return dao.addMovie(name,certificate,director,description,duration,image,languages);
	}
	
	
	@PUT
	@Path("movie/{movie_id}")
	@Produces({MediaType.APPLICATION_JSON})
	@Consumes({MediaType.APPLICATION_FORM_URLENCODED})
	public Movie updateMovie(@PathParam("movie_id") int movie_id,@FormParam("name") String name,@FormParam("certificate") String certificate,@FormParam("director") String director,
			@FormParam("description") String description,@FormParam("duration") String duration,@FormParam("image") String image,
			@FormParam("language1") String language1,@FormParam("language2") String language2,@FormParam("language3") String language3)
	{
		
        System.out.println("Languages : "+language1+" "+language2+" "+language3);
		List<Integer> languages=new LinkedList<Integer>();
		if(language1!=null)
		{
			languages.add(Integer.parseInt(language1));
		}
		if(language2!=null)
		{
			languages.add(Integer.parseInt(language2));
		}
		if(language3!=null)
		{
			languages.add(Integer.parseInt(language3));
		}
		return dao.updateMovie(movie_id,name,certificate,director,description,duration,image,languages);
		
		 
	}
	

	
	@PUT
	@Path("movie/cancel/{movie_id}")
	public String cancelMovie(@PathParam("movie_id") int movie_id)
	{
		Movie m=dao.getMovie(movie_id);
	    dao.cancelMovie(movie_id);
	    return ("Movie '"+m.getName()+"' deleted..");
	}
	

	
	@GET
	@Path("movie/latest")
	@Produces({MediaType.APPLICATION_JSON})
	public List<Movie> getLatestMovies()
	{
		return dao.getLatestMovies();
	}
	
	@GET
	@Path("movie/upcoming")
	@Produces({MediaType.APPLICATION_JSON})
	public List<Movie> getUpcomingMovies()
	{
		return dao.getUpcomingMovies();
	}
	
	@GET
	@Path("movie/{movie_id}/show")
	@Produces({MediaType.APPLICATION_JSON})
	public String getShows(@PathParam("movie_id") int movie_id)
	{
		
		return dao.getShows(movie_id).toString();
	}
	
	
	//ShowResource
	
	@PUT
	@Path("show/{show_id}")
	public String updateShow(@PathParam("show_id") int show_id,String jsonObject)
	{
		JSONObject jsonData=null;
		try {
			jsonData = (JSONObject)new JSONParser().parse(jsonObject);
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	    return dao.updateShow(show_id,jsonData);
	}
	
	@PUT
	@Path("show/{show_id}/cancel")
	public String deleteShow(@PathParam("show_id") int show_id)
	{
	    return dao.deleteShow(show_id);
	}
	
	@GET
	@Path("show/{show_id}/seatingArrangement")
	@Produces({MediaType.APPLICATION_JSON})
	public String getSeatingArrangement(@PathParam("show_id") int show_id)
	{
		return dao.getShowSeatingArrangement(show_id).toString();
	}
	
	@POST
	@Path("show/{show_id}/bookTicket")
	public String bookTicket(@PathParam("show_id") int show_id,String jsonObject) 
	{
		JSONObject jsonData=null;
		try {
			jsonData = (JSONObject)new JSONParser().parse(jsonObject);
		} catch (ParseException e) 
		{
			System.out.println("Exception in bookTicket() "+e);
		}
		return dao.bookTicket(show_id,jsonData);
	}
	
	@GET
	@Path("show/{show_id}/cancellationPercentage")
	@Produces({MediaType.APPLICATION_JSON})
	public String getCancellationPercentage(@PathParam("show_id") int show_id)
	{
		return dao.getCancellationPercentage(show_id).toString();
	}
	
	@GET
	@Path("booking/{booking_id}/cancellationPercentage")
	@Produces({MediaType.APPLICATION_JSON})
	public String getBookingCancellationPercentage(@PathParam("booking_id") int booking_id)
	{
		return dao.getBookingCancellationPercentage(booking_id).toString();
	}
	
	@GET
	@Path("show/{show_id}/collection")
	@Produces({MediaType.APPLICATION_JSON})
	public String getCollection(@PathParam("show_id") int show_id)
	{
		return dao.getCollection(show_id).toString();
	}
	
	
	//ScreenResource
	
	@POST
	@Path("screen/{screen_id}/availableSlots")
	@Produces({MediaType.APPLICATION_JSON})
	public String getAvailableSlots(@PathParam("screen_id") int screen_id,String jsonObject)
	{
		JSONObject jsonData=null;
		try {
			jsonData = (JSONObject)new JSONParser().parse(jsonObject);
		} catch (Exception e) {
			System.out.println("Exption in parsing jsonObject in getAvaliableSlots "+e);
		}
		System.out.println(jsonData);
		return dao.getAvailableSlots(screen_id,jsonData).toString();
		
	}
	
	@POST
	@Path("screen")
	public String addScreen(String jsonObject)
	{
		JSONObject jsonData=null;
		try {
			jsonData = (JSONObject)new JSONParser().parse(jsonObject);
		} catch (Exception e) {
			System.out.println("Exption in parsing jsonObject in addScreen "+e);
		}
		System.out.println(jsonData);
		return dao.addScreen(jsonData);
	}
	
	@PUT
	@Path("screen/{screen_id}")
	public String updateScreen(@PathParam("screen_id") int screen_id,String jsonObject)
	{
		JSONObject jsonData=null;
		try {
			jsonData = (JSONObject)new JSONParser().parse(jsonObject);
		} catch (Exception e) {
			System.out.println("Exption in parsing jsonObject in addScreen "+e);
		}
		System.out.println(jsonData);
		return dao.updateScreen(screen_id,jsonData);
	}
	
	@GET
	@Path("screen/{screen_id}/collection")
	@Produces({MediaType.APPLICATION_JSON})
	public String getScreenCollection(@PathParam("screen_id") int screen_id)
	{
		return dao.getScreenCollection(screen_id).toString();
	}
	
	@PUT
	@Path("screen/{screen_id}/cancel")
	public String removeScreen(@PathParam("screen_id") int screen_id)
	{
		return dao.removeScreen(screen_id);
	}
	
	
	
	//TheaterResource
	
	@GET
	@Path("theater")
	@Produces({MediaType.APPLICATION_JSON})
	public String getTheaters()
	{
		
		return dao.getAllTheaters().toString();
	}
	
	@POST
	@Path("theater")
	public String addTheater(String jsonData)
	{
		System.out.println(jsonData);
		JSONObject jsonObj=null;
		try {
			jsonObj = (JSONObject)new JSONParser().parse(jsonData);
			
		} catch (ParseException e) 
		{
			System.out.println("Exception in parsing json data "+e);
		}
		
		return dao.addTheater(jsonObj );
	}
	
	@PUT
	@Path("theater/{theater_id}")
	public String updateTheater(@PathParam("theater_id") int theater_id,String jsonData)
	{
		System.out.println(jsonData);
		JSONObject jsonObj=null;
		try {
			jsonObj = (JSONObject)new JSONParser().parse(jsonData);
			
		} catch (ParseException e) 
		{
			System.out.println("Exception in parsing json data "+e);
		}
		return dao.updateTheater(theater_id,jsonObj );
	}
	
	@PUT
	@Path("theater/{theater_id}/cancel")
	public String removeTheater(@PathParam("theater_id") int theater_id)
	{
		return dao.cancelTheater(theater_id );
	}
	
	
	@GET
	@Path("theater/{theater_id}")
	@Produces({MediaType.APPLICATION_JSON})
	public String getTheater(@PathParam("theater_id") int theater_id)
	{
		
		return dao.getTheater(theater_id).toString();
	}
	
	@GET
	@Path("theater/{theater_id}/screen")
	@Produces({MediaType.APPLICATION_JSON})
	public List<Screen> getScreens(@PathParam("theater_id") int theater_id)
	{
		
		return dao.getScreens(theater_id);
	}
	
	@GET
	@Path("theater/{theater_id}/screen/{screen_id}")
	@Produces({MediaType.APPLICATION_JSON})
	public Screen getScreens(@PathParam("theater_id") int theater_id,@PathParam("screen_id") int screen_id)
	{
		
		return dao.getScreen(theater_id,screen_id);
	}
	
	@POST
	@Path("theater/{theater_id}/screen/{screen_id}/seatingArrangement")
	@Produces({MediaType.APPLICATION_JSON})
	public String updateSeatingArrangement(@PathParam("theater_id") int theater_id,@PathParam("screen_id") int screen_id,String jsonData)
	{
		System.out.println(jsonData);
		JSONObject jsonObj=null;
		try {
			jsonObj = (JSONObject)new JSONParser().parse(jsonData);
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return dao.updateSeatingArrangement(theater_id,screen_id,jsonObj).toString();
		
	}
	
	@GET
	@Path("theater/{theater_id}/screen/{screen_id}/seatingArrangement")
	@Produces({MediaType.APPLICATION_JSON})
	public String getSeatingArrangement(@PathParam("theater_id") int theater_id,@PathParam("screen_id") int screen_id)
	{
		return dao.getSeatingArrangement(screen_id).toString();
	}
	
	
	@POST
	@Path("theater/{theater_id}/screen/{screen_id}/show")
	@Consumes({MediaType.APPLICATION_JSON})
	@Produces({MediaType.APPLICATION_JSON})
	public List<String> addShow(@PathParam("theater_id") int theater_id,@PathParam("screen_id") int screen_id,String jsonData)
	{
		System.out.println(jsonData);
		JSONObject jsonObj=null;
		try {
			jsonObj = (JSONObject)new JSONParser().parse(jsonData);
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return dao.addShow(theater_id,screen_id,jsonObj);
		
		
	}
	
	
	@GET
	@Path("theater/{theater_id}/screen/{screen_id}/show")
	@Produces({MediaType.APPLICATION_JSON})
	public String getAllShows(@PathParam("theater_id") int theater_id,@PathParam("screen_id") int screen_id)
	{
		System.out.println("inside get shows");
		return dao.getAllShows(theater_id,screen_id).toString();
		
	}
	
	@GET
	@Path("theater/{theater_id}/collection")
	@Produces({MediaType.APPLICATION_JSON})
	public String getTheaterCollection(@PathParam("theater_id") int theater_id)
	{
		return dao.getTheaterCollection(theater_id).toString();
	}
	
	
	//OfferResource
	@GET
	@Path("offer")
	public String getOffers()
	{
		return dao.getOffers().toString();
	}
	
	@POST
	@Path("offer")
	public String addOffer(String jsonData)
	{
		System.out.println(jsonData);
		JSONObject jsonObj=null;
		try {
			jsonObj = (JSONObject)new JSONParser().parse(jsonData);
			
		} catch (ParseException e) {
			System.out.println("exception in parsing json in adding offer"+e);
		}
		return dao.addOffer(jsonObj);
	}
	
	@PUT
	@Path("offer/{offer_id}")
	public String updateOffer(@PathParam("offer_id") int offer_id,String jsonData)
	{
		JSONObject jsonObj=null;
		try {
			jsonObj = (JSONObject)new JSONParser().parse(jsonData);
			
		} catch (ParseException e) {
			System.out.println("exception in parsing json in updating offer"+e);
		}
		return dao.updateOffer(offer_id,jsonObj);
	}
	
	@PUT
	@Path("offer/{offer_id}/cancel")
	public String removeOffer(@PathParam("offer_id") int offer_id)
	{
		return dao.cancelOffer(offer_id);
	}
	
	@GET
	@Path("offer/valid")
	public String getValidOffers()
	{
		return dao.getValidOffers().toString();
	}
	
	
}
