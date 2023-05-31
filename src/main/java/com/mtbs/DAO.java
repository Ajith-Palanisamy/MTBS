package com.mtbs;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.mtbs.db.DBHandler;


import com.mtbs.Movie;
import com.mtbs.Screen;
import com.mtbs.User;

public class DAO 
{
	 DBHandler obj=DBHandler.getInstance();
     Connection con = obj.getConnection();
     PreparedStatement ps;
     Statement st;
     ResultSet rs;
     
     public int addUser(String name,String mobile,String email,String password,int user_role_id)
     {
        try {
                ps = con.prepareStatement("insert into users(name,mobilenumber,email,password,user_role_id) VALUES(?,?,?,?,?)");
                ps.setString(1,name);
                ps.setString(2, mobile);
                ps.setString(3, email);
                ps.setString(4, password);
                ps.setInt(5 , user_role_id);
                ps.executeUpdate();
                System.out.println("user added");
                return 0;
                           
        } catch (SQLException ex) {
            if(ex.getMessage().contains("duplicate")){
                return 1;
            }
            else
            {	
            	System.out.println(ex);
                return -1;
            }
        }
    }
     
     public List<User> getUsers()
     {
    	 List<User> list=new LinkedList<User>();
    	 User user=null;
    	 try {
      
             ps = con.prepareStatement("select * from users where user_role_id=2 and \"isActive\"= 1 ");
             rs=ps.executeQuery();
             while(rs.next())
             {
                 user=new User(rs.getInt("user_id"),rs.getString("name"),rs.getString("mobilenumber"),rs.getString("email"),rs.getString("password"),rs.getInt("user_role_id"));
                 list.add(user);
             }
                      
         } catch (SQLException ex) 
    	 {
             System.out.println("Exception in retriving users "+ex);
         }
    	 return list; 
     }
     
     public User getUser(String email,String password)
     {
    	 User user=null;
    	 try {
      
             ps = con.prepareStatement("select * from users where email=? and password=?");
             ps.setString(1,email);
             ps.setString(2, password);
			 rs=ps.executeQuery();
             if(rs.next())
             {
                 user=new User(rs.getInt("user_id"),rs.getString("name"),rs.getString("mobilenumber"),rs.getString("email"),rs.getString("password"),rs.getInt("user_role_id"));
             }
             return user;          
         } catch (SQLException ex) {
             return user;
         }
     }
     
     public JSONArray getManagers()
     {
    	 JSONArray arr=new JSONArray();
    	 try {
    	 ps = con.prepareStatement("select * from users where user_role_id=3 and \"isActive\"=1 ");
         rs=ps.executeQuery();
         while(rs.next())
         {
        	 JSONObject s=new JSONObject();
        	 s.put("user_id", rs.getInt("user_id"));
        	 s.put("name",rs.getString("name") );
        	 s.put("email",rs.getString("email") );
        	 
        	arr.add(s);
         }
    	 }
    	 catch(SQLException e)
    	 {
    		 System.out.println("Exception in retriving managers "+e);
    	 }
    	 return arr;
    	 
     }
     
     public JSONArray getTheaters(int user_id)
     {
    	 JSONArray arr=new JSONArray();
    	 try 
    	 {
    	 ps = con.prepareStatement("select * from theater where manager_id=? and \"isAvailable\"=1 ");
    	 ps.setInt(1,user_id);
         rs=ps.executeQuery();
         while(rs.next())
         {
        	 JSONObject t=new JSONObject();
         	t.put("theater_id",rs.getInt("theater_id"));
         	t.put("theater_name",rs.getString("name"));
         	t.put("manager_id",rs.getInt("manager_id"));
//         	t.put("door_no",door_no);
//         	t.put("street",street);
//         	t.put("city",city);
//         	t.put("state",state);
//         	t.put("pin_code",pin_code);
         	t.put("district",rs.getString("district"));
        	 
        	arr.add(t);
         }
    	 }
    	 catch(SQLException e)
    	 {
    		 System.out.println("Exception in retriving managers "+e);
    	 }
    	 return arr;
    	 
     }
     
     public String getBookings(int user_id)
     {
    	 JSONArray arr=new JSONArray();
    	 try
    	 {
    		 ps=con.prepareStatement("select booking.booking_id,STRING_AGG(ticket.seat_number,',') AS seats,COUNT(ticket.ticket_id) as count,MAX(show.show_date) as show_date,MAX(show.start_time) as show_time, concat(MAX(theater.name),',',MAX(theater.district)) as theater,"
    		 		+ "MAX(screen.screen_name) as screen_name,MAX(movie.name) movie_name,STRING_AGG(ticket.seat_prize::text,',') as prizes,STRING_AGG(ticket.seat_type,',') as types,max(booking.status) as status,max(language) as language,max(show.show_id) as show_id,STRING_AGG(ticket.refund::text,',') as refund, max(offer.offer_name) as offer_name,max(ticket.discount) as discount "
    		 		+ "from ticket inner join booking on "
    		 		+ "booking.booking_id=ticket.booking_id and user_id=? "
    		 		+ "inner join show show on show.show_id=booking.show_id "
    		 		+ "inner join screen on screen.screen_id=show.screen_id "
    		 		+ "inner join theater on theater.theater_id = screen.theater_id "
    		 		+ "inner join movie_language_mapping on movie_language_mapping.movie_language_mapping_id=show.movie_language_mapping_id "
    		 		+ "inner join language ON language.language_id = movie_language_mapping.language_id "
    		 		+ "inner join movie on movie.movie_id= movie_language_mapping.movie_id "
    		 		+ "left join offer on booking.offer_id= offer.offer_id "
    		 		+ "group by booking.booking_id order by show_date,show_time") ;
    		 ps.setInt(1, user_id);
    		 rs=ps.executeQuery();
    		 while(rs.next())
    		 {
    			 JSONObject s=new JSONObject();
    			 LocalDate showDate;
    			 LocalTime showTime;
    			 s.put("booking_id",rs.getInt("booking_id"));
    			 s.put("show_date",rs.getString("show_date"));
    			 s.put("show_time",rs.getString("show_time"));
    			 s.put("movie_name",rs.getString("movie_name"));
    			 s.put("screen_name",rs.getString("screen_name"));
    			 s.put("theater",rs.getString("theater"));
    			 s.put("count",rs.getString("count"));
    			 s.put("seats",rs.getString("seats"));
    			 s.put("prizes",rs.getString("prizes"));
    			 s.put("types",rs.getString("types"));
    			 s.put("status",rs.getString("status"));
    			 s.put("language",rs.getString("language"));
    			 s.put("show_id", rs.getInt("show_id"));
    			 s.put("refunds",rs.getString("refund"));
    			 
    			 String t=rs.getString("offer_name");
    			 if (rs.wasNull()) 
    			 {
    				s.put("offer_name","-");
    				s.put("discount","-");
    		      } else {
    				  
    		    	  s.put("offer_name",t);
      				s.put("discount",rs.getInt("discount"));
    				}
    			 
    			 
    			 showDate=LocalDate.parse((String)s.get("show_date"));
    			 showTime=LocalTime.parse((String)s.get("show_time"));
    			 
    			 LocalDateTime dateTime = LocalDateTime.of(showDate,showTime);
    		     LocalDateTime currentDateTime = LocalDateTime.now();
    		     if (dateTime.isBefore(currentDateTime) && s.get("status").equals("Booked")) 
    		     {
    		    	
    		        s.put("status","Cancellation unavailable");
    		     }  
    			 arr.add(s);
    		 }
    	 }
	      catch (SQLException ex) 
    	 {
	         System.out.println("Exception in get bookings foR user "+ex);
	     }
    	 
    	 return arr.toString();
     }
     
     public String cancelBooking(int booking_id,int user_id,JSONObject jsonData)
     {
    	 try
    	 {
    		int vip=((Long)jsonData.get("vip")).intValue();
    		int premium=((Long)jsonData.get("premium")).intValue();
    		int normal=((Long)jsonData.get("normal")).intValue();
    		//System.out.println("vip-refund "+vip);
    		//System.out.println("premium-refund "+premium);
    		//System.out.println("normal-refund "+normal);
    		
    		int theater_wallet=((Long)jsonData.get("theater_wallet")).intValue();
    		int user_wallet=((Long)jsonData.get("user_wallet")).intValue();
    		ps=con.prepareStatement("update booking set status='Cancelled' where booking_id=?");
    		ps.setInt(1,booking_id);
    		ps.executeUpdate();
    		
    		ps=con.prepareStatement("update ticket set refund=seat_prize-? where booking_id=? and seat_type=?");
    		ps.setInt(1,vip);
    		ps.setInt(2,booking_id);
    		ps.setString(3,"VIP");
    		ps.addBatch();
    		
    		ps.setInt(1,premium);
    		ps.setInt(2,booking_id);
    		ps.setString(3,"Premium");
    		ps.addBatch();
    		
    		ps.setInt(1,normal);
    		ps.setInt(2,booking_id);
    		ps.setString(3,"Normal");
    		ps.addBatch();
    		ps.executeBatch();
    		
    		ps=con.prepareStatement("update users set wallet=wallet+? where user_id=?");
    		ps.setInt(1,user_wallet);
    		ps.setInt(2,user_id);
    		ps.executeUpdate();
    		
    		ps=con.prepareStatement("update theater set wallet=wallet-? where theater_id=(select theater.theater_id from theater inner join screen on screen.theater_id = theater.theater_id inner join show on show.screen_id=screen.screen_id inner join booking on booking.show_id=show.show_id and booking_id=?)");
    		ps.setInt(1,user_wallet);
    		ps.setInt(2,booking_id);
    		ps.executeUpdate();
    		
    		return "success"; 
    	 }
    	 catch (SQLException ex) 
    	 {
	         System.out.println("Exception in cancel booking for user "+ex);
	         return "error";
	     }
    	 
     }
     
     
     
     //MovieDAO
     
    int movie_id;
 	String name;
 	String certificate;
 	String director;
 	String description;
 	LocalTime duration;
 	String image;
 	List<String> languages=null;
 	LocalDateTime added_time;
 	Time sqlTime;
 	Timestamp sqlTimestamp;
 	Date utilDate;
 	
     public List<Movie> getAllMovies()
     {
     List<Movie> list=new LinkedList<Movie>();
    	 try {
             ps = con.prepareStatement("select movie.*,STRING_AGG(language,',') as languages from movie "
             		+ "inner join movie_language_mapping ON movie_language_mapping.movie_id = movie.movie_id and  movie.\"isAvailable\"=1 and movie_language_mapping.\"isAvailable\"=1 "
             		+ "inner join language on movie_language_mapping.language_id = language.language_id group by movie.movie_id");
 			ResultSet rs=ps.executeQuery();
             while(rs.next())
             {
             	movie_id=rs.getInt("movie_id");
             	name=rs.getString("name");
             	//System.out.println("DB movie "+name);
             	certificate=rs.getString("certificate");
             	director=rs.getString("director");
             	description=rs.getString("description");
             	image=rs.getString("image");
             	
             	sqlTime=rs.getTime("duration");
             	utilDate = new Date(sqlTime.getTime());
             	duration=utilDate.toInstant().atZone(java.time.ZoneId.systemDefault()).toLocalTime();
             	
             	sqlTimestamp=rs.getTimestamp("added_time");
             	utilDate = new Date(sqlTime.getTime());
             	added_time=utilDate.toInstant().atZone(java.time.ZoneId.systemDefault()).toLocalDateTime();
             	
             	String arr[]=rs.getString("languages").split(",");
             	
             	languages=new LinkedList<>(Arrays.asList(arr));	
             	Movie movie=new Movie(movie_id,name,certificate,languages,director,description,duration,image,added_time);
             	list.add(movie); 
             	//System.out.println("Hi "+movie.toString());
             }
             return list;          
         } catch (SQLException ex) {
         	System.out.println(ex.getMessage());
         	return null;
         }
     }
     public Movie addMovie(String name,String certificate,String director,String description,String duration,String image,List<Integer> languages)
     {
     		System.out.println("inside add movie DB");
     	 try{
              ps = con.prepareStatement("insert into movie(name,certificate,director,description,duration,image) values(?,?,?,?,?,?)");
              ps.setString(1,name);
              ps.setString(2,certificate);
              ps.setString(3,director);
              ps.setString(4,description);
              System.out.println(duration);
              ps.setTime(5,Time.valueOf(duration+":00"));
              ps.setString(6,image);
              ps.executeUpdate();
              System.out.println("Movie added");
              
              int movie_id=-1;
              ps = con.prepareStatement("select * from movie order by added_time desc limit 1");
              ResultSet rs=ps.executeQuery();
              while(rs.next())
              {
             	 movie_id=rs.getInt("movie_id");
              }
              System.out.println("Movie ID "+movie_id);
              
              ps = con.prepareStatement("insert into movie_language_mapping(movie_id,language_id) values(?,?)");
              for(int i=0;i<languages.size();i++)
              {
                  ps.setInt(1,movie_id);
                  ps.setInt(2,languages.get(i));
                  ps.addBatch();
              }
              ps.executeBatch();
              
              return getMovie(movie_id);
      } catch (SQLException ex) 
     	 {
          
              System.out.println(ex.getMessage());
              return null;
          }
     }
     
     public Movie updateMovie(int movie_id,String name,String certificate,String director,String description,String duration,String image,List<Integer> languages)
     {
     	 try{
     		 
     		 ps=con.prepareStatement("update movie set duration=?,certificate=? where "
     		 		+ "movie_id=? and (duration!=? or UPPER(certificate)!=UPPER(?))");
     		 
     		 ps.setTime(1,Time.valueOf(duration+":00"));
     		 ps.setString(2,certificate);
     		 ps.setInt(3,movie_id);
     		 ps.setTime(4,Time.valueOf(duration+":00"));
     		 ps.setString(5,certificate);
     		 int n=ps.executeUpdate();
  			 System.out.println("Number of rows affected : "+n);
     		 
     		 
              ps = con.prepareStatement("update movie set name=?,director=?,description=?,image=? where movie_id=?");
              ps.setString(1,name);
              ps.setString(2,director);
              ps.setString(3,description);
              ps.setString(4,image);
              ps.setInt(5,movie_id);
              ps.executeUpdate();
              
//              ps = con.prepareStatement("delete from movie_language_mapping where movie_id=?");
//              ps.setInt(1,movie_id);
//              ps.executeUpdate();
              
             
              ps = con.prepareStatement("insert into movie_language_mapping(movie_id,language_id) values(?,?) ON CONFLICT(movie_id,language_id) DO UPDATE set \"isAvailable\" = 1 ");
              ps.setInt(1,movie_id);
              for(int i=0;i<languages.size();i++)
              {
                  ps.setInt(2,languages.get(i));
                  ps.addBatch();
                  
              }
              ps.executeBatch();
              
              String sql="update movie_language_mapping set \"isAvailable\"=0 where movie_id=? and language_id not in (";
              
              ps=con.prepareStatement(sql);
              for (int i = 0; i < languages.size(); i++) {
                  sql += (i == 0 ? "?" : ", ?");
              }
              sql += ")";
              ps=con.prepareStatement(sql);
             
              ps.setInt(1,movie_id);
              for (int i = 0; i < languages.size(); i++) {
                 ps.setInt(i + 2, languages.get(i));
              }
              ps.executeUpdate();
              
              if(n==1)
              {
             	 ps=con.prepareStatement("select show.* from "
             	 		+ "show inner join movie_language_mapping "
             	 		+ "on show.movie_language_mapping_id=movie_language_mapping.movie_language_mapping_id "
             	 		+ "and movie_language_mapping.movie_id=? and status='Booking opened' where "
             	 		+ "show_date > CURRENT_DATE OR (show_date = CURRENT_DATE and start_time >= CURRENT_TIME)");
             	 ps.setInt(1,movie_id);
             	 rs=ps.executeQuery();
             	 while(rs.next())
             	 {
             		 int show_id=rs.getInt("show_id");
                 	 deleteShow(show_id);
             	 }
             	 
             	 System.out.println("Inside cancelling shows while update in duration or certificate");
             	 return getMovie(movie_id);
             	 
              }
              
              //Long[] langArray =languages.stream().map(Long::valueOf).toArray(Long[]::new);
              
//              for (int i = 0; i < languages.size(); i++) {
//                  langArray[i] = (long) languages.get(i);
//              }
              //Array sqlArray = con.createArrayOf("INTEGER", langArray);
              //Array sqlArray = con.createArrayOf("INTEGER",languages.toArray());
              
              //String values = String.join(",", languages.toString());
              //System.out.println("values "+values);
              sql="select show.* from show "
 		           		+ "inner join movie_language_mapping on show.movie_language_mapping_id=movie_language_mapping.movie_language_mapping_id and movie_language_mapping.movie_id=? and status='Booking opened' "
 		           		+ "where show_date > CURRENT_DATE OR (show_date = CURRENT_DATE and start_time >= CURRENT_TIME) "
 		           		+ "and language_id not in (";
              for (int i = 0; i < languages.size(); i++) {
                  sql += (i == 0 ? "?" : ", ?");
              }
              sql += ")";
              ps=con.prepareStatement(sql);
             
              ps.setInt(1,movie_id);
              for (int i = 0; i < languages.size(); i++) {
                 ps.setInt(i + 2, languages.get(i));
              }
              System.out.println(ps);
              rs=ps.executeQuery();
             
              while(rs.next())
              {
             	 int show_id=rs.getInt("show_id");
             	 deleteShow(show_id);
             	
              }
              
         
              System.out.println(ps);
              
              System.out.println("Movie updated");
             
              return getMovie(movie_id);
      } catch (SQLException ex) 
     	 {
          
              System.out.println("Exception in updating movie "+ex);
              ex.printStackTrace();
              return null;
          }
     }
     
     
     public Movie getMovie(int id)
     {
     	Movie movie = null;
    	 try {
             ps = con.prepareStatement("select movie.*,STRING_AGG(language,',') as languages from movie "
             		+ "inner join movie_language_mapping ON movie_language_mapping.movie_id = movie.movie_id and  movie.\"isAvailable\"=1 and movie_language_mapping.\"isAvailable\"=1 and movie.movie_id=?  "
             		+ "inner join language on movie_language_mapping.language_id = language.language_id group by movie.movie_id");
             ps.setInt(1,id);
 			ResultSet rs=ps.executeQuery();
             while(rs.next())
             {
             	movie_id=rs.getInt("movie_id");
             	name=rs.getString("name");
             	certificate=rs.getString("certificate");
             	director=rs.getString("director");
             	description=rs.getString("description");
             	image=rs.getString("image");
             	
             	sqlTime=rs.getTime("duration");
             	utilDate = new Date(sqlTime.getTime());
             	duration=utilDate.toInstant().atZone(java.time.ZoneId.systemDefault()).toLocalTime();
             	
             	sqlTimestamp=rs.getTimestamp("added_time");
             	utilDate = new Date(sqlTime.getTime());
             	added_time=utilDate.toInstant().atZone(java.time.ZoneId.systemDefault()).toLocalDateTime();
             	
                 String arr[]=rs.getString("languages").split(",");
             	
             	languages=new LinkedList<>(Arrays.asList(arr));	
             	
             	movie=new Movie(movie_id,name,certificate,languages,director,description,duration,image,added_time);
             }
             return movie;          
         } catch (SQLException ex) {
         	System.out.println(ex.getMessage());
         	return null;
         }
     }
     
     public void cancelMovie(int movie_id)
     {
     	try {
     		 ps = con.prepareStatement("select show.show_id from show "
     		 		+ "inner join movie_language_mapping "
     		 		+ "on show.movie_language_mapping_id=movie_language_mapping.movie_language_mapping_id and movie_language_mapping.movie_id=? and status='Booking opened' "
     		 		+ "where show_date > CURRENT_DATE OR (show_date = CURRENT_DATE and start_time >= CURRENT_TIME)");
     		 
              ps.setInt(1,movie_id);
              rs=ps.executeQuery();
              while(rs.next())
              {
             	 int show_id=rs.getInt(1);
             	 deleteShow(show_id);
              }
             
 	    	 ps = con.prepareStatement("update movie set \"isAvailable\"=0 where movie_id=?");
 	         ps.setInt(1,movie_id);
 	         ps.executeUpdate();
        } 
     	catch (SQLException ex) {
        	System.out.println(ex.getMessage());
        	
        }
     	
     }
     
     public List<Movie> getLatestMovies()
     {
      List<Movie> list=new LinkedList<Movie>();
    	 try {
             ps = con.prepareStatement("select movie.* from movie "
             		+ "inner join movie_language_mapping ON movie_language_mapping.movie_id = movie.movie_id "
             		+ "inner join show on show.movie_language_mapping_id=movie_language_mapping.movie_language_mapping_id and status='Booking opened' "
             		+ "where show_date > CURRENT_DATE OR (show_date = CURRENT_DATE and start_time >= CURRENT_TIME) "
             		+ "group by movie.movie_id  order by name;");
 			ResultSet rs=ps.executeQuery();
             while(rs.next())
             {
             	movie_id=rs.getInt("movie_id");
             	name=rs.getString("name");
             	//System.out.println("DB movie "+name);
             	certificate=rs.getString("certificate");
             	director=rs.getString("director");
             	description=rs.getString("description");
             	image=rs.getString("image");
             	
             	sqlTime=rs.getTime("duration");
             	utilDate = new Date(sqlTime.getTime());
             	duration=utilDate.toInstant().atZone(java.time.ZoneId.systemDefault()).toLocalTime();
             	
             	sqlTimestamp=rs.getTimestamp("added_time");
             	utilDate = new Date(sqlTime.getTime());
             	added_time=utilDate.toInstant().atZone(java.time.ZoneId.systemDefault()).toLocalDateTime();
             	
             	languages=new LinkedList<String>();
           
             	Movie movie=new Movie(movie_id,name,certificate,languages,director,description,duration,image,added_time);
             	list.add(movie); 
             }
             return list;          
         } catch (SQLException ex) {
         	System.out.println(ex.getMessage());
         	return null;
         }
     }
     
     public List<Movie> getUpcomingMovies()
     {
      List<Movie> list=new LinkedList<Movie>();
    	 try {
             ps = con.prepareStatement("select movie.* from movie where \"isAvailable\"=1 and  movie_id not in (select movie.movie_id "
             		+ "from movie inner join movie_language_mapping ON movie_language_mapping.movie_id = movie.movie_id "
             		+ "inner join show on show.movie_language_mapping_id=movie_language_mapping.movie_language_mapping_id and status='Booking opened' "
             		+ "where show_date > CURRENT_DATE OR (show_date = CURRENT_DATE and start_time >= CURRENT_TIME) "
             		+ "group by movie.movie_id  order by name) order by name");
 			ResultSet rs=ps.executeQuery();
             while(rs.next())
             {
             	movie_id=rs.getInt("movie_id");
             	name=rs.getString("name");
             	//System.out.println("DB movie "+name);
             	certificate=rs.getString("certificate");
             	director=rs.getString("director");
             	description=rs.getString("description");
             	image=rs.getString("image");
             	
             	sqlTime=rs.getTime("duration");
             	utilDate = new Date(sqlTime.getTime());
             	duration=utilDate.toInstant().atZone(java.time.ZoneId.systemDefault()).toLocalTime();
             	
             	sqlTimestamp=rs.getTimestamp("added_time");
             	utilDate = new Date(sqlTime.getTime());
             	added_time=utilDate.toInstant().atZone(java.time.ZoneId.systemDefault()).toLocalDateTime();
             	
             	languages=new LinkedList<String>();
             	Movie movie=new Movie(movie_id,name,certificate,languages,director,description,duration,image,added_time);
             	list.add(movie); 
             }
             return list;          
         } catch (SQLException ex) {
         	System.out.println(ex.getMessage());
         	return null;
         }
     }
     
     public JSONArray getShows(int movie_id)
     {
     	JSONArray arr=new JSONArray();
     	try
     	{
     		ps=con.prepareStatement("select movie.movie_id,movie.name,show.*,theater.name as theaterName,theater.theater_id,theater.district,"
     				+ "language.language,screen.screen_id,screen_name from show inner join movie_language_mapping on "
     				+ "(show.movie_language_mapping_id=movie_language_mapping.movie_language_mapping_id "
     				+ " and movie_language_mapping.movie_id=? and show.status='Booking opened') "
     				+ "inner join screen on screen.screen_id=show.screen_id "
     				+ "inner join movie on movie.movie_id=movie_language_mapping.movie_id "
     				+ "inner join language on language.language_id=movie_language_mapping.language_id "
     				+ "inner join theater on theater.theater_id=screen.theater_id "
     				+ "where show_date > CURRENT_DATE OR (show_date = CURRENT_DATE and start_time >= CURRENT_TIME) order by show_date,show.start_time");
     		
     		ps.setInt(1, movie_id);
     		rs=ps.executeQuery();
     		while(rs.next())
     		{
     			JSONObject s=new JSONObject();
     			//s.put("movie_id",rs.getInt("movie_id"));
     			s.put("movie_name",rs.getString("name"));
     			s.put("show_id",rs.getInt("show_id"));
     			java.sql.Date t=(java.sql.Date)rs.getDate("show_date");
     			s.put("show_date",t.toLocalDate().toString());
     			Time tt=rs.getTime("start_time");
     			s.put("start_time",tt.toLocalTime().toString());
//     			tt=rs.getTime("end_time");
//     			s.setEnd_time(tt.toLocalTime());
     			s.put("language",rs.getString("language"));
     			s.put("theater_name",rs.getString("theaterName"));
     			s.put("district",rs.getString("district"));
     			s.put("screen_id",rs.getInt("screen_id"));
     			s.put("screen_name",rs.getString("screen_name"));
     			
     			arr.add(s);
     		}
     	}
     	catch (SQLException ex) 
  	    { 
      
           System.out.println("Exception in retriving shows "+ex);
         }
     	return arr;
     }
     
     
     
     //ShowDAO
     public List<String> addShow(int theater_id,int screen_id,JSONObject jsonData)
     {
     	int movie_id=((Long) jsonData.get("movie_id")).intValue();
     	String language=(String)jsonData.get("language");
     	String start_time=(String)jsonData.get("start-time");
     	String start_date=(String)jsonData.get("start-date");
     	String end_date=(String)jsonData.get("end-date");
     	String show_duration=(String)jsonData.get("show-duration");
     	JSONArray arr=(JSONArray)jsonData.get("slot");
     	//System.out.println(no_of_shows);
     	int vip_prize =Integer.parseInt((String)jsonData.get("vip-prize"));
     	int premium_prize=Integer.parseInt((String)jsonData.get("premium-prize"));
     	int normal_prize=Integer.parseInt((String)jsonData.get("normal-prize"));
     	
     	int vip_cancel =Integer.parseInt((String)jsonData.get("vip-cancel"));
     	int premium_cancel=Integer.parseInt((String)jsonData.get("premium-cancel"));
     	int normal_cancel=Integer.parseInt((String)jsonData.get("normal-cancel"));
     	
     	
     	int movie_language_mapping_id=0;
     	List<String> message=new LinkedList<String>();
     	try
     	{
     		ps=con.prepareStatement("select movie_language_mapping_id from movie_language_mapping inner join language on movie_language_mapping.language_id=language.language_id where movie_id=? and language=?");
     		ps.setInt(1, movie_id);
     		ps.setString(2,language);
     		rs=ps.executeQuery();
     		while(rs.next())
     		{
     			movie_language_mapping_id=rs.getInt(1);
     			break;
     		}
     	}
     	catch (SQLException ex) 
  	     { 
           System.out.println("Exception in getting movie_lang id"+ex);
         }
     	try{
             
             LocalDate startDate=LocalDate.parse(start_date);
             LocalDate endDate=LocalDate.parse(end_date);
             int days = (int)ChronoUnit.DAYS.between(startDate, endDate)+1;
             LocalTime duration=LocalTime.parse(show_duration);
             
             
             ps=con.prepareStatement("insert into show(show_date,start_time,end_time,screen_id,movie_language_mapping_id,vip_prize,premium_prize,normal_prize,status,vip_cancel,premium_cancel,normal_cancel) values(?,?,?,?,?,?,?,?,?,?,?,?)");
             ps.setInt(4,screen_id);
             ps.setInt(5,movie_language_mapping_id);
             ps.setInt(6,vip_prize);
             ps.setInt(7,premium_prize);
             ps.setInt(8,normal_prize);
             ps.setString(9,"Booking opened");
             ps.setInt(10,vip_cancel);
             ps.setInt(11,premium_cancel);
             ps.setInt(12,normal_cancel);
             
             for(int i=0;i<arr.size();i++)//Iterating through all slots arr
             {
             	String[] slot=((String)arr.get(i)).split("-");
             	LocalTime startTime=LocalTime.parse(slot[0]);
             	LocalTime endTime=LocalTime.parse(slot[1]);	
             	ps.setTime(2, Time.valueOf(startTime));
     			ps.setTime(3,Time.valueOf(endTime));
     			
     			
     			 //get unavailable show dates for the particular time slot
     			 PreparedStatement  ps1 = con.prepareStatement("select show.show_date from show "
             			  		  + "where screen_id=? AND (show_date BETWEEN ? AND ?) and show.status='booking opened' and "
             			       	  + " (((show.start_time BETWEEN ? AND ?) OR (end_time BETWEEN ? AND ?)) or "
             			     	  + " (? BETWEEN show.start_time AND end_time) ) group by show.show_date");
             	ps1.setInt(1,screen_id);
             	ps1.setDate(2,java.sql.Date.valueOf(startDate));
             	ps1.setDate(3,java.sql.Date.valueOf(endDate));
             	ps1.setTime(4,Time.valueOf(startTime));
             	ps1.setTime(5,Time.valueOf(endTime));
             	ps1.setTime(6,Time.valueOf(startTime));
             	ps1.setTime(7,Time.valueOf(endTime));
             	ps1.setTime(8,Time.valueOf(startTime));
             	rs=ps1.executeQuery();
             	List<String> al=new ArrayList<String>();
             	while(rs.next())
             	{
             		String t=rs.getDate(1).toString();
             		message.add("Date : "+t+", Slot : "+startTime+"-"+endTime+".");
                     al.add(t);
             	}
             	LocalDate showDate=startDate;
             	
             	//after getting list of unavailable dates iterate through all the dates,if date not present in array add show for the day
         		for(int j=days;j>0;j--)
         		{
         			if(!al.contains(showDate.toString()))
         			{
 	        			ps.setDate(1,java.sql.Date.valueOf(showDate));
 	        			ps.addBatch();
         			}
         			
         			showDate=showDate.plusDays(1);
         		}
         		startTime=endTime.plusMinutes(10);
         		
         	}
             
             ps.executeBatch();
            
           } catch (SQLException ex) 
    	      { 
         
             System.out.println("Exception in adding show "+ex);
         
           }
           return message;
   
     }
     
     public JSONArray getAllShows(int theater_id,int screen_id)
     {
     	JSONArray arr=new JSONArray();
     	try
     	{
     		ps=con.prepareStatement("select show.*,language,name from show inner join movie_language_mapping on "
     				+ "movie_language_mapping.movie_language_mapping_id=show.movie_language_mapping_id and show.screen_id=? "
     				+ "inner join movie on movie.movie_id = movie_language_mapping.movie_id "
     				+ "inner join language on movie_language_mapping.language_id=language.language_id "
     				//+ "where show_date > CURRENT_DATE OR (show_date = CURRENT_DATE and end_time >= CURRENT_TIME) "
     				+ "order by show_date ,start_time");
     		ps.setInt(1, screen_id);
     		rs=ps.executeQuery();
     		while(rs.next())
     		{
     			JSONObject show= new JSONObject();
     			show.put("show_id",rs.getInt("show_id"));
     			show.put("show_date",rs.getDate("show_date").toString());
     			show.put("start_time",rs.getTime("start_time").toString());
     			show.put("end_time",rs.getTime("end_time").toString());
     			show.put("movie_name", rs.getString("name"));
     			show.put("language", rs.getString("language"));
     			show.put("vip_prize",rs.getInt("vip_prize"));
     			show.put("premium_prize",rs.getInt("premium_prize"));
     			show.put("normal_prize",rs.getInt("normal_prize"));
     			show.put("vip_cancel",rs.getInt("vip_cancel"));
     			show.put("premium_cancel",rs.getInt("premium_cancel"));
     			show.put("normal_cancel",rs.getInt("normal_cancel"));
     			show.put("status",rs.getString("status"));
     			
     			arr.add(show);
     		}
     		
     	}
     	catch (SQLException ex) 
  	    { 
           
           System.out.println("Exception in retriving shows "+ex);
         }
     	return arr;
     }
     
     public String updateShow(int show_id,JSONObject jsonData)
     {
     	int vip_prize =Integer.parseInt((String)jsonData.get("vip_prize"));
     	int premium_prize=Integer.parseInt((String)jsonData.get("premium_prize"));
     	int normal_prize=Integer.parseInt((String)jsonData.get("normal_prize"));
     	
     	int vip_cancel =Integer.parseInt((String)jsonData.get("vip_cancel"));
     	int premium_cancel=Integer.parseInt((String)jsonData.get("premium_cancel"));
     	int normal_cancel=Integer.parseInt((String)jsonData.get("normal_cancel"));
     	
     	try
     	{
     		 ps=con.prepareStatement("update show set vip_prize=?,premium_prize=?,normal_prize=?,vip_cancel=?,premium_cancel=?,normal_cancel=? where show_id=?");
     		 ps.setInt(1,vip_prize);
              ps.setInt(2,premium_prize);
              ps.setInt(3,normal_prize);
     		 
     		 ps.setInt(4,vip_cancel);
              ps.setInt(5,premium_cancel);
              ps.setInt(6,normal_cancel);
              
              ps.setInt(7,show_id);
     		 ps.executeUpdate();
     		
     	}
     	catch (SQLException ex) 
  	    { 
           System.out.println("Exception in deleting show "+ex);
         }
     	return "Ticket Prize Updated..";
     }
     
     
     public String deleteShow(int show_id)
     {
     	try
     	{
     		
     		ps=con.prepareStatement("update booking set status='Show cancelled' where show_id=? and booking.status='Booked'");
     		ps.setInt(1,show_id);
     		ps.executeUpdate();
     		
     		ps=con.prepareStatement("update show set status='Cancelled' where show_id=?");
     		ps.setInt(1,show_id);
     		ps.executeUpdate();
     		
     		ps=con.prepareStatement("select max(user_id),booking.booking_id,sum(seat_prize) as prize from booking inner join "
     				+ " ticket on ticket.booking_id = booking.booking_id and show_id=? and booking.status='Show cancelled' group by booking.booking_id;");
     		ps.setInt(1,show_id);
     		rs=ps.executeQuery();
     		int total=0;
     		while(rs.next())
     		{
     			int user_id=rs.getInt(1);
     			int booking_id=rs.getInt(2);
     			System.out.println("booking ID : "+booking_id);
     			int amt=rs.getInt(3);
     			total=total+amt;
     			
     			PreparedStatement ps1=con.prepareStatement("update ticket set refund=seat_prize where booking_id=?");
     			ps1.setInt(1,booking_id);
     			ps1.executeUpdate();
     			
     			ps1=con.prepareStatement("update users set wallet=wallet+? where user_id=?");
     			ps1.setInt(1,amt);
     			ps1.setInt(2, user_id);
     			ps1.executeUpdate();
     			
     		}
     		
     		ps=con.prepareStatement("update theater set wallet=wallet-? where theater_id=(select theater.theater_id from theater inner join screen on theater.theater_id=screen.theater_id inner join show on show.screen_id=screen.screen_id and show_id=?)");
     		ps.setInt(1, total);
     		ps.setInt(2,show_id);
     		ps.executeUpdate();
     	}
     	catch (SQLException ex) 
  	    { 
           System.out.println("Exception in deleting show "+ex);
         }
     	return "Show deleted";
     }
     
     public String bookTicket(int show_id,JSONObject jsonData)
     {
     	int booking_id=0;
     	JSONArray arr=(JSONArray) jsonData.get("selectedSeats");
     	int n=arr.size();
     	int offer_id=-1,discount=0;
     	String offer_name="";
     	//check for any offer applicable
     	try
     	{
     		ps=con.prepareStatement("select discount,offer_id,offer_name from offer where start_DATE <= CURRENT_DATE and end_date >=CURRENT_DATE  and no_of_tickets <= ? and \"isAvailable\"=1 order by no_of_tickets,discount desc,start_date,end_date ");
     		ps.setInt(1,n);
     		rs=ps.executeQuery();
     		while(rs.next())
     		{
     			int t=rs.getInt(1);
     			if(t>discount)
     			{
     				discount=t;
     				offer_id=rs.getInt(2);
     				offer_name=rs.getString(3);
     			}
     			
     		}
     	}
     	catch(SQLException e)
     	{
     		System.out.println("Exception in finding offer "+e);
     		return "Exception in finding offer ";
     	}
     	
     	
     
     	JSONObject cancel_percentage=getCancellationPercentage(show_id);
     	try
     	{
     		ps=con.prepareStatement("insert into booking(show_id,user_id,status,offer_id,vip_cancel,premium_cancel,normal_cancel) values(?,?,?,?,?,?,?)");
     		ps.setInt(1,show_id);
     		ps.setInt(2, Integer.parseInt((String)jsonData.get("user_id")));
     		ps.setString(3,"Booked");
     		//ps.setInt(4,Integer.parseInt((String)jsonData.get("amount")));
     		if(offer_id!=-1)
     		{
     			ps.setInt(4,offer_id);
     		}
     		else
     		{
     			ps.setNull(4,Types.BIGINT);
     		}
     		System.out.println("show Premium cancel% "+(int)cancel_percentage.get("vip_cancel"));
     		ps.setInt(5,(int)cancel_percentage.get("vip_cancel"));
     		ps.setInt(6,(int)cancel_percentage.get("premium_cancel"));
     		ps.setInt(7,(int)cancel_percentage.get("normal_cancel"));
     		ps.executeUpdate();
     		ps=con.prepareStatement("select booking_id from booking order by booked_time desc limit 1");
     		rs=ps.executeQuery();
     		while(rs.next())
     		{
     			booking_id=rs.getInt(1);
     			break;
     		}
     		System.out.println("Booking_id "+booking_id);
     	}
     	catch(SQLException e)
     	{
     		System.out.println("Exception in booking tickets "+e);
     		return "Exception in adding bookings table";
     	}
     	 
     	//new code start since all the seat number,prizes,types of selected seats are coming as payload from client..so any one can change the payload..So better get selected seatnumbers alone and generate type,and prize from db at backend side 
     	/*
     	int screen_id=0;
     	int vip_prize=0,premium_prize=0,normal_prize=0;
     	
        try 
        {
     		ps=con.prepareStatement("select * from show where show_id=?");
     		ps.setInt(1,show_id);
     		while(rs.next())
     		{
     			screen_id=rs.getInt("screen_id");
     			premium_prize=rs.getInt("premium_prize");
     			vip_prize=rs.getInt("vip_prize");
     			normal_prize=rs.getInt("normal_prize");
     		}
     		
     	}
     	catch(SQLException e)
     	{
     		System.out.println("Exception in getting show Details while booking tickets");
     	}
     	
     	
     	
     	try 
     	{
     		String sql="select seat_number,seat_type from (select seat_type,concat(row_name,'',column_number) as seat_number "
     				+ "from seating_arrangement where screen_id=?) as sub where seat_number in(";
     		
     		for(int i=0;i<arr.size();i++)
         	{
         		sql += (i == 0 ? "?" : ", ?");
         	}
     		sql += ")";
     		ps=con.prepareStatement(sql);
     		ps.setInt(1,screen_id);
     		for(int i=0;i<arr.size();i++)
         	{
     			JSONObject t=(JSONObject) arr.get(i);
         		ps.setString(i+2,(String)t.get("number"));
         	}
     		System.out.println(ps);
     		
     		JSONArray arr1=new JSONArray();
     		while(rs.next())
     		{
     			String number=rs.getString("seat_number");
     			String type=rs.getString("seat_type");
     			int prize=-1;
     			if(type.equalsIgnoreCase("VIP"))
     				prize=vip_prize;
     			else if(type.equalsIgnoreCase("Premium"))
     				prize=premium_prize;
     			else if(type.equalsIgnoreCase("Normal"))
     				prize=normal_prize;
     			if(prize!=-1)
     			{
     				JSONObject seat= new JSONObject();
     				seat.put("number", number);
     				seat.put("type", type);
     				seat.put("prize",prize);
     				
     				arr1.add(seat);
     			}
     		}
     		for(int i=0;i<arr1.size();i++)
         	{
     			JSONObject t=(JSONObject) arr.get(i);
     			System.out.println(t.get("number")+" "+t.get("type")+" "+t.get("prize"));
         	}
     	}
     	catch(SQLException e)
     	{
     		System.out.println("Exception in getting seatDetails for selected seats");
     	}
     	*/
       //new code end
     	try
     	{
     	ps=con.prepareStatement("insert into ticket(booking_id,seat_number,seat_type,seat_prize,discount) values(?,?,?,?,?)");
     	ps.setInt(1, booking_id);
     	
     	int total=0;
     	for(int i=0;i<arr.size();i++)
     	{
     		JSONObject t=(JSONObject) arr.get(i);
     		ps.setString(2,(String)t.get("number"));
     		ps.setString(3,(String)t.get("type"));
     		
     		int prize=((Long)t.get("prize")).intValue();
     		if(offer_id!=-1)
     		{
     			prize=prize-(int)(Math.round(prize*0.01*discount));
     		}
     		ps.setInt(4,prize);
     		total=total+prize;
     		ps.setInt(5,discount);
     		ps.addBatch();
     	}
     	 ps.executeBatch();
     	 
     	 ps=con.prepareStatement("update users set wallet= CASE WHEN (wallet - ?) < 0 THEN 0 ELSE wallet - ? END where user_id=?");
     	 ps.setInt(1, total);
     	 ps.setInt(2, total);
     	 ps.setInt(3, Integer.parseInt((String)jsonData.get("user_id")));
     	 ps.executeUpdate();
     	 
     	 ps=con.prepareStatement("update theater set wallet=wallet+? where theater_id=(select theater.theater_id from theater inner join screen on screen.theater_id = theater.theater_id inner join show ON show.screen_id = screen.screen_id and show_id=? )");
     	 ps.setInt(1, total);
     	 ps.setInt(2,show_id);
     	 ps.executeUpdate();
     	 
     	 
     	}
 		catch(SQLException e)
 		{
 			System.out.println("Exception in booking tickets "+e);
 			return "Exception in adding tickets table";
 		}
     	if(offer_id!=-1)
 		{
 		  return "Tickets booked!!!.You got "+discount+"% offer by "+offer_name;
 		}
     	return "Tickets booked!!!";
     }
     
     public JSONObject getCancellationPercentage(int show_id)
     {
     	JSONObject t=new JSONObject();
     	try
     	{
     		ps=con.prepareStatement("select * from show where show_id=?");
     		ps.setInt(1, show_id);
     		rs=ps.executeQuery();
     		if(rs.next())
     		{
     			t.put("vip_cancel",rs.getInt("vip_cancel"));
     			t.put("premium_cancel",rs.getInt("premium_cancel"));
     			t.put("normal_cancel",rs.getInt("normal_cancel"));
     		}
     	}
     	catch(SQLException e)
 		{
 			System.out.println("Exception in getting cancellation percentage "+e);
 		}
     	return t;
     }
     
     public JSONObject getBookingCancellationPercentage(int booking_id)
     {
     	JSONObject t=new JSONObject();
     	try
     	{
     		ps=con.prepareStatement("select * from booking where booking_id=?");
     		ps.setInt(1, booking_id);
     		rs=ps.executeQuery();
     		if(rs.next())
     		{
     			t.put("vip_cancel",rs.getInt("vip_cancel"));
     			t.put("premium_cancel",rs.getInt("premium_cancel"));
     			t.put("normal_cancel",rs.getInt("normal_cancel"));
     		}
     	}
     	catch(SQLException e)
 		{
 			System.out.println("Exception in getting cancellation percentage for booking "+e);
 		}
     	return t;
     }
     
     public JSONArray getCollection(int show_id) 
     {
     	JSONArray arr=new JSONArray();
     	try
     	{
     		ps=con.prepareStatement("select booking.booking_id,STRING_AGG(ticket.seat_number,',') AS seats,COUNT(ticket.ticket_id) as count,"
     				+ "SUM(ticket.seat_prize) as prize,"
     				+ "max(booking.status) as status,SUM(ticket.refund) as refund,MAX(users.email) as user "
     				+ "from ticket inner join booking on "
     				+ "booking.booking_id=ticket.booking_id and booking.show_id=? "
     				+ "inner join show show on show.show_id=booking.show_id "
     				+ "inner join users on booking.user_id=users.user_id "
     				+ "group by booking.booking_id order by booking.booked_time "
     				+ "");
     		ps.setInt(1,show_id);
     		rs=ps.executeQuery();
     		while(rs.next())
     		{
     			JSONObject s=new JSONObject();
     			s.put("booking_id",rs.getInt("booking_id"));
     			s.put("seats",rs.getString("seats"));
     			s.put("count",rs.getInt("count"));
     			s.put("prize",rs.getInt("prize"));
     			s.put("status",rs.getString("status"));
     			s.put("refund",rs.getInt("refund"));
     			s.put("user", rs.getString("user"));
     			
     			arr.add(s);
     		}
     	}
     	catch(SQLException e)
 		{
 			System.out.println("Exception in getting show collection "+e);
 		}
     	finally
     	{
     		if(ps!=null)
 				try {
 					ps.close();
 				} catch (SQLException e1) {
 					// TODO Auto-generated catch block
 					e1.printStackTrace();
 				}
     		if(rs!=null)
 				try {
 					rs.close();
 				} catch (SQLException e) {
 					// TODO Auto-generated catch block
 					e.printStackTrace();
 				}
     	}
     	
     	return arr;
     }
     
     
     //seatingArrangementDAO
     public JSONObject updateSeatingArrangement(int theater_id,int screen_id,JSONObject jsonObj)
     {
     	
     	JSONArray arr=(JSONArray)jsonObj.get("path");
     	int total_rows=((Long) jsonObj.get("total_rows")).intValue();
     	int total_columns=((Long) jsonObj.get("total_columns")).intValue();
     	int vip=((Long) jsonObj.get("vip")).intValue();
     	int premium=((Long) jsonObj.get("premium")).intValue();
     	int normal=((Long)  jsonObj.get("normal")).intValue();
     	int pathCount=arr.size();
     	int seatCount=(total_rows*total_columns)-pathCount;
     	
     	
     	try {
     		ps = con.prepareStatement("delete from seating_arrangement where screen_id=?");
     		ps.setInt(1,screen_id);
     		ps.executeUpdate();
     	}
     	catch(SQLException e)
     	{
     		System.out.println("Exception in deleting the existing seat arrangement "+e);
     	}
     	
     	char c='A';
     	String type="VIP";
     	try
 		{
     		ps=con.prepareStatement("insert into seating_arrangement(screen_id,row_name,column_number,seat_type) values(?,?,?,?)");
 	    	for(int i=0;i<total_rows;i++)
 	    	{
 	    		if(i<vip)
 	    		{
 	    			type="VIP";
 	    		}
 	    		else if(i<(vip+premium))
 	    			type="Premium";
 	    		else
 	    			type="Normal";
 	    		
 	    		for(int j=1;j<=total_columns;j++)
 	    		{
 	    				ps.setInt(1,screen_id);
 	    				ps.setString(2,String.valueOf(c));
 	    				ps.setInt(3,j);
 	    				String seat=c+""+j;
 	    				if(!arr.contains(seat))
 	    				{
 	    				  ps.setString(4,type);
 	    				}
 	    				else
 	    				{
 	    					ps.setString(4,"P");
 	    				}
 	    				ps.addBatch();
 	    		}
 	    		c++;
 	    	}
 	    	ps.executeBatch();
 		}
 		catch(SQLException e)
     	{
     		System.out.println("Exception in inserting  seat arrangement "+e);
     	}
     	
     	//cancel and refund all the upcoming shows
     	try
     	{
     		ps=con.prepareStatement("select show_id from show where screen_id=? and show.status='Booking opened' and (show_date > CURRENT_DATE OR (show_date = CURRENT_DATE and start_time >= CURRENT_TIME))");
     		ps.setInt(1,screen_id);
     		rs=ps.executeQuery();
     		while(rs.next())
     		{
     			deleteShow(rs.getInt(1));
     		}
     		
     	}
     	catch(SQLException e)
     	{
     		System.out.println("Exception in cancelling for shows while updating seat arrangement "+e);
     	}
     	  	
     	return getSeatingArrangement(screen_id);
     }
     
     public JSONObject getSeatingArrangement(int screen_id)
     {
     	JSONArray seats = new JSONArray();
         Screen sc=null;
         int columns=0;
     	try
     	{
     		ps = con.prepareStatement("select * from seating_arrangement where screen_id=? order by row_name,column_number");
             ps.setInt(1,screen_id);
 			ResultSet rs=ps.executeQuery();
             while(rs.next())
             {
             	JSONObject seat=new JSONObject();
             	seat.put("seating_id",rs.getInt("seating_id"));
             	seat.put("screen_id",rs.getInt("screen_id"));
             	seat.put("row_name", rs.getString("row_name"));
             	seat.put("column_number", rs.getString("column_number"));
             	seat.put("seat_type",rs.getString("seat_type"));
             	seats.add(seat);
             }
     	}
     	catch(SQLException e)
     	{
     		System.out.println("Exception in retriving  seat arrangement "+e);
     	}
     	columns=getColumnCount(screen_id);
         JSONObject result=new JSONObject();
         result.put("seats", seats);
         result.put("columns", columns);
         
         return result;
     }
     
     
     public Screen getScreenDetails(int screen_id,int columns )
     {
     	Screen sc=null;
     	try
     	{
     		ps = con.prepareStatement("select * from screen where screen_id=?");
             ps.setInt(1,screen_id);
 			ResultSet rs=ps.executeQuery();
             while(rs.next())
             {
             	sc=new Screen(rs.getInt("screen_id"),rs.getInt("theater_id"),rs.getString("screen_name"));
             	break;
             }
     	}
     	catch(SQLException e)
     	{
     		System.out.println("Exception in retriving screen "+e);
     	}
     	return sc;
     }
     
     public int getColumnCount(int screen_id)
     {
     	int columns=0;
     	try
     	{
     		ps = con.prepareStatement("select count(distinct(column_number)) from seating_arrangement where screen_id=? and row_name='A'");
             ps.setInt(1,screen_id);
 			ResultSet rs=ps.executeQuery();
             while(rs.next())
             {
             	columns=rs.getInt(1);
             }
     	}
     	catch(SQLException e)
     	{
     		System.out.println("Exception in retriving screen column count "+e);
     	}
     	return columns;
     }
     
     
     public JSONObject getShowSeatingArrangement(int show_id)
     {
     	JSONArray booked = new JSONArray();
         int columns=0;
         int screen_id=0;
         int vip_prize=0,premium_prize=0,normal_prize=0;
         int vip_cancel=0,premium_cancel=0,normal_cancel=0;
     	try
     	{
     		ps = con.prepareStatement("select screen_id,vip_prize,premium_prize,normal_prize,vip_cancel,premium_cancel,normal_cancel from show where show_id=?");
             ps.setInt(1,show_id);
 			ResultSet rs=ps.executeQuery();
             while(rs.next())
             {
             	screen_id=rs.getInt("screen_id");
             	vip_prize=rs.getInt("vip_prize");
             	premium_prize=rs.getInt("premium_prize");
             	normal_prize=rs.getInt("normal_prize");
             	vip_cancel=rs.getInt("vip_cancel");
             	premium_cancel=rs.getInt("premium_cancel");
             	normal_cancel=rs.getInt("normal_cancel");
             }
     	}
     	catch(SQLException e)
     	{
     		System.out.println("Exception in retriving  screen_id for the show "+e);
     	}
     	
     	JSONObject result=getSeatingArrangement(screen_id);
     	result.put("vip_prize",vip_prize);
     	result.put("premium_prize",premium_prize);
     	result.put("normal_prize",normal_prize);
     	
     	result.put("vip_cancel",vip_cancel);
     	result.put("premium_cancel",premium_cancel);
     	result.put("normal_cancel",normal_cancel);
     	try
     	{
     		ps = con.prepareStatement("select seat_number from ticket inner join booking on ticket.booking_id=booking.booking_id and booking.show_id=? and booking.status='Booked'");
             ps.setInt(1,show_id);
 			ResultSet rs=ps.executeQuery();
             while(rs.next())
             {
             	booked.add(rs.getString(1));
             }
     	}
     	catch(SQLException e)
     	{
     		System.out.println("Exception in retriving  booked seats for the show "+e);
     	}
     	result.put("booked",booked);
     	
     	return result;
     }
     
     
     
     //ScreenDAO
     
     private int screen_id;
     private int theater_id;
     private String screen_name;
     private int no_of_seats;
     private int normal_row;
     private int premium_row;
     private int vip_row;
   

     public List<Screen> getScreens(int theater_id)
     {
     	List<Screen> list=new LinkedList<Screen>();
     	 try {
             ps = con.prepareStatement("select * from screen  where theater_id=? and \"isAvailable\"=1 order by screen_name");
             ps.setInt(1, theater_id);
     		ResultSet rs=ps.executeQuery();
             while(rs.next())
             {
             	screen_id=rs.getInt("screen_id");
             	screen_name=rs.getString("screen_name");
             	theater_id=rs.getInt("theater_id");
             	
             	Screen screen=new Screen(screen_id,theater_id,screen_name);
             	list.add(screen); 
             }
             return list;          
         } catch (SQLException ex) {
         	System.out.println(ex);
         	return null;
         }
     }

     public Screen getScreen(int theater_id,int screen_id)
     {
     	 Screen screen=null;
     	 try {
             ps = con.prepareStatement("select * from screen where theater_id=? and screen_id=?");
             ps.setInt(1,theater_id);
             ps.setInt(2,screen_id);
     		ResultSet rs=ps.executeQuery();
             while(rs.next())
     	    {	
             	screen_id=rs.getInt("screen_id");
     	    	screen_name=rs.getString("screen_name");
     	    	no_of_seats=rs.getInt("no_of_seats");
     	    	normal_row=rs.getInt("normal_row");
     	    	premium_row=rs.getInt("premium_row");
     	    	vip_row=rs.getInt("vip_row");
     	    	
     	    	screen=new Screen(screen_id,theater_id,screen_name);
             	
             }
             return screen;          
         } catch (SQLException ex) {
         	System.out.println(ex);
         	return null;
         }
     }

     public String updateScreen(int screen_id,JSONObject jsonData)
     {
     	String screen_name=(String)jsonData.get("screen_name");
     	try 
     	{
     		ps=con.prepareStatement("update screen set screen_name=? where screen_id=? ");
     		ps.setString(1, screen_name);
     		ps.setInt(2, screen_id);
     		ps.executeUpdate();
     		return "Screen Details updated!!";
     	}
     	catch (SQLException ex) 
     	{ 
     	
     	  System.out.println("Error in updating screen : "+ex);
     	  return "Error in updating screen : "+ex;
     	  
     	}
     }

     public String removeScreen(int screen_id)
     {
     	try 
     	{
     		ps=con.prepareStatement("select show.* from show inner join screen on screen.screen_id=show.screen_id "
     				+ "and show.screen_id=? and status='Booking opened' where show_date > CURRENT_DATE "
     				+ "OR (show_date = CURRENT_DATE and start_time >= CURRENT_TIME)");
     		ps.setInt(1,screen_id);
     		rs=ps.executeQuery();
     		while(rs.next())
     		{
     			int show_id=rs.getInt("show_id");
     			deleteShow(show_id);
     			System.out.println("Sho_id"+show_id);
     		}
     		
     		ps=con.prepareStatement("update screen set \"isAvailable\"=0 where screen_id=? ");
     		ps.setInt(1, screen_id);
     		ps.executeUpdate();
     		return "Screen Removed!!";
     	}
     	catch (SQLException ex) 
     	{ 
     	
     	  System.out.println("Error in removing screen : "+ex);
     	  return "Error in removing screen : "+ex;
     	  
     	}
     }


     	public JSONArray getAvailableSlots(int screen_id,JSONObject jsonData)
     	{
     		JSONArray arr=new JSONArray();
     		
         	String start_time=(String)jsonData.get("start_time");
         	String start_date=(String)jsonData.get("start_date");
         	String end_date=(String)jsonData.get("end_date");
         	String show_duration=(String)jsonData.get("show_duration");
         	String max_start_time=(String)jsonData.get("max_start_time");
         	try{
            
                 LocalDate startDate=LocalDate.parse(start_date);
                 LocalDate endDate=LocalDate.parse(end_date);
                 int days = (int)ChronoUnit.DAYS.between(startDate, endDate)+1;
                 
                 
                 
                 LocalTime startTime=LocalTime.parse(start_time);
                 LocalTime duration=LocalTime.parse(show_duration);
                 LocalTime maxStartTime=LocalTime.parse("23:59:00");
                 
                 boolean isWrongData=false;
                 
                 while(((startTime.compareTo(maxStartTime))<=0))
             	{
                 	System.out.println(startTime);
                 	
                 	LocalTime endTime=startTime.plusHours(duration.getHour())
                                       .plusMinutes(duration.getMinute())
                                       .plusSeconds(duration.getSecond());
                 	
                 	ps = con.prepareStatement("select show.show_date from show "
                 			+ "where screen_id=? AND (show_date BETWEEN ? AND ?) and show.status='Booking opened' and "
                 			+ " (((show.start_time BETWEEN ? AND ?) OR (end_time BETWEEN ? AND ?)) or"
                 			+ " (? BETWEEN show.start_time AND end_time) ) group by show.show_date");
                 	//checking condition if the new time period is completely lies inside existing show
                 	ps.setInt(1,screen_id);
                 	ps.setDate(2,java.sql.Date.valueOf(startDate));
                 	ps.setDate(3,java.sql.Date.valueOf(endDate));
                 	ps.setTime(4,Time.valueOf(startTime));
                 	ps.setTime(5,Time.valueOf(endTime));
                 	ps.setTime(6,Time.valueOf(startTime));
                 	ps.setTime(7,Time.valueOf(endTime));
                 	ps.setTime(8,Time.valueOf(startTime));
                 	
                 	rs=ps.executeQuery();
                 	int cnt=0;
                 	while(rs.next())
                 	{
                 		cnt++;
                 	}
                 	if(cnt<days)
                 	{
                 		
                 			JSONObject s=new JSONObject();
                 			//while parsing ajax response to json it shows error.because json cannot parse Date..so convert to string
                 			s.put("start_time", startTime.toString());
                 			s.put("end_time",endTime.toString());
                 			arr.add(s);
                 		
                 	}
                     
                 	//System.out.println("Start Time : "+startTime+" - End : "+endTime);
                 	//System.out.println("is end time is less than start  : "+endTime.compareTo(startTime));
                 	
                 	if((endTime.compareTo(startTime)) < 0)//if end-time of current show is 00:30:00 then that is the last show for that day
             		break;
                 	
                 	
             		startTime=endTime.plusMinutes(10);
             		
             		if(startTime.compareTo(endTime)<0) //if end time of previous show is 23:55 then startTime of next show 00:05
             		break;
             	
             	}
               } 
         	  catch (SQLException ex) 
        	      { 
             
                 System.out.println("Error : "+ex);
                 
               }
         	return arr;
     	}
     	
     	public String addScreen(JSONObject jsonData)
     	{
     		int theater_id=Integer.parseInt((String)jsonData.get("theater_id"));
     		String screen_name=(String)jsonData.get("screen_name");
     		try
     		{
     			ps=con.prepareStatement("insert into screen(screen_name,theater_id) values(?,?)");
     			ps.setString(1, screen_name);
     			ps.setInt(2, theater_id);
     			ps.executeUpdate();
     			return "Screen Added!!";
     		}
     		catch (SQLException ex) 
      	    { 
           
               System.out.println("Error in adding screen : "+ex);
               return "Error in adding screen : "+ex;
               
             }
     		
     	}
     	
     	public JSONArray getScreenCollection(int screen_id)
     	{
     		JSONArray arr=new JSONArray();
     		try
     		{
     			ps=con.prepareStatement("select show.show_id,Max(show.screen_id) AS screen_id,"
     					+ "MAX(show.show_date) as show_date,MAX(start_time) as start_time,MAX(show.status) as show_status,"
     					+ "MAX(movie.name) as movie_name,max(language) as language,"
     					+ "(SUM(ticket.seat_prize) - SUM(ticket.refund))as collection from ticket "
     					+ "inner join booking on booking.booking_id=ticket.booking_id "
     					+ "inner join show on show.show_id=booking.show_id and show.screen_id=? "
     					+ "inner join movie_language_mapping on show.movie_language_mapping_id=movie_language_mapping.movie_language_mapping_id "
     					+ "inner join movie on movie.movie_id = movie_language_mapping.movie_id "
     					+ "inner join language on movie_language_mapping.language_id=language.language_id "
     					+ "group by show.show_id order by show_date,start_time");
     			ps.setInt(1,screen_id);
     			rs=ps.executeQuery();
     			while(rs.next())
     			{
     				JSONObject s=new JSONObject();
     				s.put("screen_id",rs.getInt("screen_id"));
     				s.put("show_id",rs.getInt("show_id"));
     				s.put("show_date",rs.getDate("show_date").toString());
     				s.put("start_time",rs.getTime("start_time").toString());
     				s.put("movie_name",rs.getString("movie_name"));
     				s.put("show_status", rs.getString("show_status"));
     				s.put("language",rs.getString("language"));
     				s.put("collection",rs.getInt("collection"));
     				
     				arr.add(s);
     				
     			}
     		}
     		catch (SQLException ex) 
      	    { 
           
               System.out.println("Error in retriving collection for screen : "+ex);
               
             }
     		return arr;
     	}
     	
     	
     	//TheaterDAO
     	
    	private String door_no;
    	private String street;
    	private String city;
    	private String district;
    	private String state;
    	private int manager_id;
    	private String pin_code;
    	
    	public JSONArray getAllTheaters()
        {
    		JSONArray list=new JSONArray();
       	 try {
                ps = con.prepareStatement("select theater.*,users.email from theater  inner join users on users.user_id=theater.manager_id where \"isAvailable\"=1  order by name");
    			ResultSet rs=ps.executeQuery();
                while(rs.next())
                {
                	theater_id=rs.getInt("theater_id");
                	name=rs.getString("name");
                	manager_id=rs.getInt("manager_id");
                	//System.out.println("DB movie "+name);
//                	door_no=rs.getString("door_no");
//                	street=rs.getString("street");
//                	city=rs.getString("city");
//                	state=rs.getString("state");
//                	pin_code=rs.getString("pin_code");
                	district=rs.getString("district");
                	String manager_email=rs.getString("email");
                	
                	JSONObject t=new JSONObject();
                	t.put("theater_id",theater_id);
                	t.put("theater_name",name);
                	t.put("manager_id",manager_id);
//                	t.put("door_no",door_no);
//                	t.put("street",street);
//                	t.put("city",city);
//                	t.put("state",state);
//                	t.put("pin_code",pin_code);
                	t.put("district",district);
                	t.put("manager_email", manager_email);
                	
               
                	
                	//Theater theater=new Theater(theater_id,name,door_no,street,city,district,state,pin_code,manager_id);
                	list.add(t); 
                }
                        
            } catch (SQLException ex) {
            	System.out.println("Exception in getting theaters "+ex);
            	
            }
         return list;
        }
    	
    	public String addTheater(JSONObject jsonData)
    	{
    		try 
    		{
    			String theater_name=(String)jsonData.get("theater-name");
    			String email=(String)jsonData.get("theater-manager");
    			String district=(String)jsonData.get("theater-district");
    			//System.out.println("District "+district );
    			int user_id=0;
    			ps=con.prepareStatement("insert into users(email,user_role_id,name) values(?,?,?) ON CONFLICT(email,user_role_id) DO UPDATE set \"isActive\" = 1 ");
    			ps.setString(1,email);
    			ps.setInt(2,3);
    			ps.setString(3,theater_name+" - Manager");
    			ps.executeUpdate();
    			
    			ps=con.prepareStatement("select user_id from users where email=? and user_role_id=?");
    			ps.setString(1,email);
    			ps.setInt(2,3);
    			rs=ps.executeQuery();
    			while(rs.next())
    			{
    				user_id=rs.getInt(1);
    			}
    			
    			ps=con.prepareStatement("insert into theater(name,manager_id,district) values(?,?,?)");
    			ps.setString(1,theater_name);
    			ps.setInt(2,user_id);
    			ps.setString(3, district);
    			ps.executeUpdate();
    			
    			return "Theater Added!!";
    			
    		}
    		catch (SQLException e) 
    		{
            	System.out.println("Exception in adding theater"+e);
            	return "Exception in adding theater"+e;
            }
    	}
    	
    	public JSONObject getTheater(int theater_id)
        {
    		JSONObject t=new JSONObject();
       	 try {
                ps = con.prepareStatement("select theater.*,users.email from theater inner join users on theater.manager_id=users.user_id where theater_id=?");
                ps.setInt(1,theater_id);
    			ResultSet rs=ps.executeQuery();
                while(rs.next())
                {
                	theater_id=rs.getInt("theater_id");
                	name=rs.getString("name");
                	manager_id=rs.getInt("manager_id");
                	//System.out.println("DB movie "+name);
//                	door_no=rs.getString("door_no");
//                	street=rs.getString("street");
//                	city=rs.getString("city");
//                	state=rs.getString("state");
//                	pin_code=rs.getString("pin_code");
                	district=rs.getString("district");
                	String manager_email=rs.getString("email");
                	
                	t.put("theater_id",theater_id);
                	t.put("theater_name",name);
                	t.put("manager_id",manager_id);
//                	t.put("door_no",door_no);
//                	t.put("street",street);
//                	t.put("city",city);
                	t.put("district",district);
//                	t.put("state",state);
//                	t.put("pin_code",pin_code);
                	t.put("manager_email", manager_email);
                	
                }
                       
            } catch (SQLException ex) {
            	System.out.println("Exception in getting theater "+ex);
            
            }
       	 return t;
        }
    	
    	public String updateTheater(int theater_id,JSONObject jsonData)
    	{
    		try 
    		{
    			String theater_name=(String)jsonData.get("edit-theater-name");
    			String email=(String)jsonData.get("edit-theater-manager");
    			String district=(String)jsonData.get("edit-theater-district");
    			System.out.println("District "+district );
    			int user_id=0;
    			ps=con.prepareStatement("insert into users(email,user_role_id,name) values(?,?,?) ON CONFLICT(email,user_role_id) DO UPDATE set \"isActive\" = 1 ");
    			ps.setString(1,email);
    			ps.setInt(2,3);
    			ps.setString(3,theater_name+" - Manager");
    			ps.executeUpdate();
    			
    			ps=con.prepareStatement("select user_id from users where email=? and user_role_id=?");
    			ps.setString(1,email);
    			ps.setInt(2,3);
    			rs=ps.executeQuery();
    			while(rs.next())
    			{
    				user_id=rs.getInt(1);
    			}
    			
    			//No need to cancel shows
    			ps=con.prepareStatement("update theater set name=?,manager_id=? where theater_id=?");
    			ps.setString(1,theater_name);
    			ps.setInt(2,user_id);
    			ps.setInt(3,theater_id);
    			ps.executeUpdate();
    			System.out.println("theater name and manager_id got updated");
    			
    			//cancel shows if the city has changed
    			ps=con.prepareStatement("update theater set district=? where theater_id=? and LOWER(district)!=LOWER(?)");
    			ps.setString(1,district);
    			ps.setInt(2,theater_id);
    			ps.setString(3,district);
    			System.out.println(ps);
    			int n=ps.executeUpdate();
    			System.out.println("Number of rows affected : "+n);
    			if(n==1)
    			{
    				ps=con.prepareStatement("select show.* from show inner join screen on screen.screen_id=show.screen_id inner join theater\r\n"
    						+ "on screen.theater_id=theater.theater_id "
    						+ "and theater.theater_id=? and status='Booking opened' where show_date > CURRENT_DATE "
    						+ "OR (show_date = CURRENT_DATE and start_time >= CURRENT_TIME)");
    				ps.setInt(1,theater_id);
    				rs=ps.executeQuery();
    				while(rs.next())
    				{
    					int show_id=rs.getInt("show_id");
    					deleteShow(show_id);
    					System.out.println("Sho_id"+show_id);
    				}
    			}
    			
    			
    			return "Theater Details Updated!!";
    			
    		}
    		catch (SQLException e) 
    		{
            	System.out.println("Exception in adding theater"+e);
            	return "Exception in updating theater"+e;
            }
    	}
    	
    	public String cancelTheater(int theater_id)
    	{
    		try 
    		{
    				ps=con.prepareStatement("select show.* from show inner join screen on screen.screen_id=show.screen_id inner join theater "
    						+ "on screen.theater_id=theater.theater_id "
    						+ "and theater.theater_id=? and status='Booking opened' where show_date > CURRENT_DATE "
    						+ "OR (show_date = CURRENT_DATE and start_time >= CURRENT_TIME)");
    				ps.setInt(1,theater_id);
    				rs=ps.executeQuery();
    				while(rs.next())
    				{
    					int show_id=rs.getInt("show_id");
    					deleteShow(show_id);
    					System.out.println("Sho_id"+show_id);
    				}
    				
    				ps=con.prepareStatement("update theater set \"isAvailable\"=0 where theater_id=?");
    				ps.setInt(1,theater_id);
    				ps.executeUpdate();
    			
    			
    			
    			return "Theater Removed from the Application!!";
    			
    		}
    		catch (SQLException e) 
    		{
            	System.out.println("Exception in adding theater"+e);
            	return "Exception in updating theater"+e;
            }
    	}
    	
    	public JSONArray getTheaterCollection(int theater_id)
    	{
    		JSONArray arr=new JSONArray();
    		try
    		{
    			ps=con.prepareStatement("select screen.screen_id,screen.screen_name,max(theater.theater_id) as theater_id,"
    					+ "(SUM(ticket.seat_prize) - SUM(ticket.refund))as collection "
    					+ "from ticket "
    					+ "inner join booking on booking.booking_id=ticket.booking_id "
    					+ "inner join show on show.show_id=booking.show_id "
    					+ "inner join screen on show.screen_id=screen.screen_id " 
    					+ "inner join theater on screen.theater_id=theater.theater_id and theater.theater_id=? "
    					+ "group by screen.screen_id");
    			ps.setInt(1,theater_id);
    			rs=ps.executeQuery();
    			while(rs.next())
    			{
    				JSONObject s=new JSONObject();
    				s.put("screen_id",rs.getInt("screen_id"));
    				s.put("theater_id",rs.getInt("theater_id"));
    				s.put("screen_name",rs.getString("screen_name"));
    				s.put("collection",rs.getInt("collection"));
    				arr.add(s);
    				
    			}
    		}
    		catch (SQLException ex) 
     	    { 
          
              System.out.println("Error in retriving collection for screen : "+ex);
              
            }
    		return arr;
    	}

     	
    	//OfferDAO
    	
    	public String addOffer(JSONObject data)
        {
        	try
        	{
        		ps=con.prepareStatement("insert into offer(offer_name,no_of_tickets,discount,start_date,end_date) values(?,?,?,?,?)");
        		ps.setString(1,(String)data.get("offer-name"));
        		ps.setInt(2,Integer.parseInt((String)data.get("no-of-tickets")));
        		ps.setInt(3,Integer.parseInt((String)data.get("discount")));
        		ps.setDate(4,java.sql.Date.valueOf(LocalDate.parse((String)data.get("start-date"))));
        		ps.setDate(5,java.sql.Date.valueOf(LocalDate.parse((String)data.get("end-date"))));
        		ps.executeUpdate();
        		
        	}
        	catch(SQLException e)
        	{
        		System.out.println("Exception in adding offer "+e);
        	}
        	return "success";
        }
        
        public String updateOffer(int offer_id,JSONObject data)
        {
        	try
        	{
        		ps=con.prepareStatement("update offer set offer_name=?,no_of_tickets=?,discount=?,start_date=?,end_date=? where offer_id=?");
        		ps.setString(1,(String)data.get("edit-offer-name"));
        		ps.setInt(2,Integer.parseInt((String)data.get("edit-no-of-tickets")));
        		ps.setInt(3,Integer.parseInt((String)data.get("edit-discount")));
        		ps.setDate(4,java.sql.Date.valueOf(LocalDate.parse((String)data.get("edit-start-date"))));
        		ps.setDate(5,java.sql.Date.valueOf(LocalDate.parse((String)data.get("edit-end-date"))));
        		ps.setInt(6, offer_id);
        		ps.executeUpdate();
        		
        		return "Offer updated!!";
        		
        	}
        	catch(SQLException e)
        	{
        		System.out.println("Exception in updating offer "+e);
        		return "Exception in updating offer "+e;
        	}
        	
        }
        
        public String cancelOffer(int offer_id)
        {
        	try
        	{
        		ps=con.prepareStatement("update offer set \"isAvailable\"=0 where offer_id=?");
        		ps.setInt(1, offer_id);
        		ps.executeUpdate();
        		return "Offer cancelled!!";
        	}
        	catch(SQLException e)
        	{
        		System.out.println("Exception in cancelling offer "+e);
        		return "Exception in cancelling offer "+e;
        	}
        }
        
        public JSONArray getOffers()
        {
        	JSONArray arr=new JSONArray();
        	try
        	{
        		ps=con.prepareStatement("select * from offer where \"isAvailable\"=1 order by no_of_tickets,discount desc,start_date,end_date");
        		rs=ps.executeQuery();
        		while(rs.next())
        		{
        			JSONObject s=new JSONObject();
        			s.put("offer_id",rs.getInt("offer_id"));
        			s.put("offer_name",rs.getString("offer_name"));
        			s.put("no_of_tickets",rs.getInt("no_of_tickets"));
        			s.put("discount",rs.getInt("discount"));
        			s.put("start_date",rs.getDate("start_date").toString());
        			s.put("end_date",rs.getDate("end_date").toString());
        			
        			arr.add(s);
        			
        		}
        	}
        	catch(SQLException e)
        	{
        		System.out.println("Exception in retriving offers "+e);
        	}
        	return arr;
        }
        
        public JSONArray getValidOffers()
        {
        	JSONArray arr=new JSONArray();
        	try
        	{
        		ps=con.prepareStatement("select * from offer where start_DATE <= CURRENT_DATE and end_date >= CURRENT_DATE and \"isAvailable\"=1 order by no_of_tickets,discount desc,start_date,end_date ");
        		rs=ps.executeQuery();
        		while(rs.next())
        		{
        			JSONObject s=new JSONObject();
        			s.put("offer_id",rs.getInt("offer_id"));
        			s.put("offer_name",rs.getString("offer_name"));
        			s.put("no_of_tickets",rs.getInt("no_of_tickets"));
        			s.put("discount",rs.getInt("discount"));
        			s.put("start_date",rs.getDate("start_date").toString());
        			s.put("end_date",rs.getDate("end_date").toString());
        			arr.add(s);
        		}
        	}
        	catch(SQLException e)
        	{
        		System.out.println("Exception in retriving offers "+e);
        	}
        	return arr;
        }

}
