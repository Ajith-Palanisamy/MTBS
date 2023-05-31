package com.mtbs.db;

import java.sql.Connection;
import java.sql.DriverManager;

public class DBHandler 
{
	private static DBHandler obj;
	private DBHandler(){	}
	private static Connection con=null;
	private static String db="mtbs";
	private static String username="postgres";
	private static String password="root";
	
	
	public static DBHandler getInstance()
	{
		if(obj==null)
			obj=new DBHandler();
		 
		return obj;
	}
	
	public static Connection getConnection()
	{
		try
		{
			Class.forName("org.postgresql.Driver");
			con = DriverManager.getConnection("jdbc:postgresql://localhost:3307/"+db,username,password);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return con;
	}
	
	
}
