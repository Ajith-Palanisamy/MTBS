package com.mtbs;
import java.util.*;
public class User 
{
	private int user_id;
	private String name;
	private String email;
	private String password;
	private String mobileNumber;
	private int user_role_id;
	
	static Scanner sc=new Scanner(System.in);

	
	public User(int user_id,String name,String mobileNumber,String email,String password,int user_role_id)
	{
		this.user_id=user_id;
		this.name=name;
		this.mobileNumber=mobileNumber;
		this.email=email;
		this.password=password;
		this.user_role_id=user_role_id;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getEmail() {
		return email;
	}
	public void setEmail(String email) {
		this.email = email;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getMobileNumber() {
		return mobileNumber;
	}

	public void setMobileNumber(String mobileNumber) 
	{
		this.mobileNumber = mobileNumber;
	}	
	public int getUser_role_id() {
		return user_role_id;
	}

	public void setUser_role_id(int user_role_id) {
		this.user_role_id = user_role_id;
	}
	
	public int getUser_id() {
		return user_id;
	}

	public void setUser_id(int user_id) {
		this.user_id = user_id;
	}
	public User() {}
}
