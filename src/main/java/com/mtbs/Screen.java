package com.mtbs;

public class Screen 
{
	private int screen_id;
	private int theater_id;
	private String screen_name;
	
	public Screen() {}
	
	public int getScreen_id() {
		return screen_id;
	}
	public void setScreen_id(int screen_id) {
		this.screen_id = screen_id;
	}
	public int getTheater_id() {
		return theater_id;
	}
	public void setTheater_id(int theater_id) {
		this.theater_id = theater_id;
	}
	public String getScreen_name() {
		return screen_name;
	}
	public void setScreen_name(String screen_name) {
		this.screen_name = screen_name;
	}
	
	public Screen(int screen_id, int theater_id, String screen_name) {
		
		this.screen_id = screen_id;
		this.theater_id = theater_id;
		this.screen_name = screen_name;
	}
}
