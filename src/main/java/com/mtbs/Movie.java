package com.mtbs;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

import jakarta.xml.bind.annotation.XmlRootElement;

@XmlRootElement 
public class Movie 
{
	
	private int movie_id;
	private String name;
	private String certificate;
	private List<String> languages;
	private String director;
	private String description;
	private LocalTime duration;
	private String image;
	private LocalDateTime added_time;
	
	public Movie(int movie_id,String name,String certificate,List<String> languages,String director,String description,LocalTime duration,String image,LocalDateTime added_time)
	{
		this.name=name;
		this.movie_id=movie_id;
		this.certificate=certificate;
		this.languages=languages;
		this.director=director;
		this.description=description;
		this.duration=duration;
		this.image=image;
		this.added_time=added_time;
	}
	public Movie() {}	
	public int getMovie_id() {
		return movie_id;
	}
	public void setMovie_id(int movie_id) {
		this.movie_id = movie_id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getCertificate() {
		return certificate;
	}
	public void setCertificate(String certificate) {
		this.certificate = certificate;
	}
	
	public List<String> getLanguages() {
		return languages;
	}

	public void setLanguages(List<String> languages) {
		this.languages = languages;
	}

	public String getDirector() {
		return director;
	}
	public void setDirector(String director) {
		this.director = director;
	}

	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public LocalTime getDuration() {
		return duration;
	}
	public void setDuration(LocalTime duration) {
		this.duration = duration;
	}
	public String getImage() {
		return image;
	}
	public void setImage(String image) {
		this.image = image;
	}
	public LocalDateTime getAdded_time() {
		return added_time;
	}
	public void setAdded_time(LocalDateTime added_time) {
		this.added_time = added_time;
	}

	@Override
	public String toString() {
		return "Movie [movie_id=" + movie_id + ", name=" + name + ", certificate=" + certificate + ", languages="
				+ languages + ", director=" + director + ", description=" + description + ", duration=" + duration
				+ ", image=" + image + ", added_time=" + added_time + "]";
	}
	
	
}