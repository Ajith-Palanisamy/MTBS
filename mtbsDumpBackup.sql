--
-- PostgreSQL database dump
--

-- Dumped from database version 10.22
-- Dumped by pg_dump version 10.22

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: booking; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.booking (
    booking_id integer NOT NULL,
    show_id bigint,
    booked_time timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    user_id bigint,
    status character varying DEFAULT 'Booked'::character varying,
    offer_id bigint,
    vip_cancel integer DEFAULT 25,
    premium_cancel integer DEFAULT 25,
    normal_cancel integer DEFAULT 25
);


ALTER TABLE public.booking OWNER TO postgres;

--
-- Name: language; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.language (
    language_id integer NOT NULL,
    language character varying
);


ALTER TABLE public.language OWNER TO postgres;

--
-- Name: language_language_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.language_language_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.language_language_id_seq OWNER TO postgres;

--
-- Name: language_language_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.language_language_id_seq OWNED BY public.language.language_id;


--
-- Name: movie; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.movie (
    movie_id integer NOT NULL,
    name character varying(200) NOT NULL,
    certificate character varying(3) NOT NULL,
    director character varying(100) NOT NULL,
    description character varying(100),
    duration time without time zone NOT NULL,
    image character varying(200),
    added_time timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "isAvailable" integer DEFAULT 1
);


ALTER TABLE public.movie OWNER TO postgres;

--
-- Name: movie_language_mapping; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.movie_language_mapping (
    movie_language_mapping_id integer NOT NULL,
    movie_id integer,
    language_id integer,
    "isAvailable" integer DEFAULT 1
);


ALTER TABLE public.movie_language_mapping OWNER TO postgres;

--
-- Name: movie_language_mapping_movie_language_mapping_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.movie_language_mapping_movie_language_mapping_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.movie_language_mapping_movie_language_mapping_id_seq OWNER TO postgres;

--
-- Name: movie_language_mapping_movie_language_mapping_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.movie_language_mapping_movie_language_mapping_id_seq OWNED BY public.movie_language_mapping.movie_language_mapping_id;


--
-- Name: movie_movie_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.movie_movie_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.movie_movie_id_seq OWNER TO postgres;

--
-- Name: movie_movie_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.movie_movie_id_seq OWNED BY public.movie.movie_id;


--
-- Name: offer; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.offer (
    offer_id bigint NOT NULL,
    offer_name character varying,
    no_of_tickets integer,
    discount integer,
    start_date date,
    end_date date,
    "isAvailable" integer DEFAULT 1
);


ALTER TABLE public.offer OWNER TO postgres;

--
-- Name: offer_offer_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.offer_offer_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.offer_offer_id_seq OWNER TO postgres;

--
-- Name: offer_offer_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.offer_offer_id_seq OWNED BY public.offer.offer_id;


--
-- Name: screen; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.screen (
    screen_id integer NOT NULL,
    theater_id integer,
    screen_name character varying,
    "isAvailable" integer DEFAULT 1
);


ALTER TABLE public.screen OWNER TO postgres;

--
-- Name: screen_theater_screen_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.screen_theater_screen_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.screen_theater_screen_id_seq OWNER TO postgres;

--
-- Name: screen_theater_screen_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.screen_theater_screen_id_seq OWNED BY public.screen.screen_id;


--
-- Name: seating_arrangement; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.seating_arrangement (
    seating_id integer NOT NULL,
    screen_id integer,
    row_name character varying(2),
    column_number integer,
    seat_type character varying NOT NULL
);


ALTER TABLE public.seating_arrangement OWNER TO postgres;

--
-- Name: seat_seat_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.seat_seat_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.seat_seat_id_seq OWNER TO postgres;

--
-- Name: seat_seat_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.seat_seat_id_seq OWNED BY public.seating_arrangement.seating_id;


--
-- Name: show; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.show (
    show_id integer NOT NULL,
    show_date date,
    start_time time without time zone,
    end_time time without time zone,
    screen_id bigint,
    movie_language_mapping_id bigint,
    vip_prize integer,
    premium_prize integer,
    normal_prize integer,
    status character varying,
    vip_cancel integer,
    premium_cancel integer,
    normal_cancel integer
);


ALTER TABLE public.show OWNER TO postgres;

--
-- Name: show_show_id_seq1; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.show_show_id_seq1
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.show_show_id_seq1 OWNER TO postgres;

--
-- Name: show_show_id_seq1; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.show_show_id_seq1 OWNED BY public.show.show_id;


--
-- Name: theater; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.theater (
    theater_id integer NOT NULL,
    name character varying(200),
    door_no character varying(10),
    street character varying(100),
    district character varying(100),
    city character varying(100),
    state character varying(100),
    pin_code character varying(100),
    manager_id bigint,
    wallet integer DEFAULT 0,
    "isAvailable" integer DEFAULT 1
);


ALTER TABLE public.theater OWNER TO postgres;

--
-- Name: theater_theater_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.theater_theater_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.theater_theater_id_seq OWNER TO postgres;

--
-- Name: theater_theater_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.theater_theater_id_seq OWNED BY public.theater.theater_id;


--
-- Name: ticket; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ticket (
    ticket_id integer NOT NULL,
    booking_id bigint,
    seat_number character varying,
    seat_type character varying,
    seat_prize integer,
    refund integer DEFAULT 0,
    discount integer DEFAULT 0
);


ALTER TABLE public.ticket OWNER TO postgres;

--
-- Name: ticket_ticket_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.ticket_ticket_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.ticket_ticket_id_seq OWNER TO postgres;

--
-- Name: ticket_ticket_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.ticket_ticket_id_seq OWNED BY public.booking.booking_id;


--
-- Name: ticket_ticket_id_seq1; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.ticket_ticket_id_seq1
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.ticket_ticket_id_seq1 OWNER TO postgres;

--
-- Name: ticket_ticket_id_seq1; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.ticket_ticket_id_seq1 OWNED BY public.ticket.ticket_id;


--
-- Name: user_role; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.user_role (
    user_role_id integer NOT NULL,
    role character varying(30)
);


ALTER TABLE public.user_role OWNER TO postgres;

--
-- Name: users; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.users (
    name character varying(100) NOT NULL,
    email character varying(100) NOT NULL,
    password character varying(200),
    mobilenumber character varying(10),
    user_role_id integer,
    user_id integer NOT NULL,
    wallet integer DEFAULT 0,
    "isActive" integer DEFAULT 1
);


ALTER TABLE public.users OWNER TO postgres;

--
-- Name: users_user_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.users_user_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.users_user_id_seq OWNER TO postgres;

--
-- Name: users_user_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.users_user_id_seq OWNED BY public.users.user_id;


--
-- Name: booking booking_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.booking ALTER COLUMN booking_id SET DEFAULT nextval('public.ticket_ticket_id_seq'::regclass);


--
-- Name: movie movie_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.movie ALTER COLUMN movie_id SET DEFAULT nextval('public.movie_movie_id_seq'::regclass);


--
-- Name: movie_language_mapping movie_language_mapping_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.movie_language_mapping ALTER COLUMN movie_language_mapping_id SET DEFAULT nextval('public.movie_language_mapping_movie_language_mapping_id_seq'::regclass);


--
-- Name: offer offer_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.offer ALTER COLUMN offer_id SET DEFAULT nextval('public.offer_offer_id_seq'::regclass);


--
-- Name: screen screen_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.screen ALTER COLUMN screen_id SET DEFAULT nextval('public.screen_theater_screen_id_seq'::regclass);


--
-- Name: seating_arrangement seating_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.seating_arrangement ALTER COLUMN seating_id SET DEFAULT nextval('public.seat_seat_id_seq'::regclass);


--
-- Name: show show_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.show ALTER COLUMN show_id SET DEFAULT nextval('public.show_show_id_seq1'::regclass);


--
-- Name: theater theater_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.theater ALTER COLUMN theater_id SET DEFAULT nextval('public.theater_theater_id_seq'::regclass);


--
-- Name: ticket ticket_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ticket ALTER COLUMN ticket_id SET DEFAULT nextval('public.ticket_ticket_id_seq1'::regclass);


--
-- Name: users user_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.users ALTER COLUMN user_id SET DEFAULT nextval('public.users_user_id_seq'::regclass);


--
-- Data for Name: booking; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.booking (booking_id, show_id, booked_time, user_id, status, offer_id, vip_cancel, premium_cancel, normal_cancel) FROM stdin;
30	51	2023-05-25 15:35:12.690913	2	show cancelled	\N	25	25	25
37	51	2023-05-26 11:44:12.557789	4	show cancelled	\N	25	25	25
40	44	2023-05-26 12:34:56.065956	2	show cancelled	\N	25	25	25
41	44	2023-05-26 13:11:02.986093	2	show cancelled	\N	25	25	25
42	44	2023-05-26 13:21:56.521341	2	show cancelled	\N	25	25	25
43	44	2023-05-26 17:28:17.382907	2	show cancelled	\N	25	25	25
44	44	2023-05-26 17:39:01.086596	2	show cancelled	\N	25	25	25
45	44	2023-05-26 17:39:34.500989	2	show cancelled	1	25	25	25
46	44	2023-05-26 17:42:18.595874	2	show cancelled	5	25	25	25
47	44	2023-05-26 18:03:04.294839	2	show cancelled	1	25	25	25
48	44	2023-05-26 18:05:47.458564	2	show cancelled	\N	25	25	25
49	62	2023-05-26 18:22:43.9542	2	Booked	\N	25	25	25
55	69	2023-05-27 18:29:30.239697	4	Show cancelled	\N	25	25	25
56	71	2023-05-27 18:38:44.187635	4	Show cancelled	\N	25	25	25
57	72	2023-05-27 19:23:09.995078	4	Show cancelled	\N	25	25	25
58	73	2023-05-27 20:01:02.497933	4	Show cancelled	\N	25	25	25
59	73	2023-05-27 20:05:32.248281	4	Show cancelled	\N	25	25	25
60	74	2023-05-27 20:27:04.841028	4	Show cancelled	\N	25	25	25
61	74	2023-05-27 20:27:14.798191	4	Show cancelled	\N	25	25	25
32	59	2023-05-25 18:07:43.929389	2	Booked	\N	25	25	25
63	75	2023-05-27 22:46:18.849965	2	Cancelled	\N	25	25	25
36	48	2023-05-26 01:16:52.681273	2	Cancelled	\N	25	25	25
33	48	2023-05-26 01:06:12.499143	2	Cancelled	\N	25	25	25
62	75	2023-05-27 22:44:53.425212	4	Show cancelled	\N	25	25	25
50	66	2023-05-27 12:57:51.925392	4	Show cancelled	1	25	25	25
35	48	2023-05-26 01:06:43.566891	4	Cancelled	\N	25	25	25
38	48	2023-05-26 11:58:58.52319	2	Cancelled	\N	25	25	25
39	48	2023-05-26 12:19:06.254814	2	Booked	\N	25	25	25
51	66	2023-05-27 13:02:43.321515	4	Show cancelled	\N	25	25	25
52	66	2023-05-27 13:03:36.840913	2	Show cancelled	\N	25	25	25
53	66	2023-05-27 13:05:10.256671	2	Show cancelled	\N	25	25	25
54	66	2023-05-27 13:05:23.870635	2	Show cancelled	\N	25	25	25
65	90	2023-05-28 21:52:00.694229	2	Cancelled	\N	25	25	25
64	90	2023-05-28 21:51:55.498529	2	Show cancelled	\N	25	25	25
66	91	2023-05-28 22:37:48.525969	29	Cancelled	\N	25	25	25
67	91	2023-05-28 22:37:52.806665	29	Show cancelled	\N	25	25	25
68	81	2023-05-28 23:05:45.719622	29	Booked	1	25	25	25
69	81	2023-05-29 10:09:47.580634	4	Booked	\N	25	25	25
70	81	2023-05-29 10:09:56.117434	4	Booked	1	25	25	25
71	81	2023-05-29 10:10:02.328251	4	Cancelled	\N	25	25	25
72	92	2023-05-29 11:47:36.809283	29	Booked	1	25	25	25
74	79	2023-05-29 14:36:52.181794	2	Booked	1	25	25	25
75	79	2023-05-29 14:37:17.580771	2	Booked	5	25	25	25
73	79	2023-05-29 14:36:36.661506	2	Cancelled	\N	25	25	25
76	95	2023-05-29 14:50:14.9099	2	Cancelled	\N	25	25	25
77	101	2023-05-29 18:07:25.60204	4	Booked	5	25	25	25
78	101	2023-05-30 13:27:20.940316	29	Booked	\N	25	25	25
79	158	2023-05-30 14:35:48.344654	29	Cancelled	\N	25	25	25
80	157	2023-05-30 16:05:53.827662	29	Cancelled	\N	10	10	10
81	157	2023-05-30 17:32:07.351025	29	Booked	\N	30	30	30
82	80	2023-05-30 20:37:02.637086	29	Booked	\N	25	25	25
\.


--
-- Data for Name: language; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.language (language_id, language) FROM stdin;
1	Tamil
2	English
3	Hindi
\.


--
-- Data for Name: movie; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.movie (movie_id, name, certificate, director, description, duration, image, added_time, "isAvailable") FROM stdin;
8	Ponniyin Selvan 2	A	ManiRathnam	Historical movie	02:30:00	https://w0.peakpx.com/wallpaper/828/229/HD-wallpaper-ponniyin-selvan-part-1-tamil-movie.jpg	2023-05-11 23:52:25.15788	1
27	Pitchaikaran 2	A	Vijay Antony	entertainment	02:45:00	https://w0.peakpx.com/wallpaper/828/229/HD-wallpaper-ponniyin-selvan-part-1-tamil-movie.jpg	2023-05-23 15:17:45.079032	1
32	AAA	A	Vamsi	Test	03:59:00	https://image.tmdb.org/t/p/original/ncXklBeNi7X7OETqmDVEgaf8ItQ.jpg	2023-05-29 18:13:09.113302	0
28	Varisu	A	Vamsi	family	02:30:00	https://image.tmdb.org/t/p/original/ncXklBeNi7X7OETqmDVEgaf8ItQ.jpg	2023-05-26 01:05:24.40004	1
3	RRR	A	RajaMouli	Love movie	04:21:00	https://image.tmdb.org/t/p/original/ncXklBeNi7X7OETqmDVEgaf8ItQ.jpg	2023-05-11 14:29:32.370108	1
\.


--
-- Data for Name: movie_language_mapping; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.movie_language_mapping (movie_language_mapping_id, movie_id, language_id, "isAvailable") FROM stdin;
66	8	1	1
67	8	2	1
68	8	3	1
69	27	1	1
107	27	3	1
71	28	3	1
70	28	1	0
72	28	2	0
6	3	1	1
7	3	2	1
8	3	3	1
133	32	1	1
136	32	3	1
\.


--
-- Data for Name: offer; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.offer (offer_id, offer_name, no_of_tickets, discount, start_date, end_date, "isAvailable") FROM stdin;
2	Winter offer	7	15	2023-05-27	2023-05-31	1
5	Mega offer	8	25	2023-05-26	2023-06-02	1
1	Summer offer	5	20	2023-05-26	2023-05-30	1
3	Festival offer	3	5	2023-05-31	2023-06-04	1
\.


--
-- Data for Name: screen; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.screen (screen_id, theater_id, screen_name, "isAvailable") FROM stdin;
1	1	Screen 1	1
2	1	Screen 2	1
3	2	Screen 1	1
4	12	Screen 1	1
5	9	Screen 1	1
6	9	screen 2	0
7	9	Screen 2	1
\.


--
-- Data for Name: seating_arrangement; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.seating_arrangement (seating_id, screen_id, row_name, column_number, seat_type) FROM stdin;
4690	3	A	1	VIP
4691	3	A	2	VIP
4692	3	A	3	VIP
4693	3	A	4	VIP
4384	1	A	1	VIP
4385	1	A	2	VIP
4386	1	A	3	VIP
4387	1	A	4	VIP
4388	1	A	5	VIP
4389	1	A	6	VIP
4390	1	A	7	VIP
4391	1	A	8	VIP
4392	1	A	9	VIP
4393	1	A	10	VIP
4394	1	A	11	VIP
4395	1	A	12	VIP
4396	1	A	13	VIP
4397	1	A	14	VIP
4398	1	A	15	VIP
4399	1	B	1	Premium
4400	1	B	2	Premium
4401	1	B	3	Premium
4402	1	B	4	P
4403	1	B	5	P
4404	1	B	6	P
4405	1	B	7	P
4406	1	B	8	P
4407	1	B	9	P
4408	1	B	10	P
4409	1	B	11	P
4410	1	B	12	Premium
4411	1	B	13	Premium
4412	1	B	14	Premium
4413	1	B	15	Premium
4414	1	C	1	Premium
4415	1	C	2	Premium
4416	1	C	3	Premium
4417	1	C	4	P
4418	1	C	5	Premium
4419	1	C	6	Premium
4420	1	C	7	Premium
4421	1	C	8	Premium
4422	1	C	9	Premium
4423	1	C	10	Premium
4424	1	C	11	P
4425	1	C	12	Premium
4426	1	C	13	Premium
4427	1	C	14	Premium
4428	1	C	15	Premium
4429	1	D	1	Premium
4430	1	D	2	Premium
4431	1	D	3	Premium
4432	1	D	4	P
4433	1	D	5	Premium
4434	1	D	6	Premium
4435	1	D	7	Premium
4436	1	D	8	Premium
4437	1	D	9	Premium
4438	1	D	10	Premium
4439	1	D	11	P
4440	1	D	12	Premium
4441	1	D	13	Premium
4442	1	D	14	Premium
4443	1	D	15	Premium
4444	1	E	1	Premium
4445	1	E	2	Premium
4446	1	E	3	Premium
4447	1	E	4	P
4448	1	E	5	Premium
4449	1	E	6	Premium
4450	1	E	7	Premium
4451	1	E	8	Premium
4452	1	E	9	Premium
4453	1	E	10	Premium
4454	1	E	11	P
4455	1	E	12	Premium
4456	1	E	13	Premium
4457	1	E	14	Premium
4458	1	E	15	Premium
4459	1	F	1	Normal
4460	1	F	2	Normal
4461	1	F	3	Normal
4462	1	F	4	P
4463	1	F	5	Normal
4464	1	F	6	Normal
4465	1	F	7	Normal
4466	1	F	8	Normal
4467	1	F	9	Normal
4468	1	F	10	Normal
4469	1	F	11	P
4470	1	F	12	Normal
4471	1	F	13	Normal
4472	1	F	14	Normal
4473	1	F	15	Normal
4474	1	G	1	Normal
4475	1	G	2	Normal
4476	1	G	3	Normal
4477	1	G	4	P
4478	1	G	5	Normal
4479	1	G	6	Normal
4480	1	G	7	Normal
4481	1	G	8	Normal
4482	1	G	9	Normal
4483	1	G	10	Normal
4484	1	G	11	P
4485	1	G	12	Normal
4486	1	G	13	Normal
4487	1	G	14	Normal
4488	1	G	15	Normal
4489	1	H	1	P
4490	1	H	2	P
4491	1	H	3	P
4492	1	H	4	P
4493	1	H	5	Normal
4494	1	H	6	Normal
4495	1	H	7	Normal
4496	1	H	8	Normal
4497	1	H	9	Normal
4498	1	H	10	Normal
4499	1	H	11	P
4500	1	H	12	P
4501	1	H	13	P
4502	1	H	14	P
4503	1	H	15	P
4504	1	I	1	Normal
4505	1	I	2	Normal
4506	1	I	3	Normal
4507	1	I	4	Normal
4508	1	I	5	Normal
4509	1	I	6	Normal
4510	1	I	7	Normal
4511	1	I	8	Normal
4512	1	I	9	Normal
4513	1	I	10	Normal
4514	1	I	11	Normal
4515	1	I	12	Normal
4516	1	I	13	Normal
4517	1	I	14	Normal
4518	1	I	15	Normal
4519	1	J	1	Normal
4520	1	J	2	Normal
4521	1	J	3	Normal
4522	1	J	4	Normal
4523	1	J	5	Normal
4524	1	J	6	Normal
4525	1	J	7	Normal
4526	1	J	8	Normal
4527	1	J	9	Normal
4528	1	J	10	Normal
4529	1	J	11	Normal
4530	1	J	12	Normal
4531	1	J	13	Normal
4532	1	J	14	Normal
4533	1	J	15	Normal
4694	3	A	5	VIP
4695	3	B	1	VIP
4696	3	B	2	VIP
4697	3	B	3	P
4698	3	B	4	VIP
4699	3	B	5	VIP
4700	3	C	1	Premium
4701	3	C	2	Premium
4702	3	C	3	P
4703	3	C	4	Premium
4704	3	C	5	Premium
4705	3	D	1	P
4706	3	D	2	P
4707	3	D	3	P
4708	3	D	4	P
4709	3	D	5	P
4710	3	E	1	Normal
4711	3	E	2	Normal
4712	3	E	3	Normal
4713	3	E	4	Normal
4714	3	E	5	Normal
4723	4	A	9	VIP
4724	4	A	10	VIP
4725	4	A	11	VIP
4726	4	A	12	P
4727	4	A	13	VIP
4728	4	A	14	VIP
4729	4	A	15	VIP
4730	4	B	1	VIP
4731	4	B	2	VIP
4732	4	B	3	VIP
4733	4	B	4	P
4734	4	B	5	VIP
4735	4	B	6	VIP
4736	4	B	7	VIP
4737	4	B	8	VIP
4738	4	B	9	VIP
4739	4	B	10	VIP
4740	4	B	11	VIP
4741	4	B	12	P
4742	4	B	13	VIP
4743	4	B	14	VIP
4744	4	B	15	VIP
4745	4	C	1	P
4746	4	C	2	P
4747	4	C	3	P
4748	4	C	4	P
4749	4	C	5	Premium
4750	4	C	6	Premium
4751	4	C	7	Premium
4752	4	C	8	Premium
4753	4	C	9	Premium
4754	4	C	10	Premium
4755	4	C	11	Premium
4756	4	C	12	P
4757	4	C	13	P
4758	4	C	14	P
4759	4	C	15	P
4760	4	D	1	Premium
4761	4	D	2	Premium
4762	4	D	3	Premium
4763	4	D	4	P
4764	4	D	5	Premium
4765	4	D	6	Premium
4766	4	D	7	Premium
4767	4	D	8	Premium
4768	4	D	9	Premium
4769	4	D	10	Premium
4770	4	D	11	Premium
4771	4	D	12	P
4772	4	D	13	Premium
4773	4	D	14	Premium
4774	4	D	15	Premium
4775	4	E	1	Premium
4776	4	E	2	Premium
4777	4	E	3	Premium
4778	4	E	4	P
4779	4	E	5	Premium
4780	4	E	6	Premium
4781	4	E	7	Premium
4782	4	E	8	Premium
4783	4	E	9	Premium
4784	4	E	10	Premium
4785	4	E	11	Premium
4786	4	E	12	P
4787	4	E	13	Premium
4788	4	E	14	Premium
4789	4	E	15	Premium
4790	4	F	1	Normal
4791	4	F	2	Normal
4792	4	F	3	Normal
4793	4	F	4	P
4794	4	F	5	Normal
4795	4	F	6	Normal
4796	4	F	7	Normal
4797	4	F	8	Normal
4798	4	F	9	Normal
4799	4	F	10	Normal
4800	4	F	11	Normal
4801	4	F	12	P
4802	4	F	13	Normal
4803	4	F	14	Normal
4804	4	F	15	Normal
4805	4	G	1	Normal
4806	4	G	2	Normal
4807	4	G	3	Normal
4808	4	G	4	P
4809	4	G	5	Normal
4810	4	G	6	Normal
4811	4	G	7	Normal
4812	4	G	8	Normal
4813	4	G	9	Normal
4814	4	G	10	Normal
4815	4	G	11	Normal
4816	4	G	12	P
4817	4	G	13	Normal
4818	4	G	14	Normal
4819	4	G	15	Normal
4820	4	H	1	Normal
4821	4	H	2	Normal
4822	4	H	3	Normal
4823	4	H	4	P
4824	4	H	5	Normal
4825	4	H	6	Normal
4826	4	H	7	Normal
4827	4	H	8	Normal
4828	4	H	9	Normal
4829	4	H	10	Normal
4830	4	H	11	Normal
4831	4	H	12	P
4832	4	H	13	Normal
4833	4	H	14	Normal
4834	4	H	15	Normal
4835	4	I	1	Normal
4836	4	I	2	Normal
4837	4	I	3	Normal
4838	4	I	4	P
4839	4	I	5	Normal
4840	4	I	6	Normal
4841	4	I	7	Normal
4842	4	I	8	Normal
4843	4	I	9	Normal
4844	4	I	10	Normal
4845	4	I	11	Normal
4846	4	I	12	P
4847	4	I	13	Normal
4848	4	I	14	Normal
4849	4	I	15	Normal
4850	4	J	1	Normal
4851	4	J	2	Normal
4852	4	J	3	Normal
4853	4	J	4	P
4534	2	A	1	VIP
4535	2	A	2	VIP
4536	2	A	3	VIP
4537	2	A	4	VIP
4538	2	A	5	VIP
4539	2	A	6	VIP
4540	2	A	7	VIP
4541	2	A	8	VIP
4542	2	A	9	VIP
4543	2	A	10	VIP
4544	2	A	11	VIP
4545	2	A	12	VIP
4546	2	A	13	VIP
4547	2	B	1	VIP
4548	2	B	2	VIP
4549	2	B	3	VIP
4550	2	B	4	VIP
4551	2	B	5	P
4552	2	B	6	VIP
4553	2	B	7	VIP
4554	2	B	8	VIP
4555	2	B	9	P
4556	2	B	10	VIP
4557	2	B	11	VIP
4558	2	B	12	VIP
4559	2	B	13	VIP
4560	2	C	1	VIP
4561	2	C	2	VIP
4562	2	C	3	VIP
4563	2	C	4	VIP
4564	2	C	5	P
4565	2	C	6	VIP
4566	2	C	7	VIP
4567	2	C	8	VIP
4568	2	C	9	P
4569	2	C	10	VIP
4570	2	C	11	VIP
4571	2	C	12	VIP
4572	2	C	13	VIP
4573	2	D	1	Premium
4574	2	D	2	Premium
4575	2	D	3	Premium
4576	2	D	4	Premium
4577	2	D	5	P
4578	2	D	6	Premium
4579	2	D	7	Premium
4580	2	D	8	Premium
4581	2	D	9	P
4582	2	D	10	Premium
4583	2	D	11	Premium
4584	2	D	12	Premium
4585	2	D	13	Premium
4586	2	E	1	Premium
4587	2	E	2	Premium
4588	2	E	3	Premium
4589	2	E	4	Premium
4590	2	E	5	P
4591	2	E	6	Premium
4592	2	E	7	Premium
4593	2	E	8	Premium
4594	2	E	9	P
4595	2	E	10	Premium
4596	2	E	11	Premium
4597	2	E	12	Premium
4598	2	E	13	Premium
4599	2	F	1	Premium
4600	2	F	2	Premium
4601	2	F	3	Premium
4602	2	F	4	Premium
4603	2	F	5	P
4604	2	F	6	Premium
4605	2	F	7	Premium
4606	2	F	8	Premium
4607	2	F	9	P
4608	2	F	10	Premium
4609	2	F	11	Premium
4610	2	F	12	Premium
4611	2	F	13	Premium
4612	2	G	1	Premium
4613	2	G	2	Premium
4614	2	G	3	Premium
4615	2	G	4	Premium
4616	2	G	5	P
4617	2	G	6	Premium
4618	2	G	7	Premium
4619	2	G	8	Premium
4620	2	G	9	P
4621	2	G	10	Premium
4622	2	G	11	Premium
4623	2	G	12	Premium
4624	2	G	13	Premium
4625	2	H	1	Normal
4626	2	H	2	Normal
4627	2	H	3	Normal
4628	2	H	4	Normal
4629	2	H	5	P
4630	2	H	6	Normal
4631	2	H	7	Normal
4632	2	H	8	Normal
4633	2	H	9	P
4634	2	H	10	Normal
4635	2	H	11	Normal
4636	2	H	12	Normal
4637	2	H	13	Normal
4638	2	I	1	Normal
4639	2	I	2	Normal
4640	2	I	3	Normal
4641	2	I	4	Normal
4642	2	I	5	P
4643	2	I	6	Normal
4644	2	I	7	Normal
4645	2	I	8	Normal
4646	2	I	9	P
4647	2	I	10	Normal
4648	2	I	11	Normal
4649	2	I	12	Normal
4650	2	I	13	Normal
4651	2	J	1	P
4652	2	J	2	P
4653	2	J	3	P
4654	2	J	4	P
4655	2	J	5	P
4656	2	J	6	Normal
4657	2	J	7	Normal
4658	2	J	8	Normal
4659	2	J	9	P
4660	2	J	10	P
4661	2	J	11	P
4662	2	J	12	P
4663	2	J	13	P
4664	2	K	1	Normal
4665	2	K	2	Normal
4666	2	K	3	Normal
4667	2	K	4	Normal
4668	2	K	5	Normal
4669	2	K	6	Normal
4670	2	K	7	Normal
4671	2	K	8	Normal
4672	2	K	9	Normal
4673	2	K	10	Normal
4674	2	K	11	Normal
4675	2	K	12	Normal
4676	2	K	13	Normal
4677	2	L	1	Normal
4678	2	L	2	Normal
4679	2	L	3	Normal
4680	2	L	4	Normal
4681	2	L	5	Normal
4682	2	L	6	Normal
4683	2	L	7	Normal
4684	2	L	8	Normal
4685	2	L	9	Normal
4686	2	L	10	Normal
4687	2	L	11	Normal
4688	2	L	12	Normal
4689	2	L	13	Normal
4715	4	A	1	VIP
4716	4	A	2	VIP
4717	4	A	3	VIP
4718	4	A	4	P
4719	4	A	5	VIP
4720	4	A	6	VIP
4721	4	A	7	VIP
4722	4	A	8	VIP
4854	4	J	5	Normal
4855	4	J	6	Normal
4856	4	J	7	Normal
4857	4	J	8	Normal
4858	4	J	9	Normal
4859	4	J	10	Normal
4860	4	J	11	Normal
4861	4	J	12	P
4862	4	J	13	Normal
4863	4	J	14	Normal
4864	4	J	15	Normal
4869	5	A	1	VIP
4870	5	A	2	P
4871	5	A	3	VIP
4872	5	A	4	VIP
4873	5	B	1	Premium
4874	5	B	2	P
4875	5	B	3	Premium
4876	5	B	4	Premium
4877	5	C	1	Normal
4878	5	C	2	P
4879	5	C	3	P
4880	5	C	4	P
4881	5	D	1	Normal
4882	5	D	2	Normal
4883	5	D	3	Normal
4884	5	D	4	Normal
4921	7	A	1	VIP
4922	7	A	2	VIP
4923	7	A	3	VIP
4924	7	A	4	VIP
4925	7	A	5	VIP
4926	7	A	6	VIP
4927	7	B	1	VIP
4928	7	B	2	VIP
4929	7	B	3	P
4930	7	B	4	VIP
4931	7	B	5	VIP
4932	7	B	6	VIP
4933	7	C	1	Premium
4934	7	C	2	P
4935	7	C	3	P
4936	7	C	4	P
4937	7	C	5	Premium
4938	7	C	6	Premium
4939	7	D	1	Premium
4940	7	D	2	Premium
4941	7	D	3	P
4942	7	D	4	Premium
4943	7	D	5	Premium
4944	7	D	6	Premium
4945	7	E	1	P
4946	7	E	2	P
4947	7	E	3	P
4948	7	E	4	P
4949	7	E	5	P
4950	7	E	6	P
4951	7	F	1	Normal
4952	7	F	2	Normal
4953	7	F	3	Normal
4954	7	F	4	Normal
4955	7	F	5	Normal
4956	7	F	6	Normal
\.


--
-- Data for Name: show; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.show (show_id, show_date, start_time, end_time, screen_id, movie_language_mapping_id, vip_prize, premium_prize, normal_prize, status, vip_cancel, premium_cancel, normal_cancel) FROM stdin;
69	2023-05-28	09:00:00	12:00:00	1	71	100	100	100	Cancelled	25	25	25
70	2023-05-28	21:40:00	00:40:00	1	71	100	100	100	Cancelled	25	25	25
71	2023-05-28	09:00:00	12:00:00	2	70	100	100	100	Cancelled	25	25	25
72	2023-05-27	23:54:00	02:54:00	2	70	100	100	100	Cancelled	25	25	25
73	2023-05-27	23:59:00	02:29:00	2	70	100	100	100	Cancelled	25	25	25
74	2023-05-27	23:00:00	02:00:00	2	71	100	100	100	Cancelled	25	25	25
75	2023-05-28	06:00:00	08:30:00	2	71	100	100	100	Cancelled	25	25	25
91	2023-05-29	02:42:00	06:19:00	3	107	999	999	999	Cancelled	25	25	25
76	2023-05-28	03:03:00	05:33:00	2	6	500	400	300	Cancelled	25	25	25
77	2023-05-28	02:02:00	05:05:00	2	6	222	333	444	Cancelled	25	25	25
92	2023-05-29	12:45:00	16:45:00	4	8	200	150	100	Booking opened	25	25	25
78	2023-05-27	23:59:00	02:59:00	1	6	100	100	100	Cancelled	25	25	25
57	2023-05-26	16:20:00	19:20:00	1	67	200	200	200	Cancelled	25	25	25
63	2023-05-28	18:20:00	21:42:00	1	67	106	109	100	Cancelled	30	25	25
42	2023-05-26	09:00:00	12:00:00	1	66	440	380	310	Booking opened	25	5	5
66	2023-05-28	21:52:00	01:14:00	1	67	106	109	100	Cancelled	25	25	25
48	2023-05-26	12:10:00	15:10:00	1	66	350	250	150	Booking opened	25	15	5
79	2023-05-30	09:00:00	12:00:00	2	67	100	100	100	Booking opened	25	25	25
59	2023-05-25	18:10:00	20:40:00	1	8	200	150	100	Booking opened	20	20	20
80	2023-05-31	09:00:00	12:00:00	2	67	100	100	100	Booking opened	25	25	25
81	2023-06-01	09:00:00	12:00:00	2	67	100	100	100	Booking opened	25	25	25
56	2023-05-27	10:00:00	13:00:00	1	67	200	200	200	cancelled	25	25	25
58	2023-05-27	16:20:00	19:20:00	1	67	200	200	200	cancelled	25	25	25
51	2023-05-28	09:00:00	12:00:00	1	69	1000	500	200	cancelled	27	39	41
44	2023-05-26	21:40:00	00:40:00	1	66	350	250	150	cancelled	25	15	5
62	2023-05-27	18:20:00	21:42:00	1	67	106	109	100	Booking opened	25	25	25
64	2023-05-26	21:52:00	01:14:00	1	67	106	109	100	Booking opened	25	25	25
65	2023-05-27	21:52:00	01:14:00	1	67	106	109	100	Booking opened	25	25	25
61	2023-05-26	18:20:00	21:42:00	1	67	106	109	100	Cancelled	25	25	25
60	2023-05-27	09:00:00	12:00:00	1	66	200	150	100	Cancelled	25	25	25
67	2023-05-28	09:00:00	12:00:00	1	72	100	100	100	Cancelled	25	25	25
68	2023-05-28	21:40:00	00:40:00	1	72	100	100	100	Cancelled	25	25	25
82	2023-05-29	13:00:00	15:46:00	3	67	100	100	100	Cancelled	25	25	25
83	2023-05-30	13:00:00	15:46:00	3	67	100	100	100	Cancelled	25	25	25
84	2023-05-31	13:00:00	15:46:00	3	67	100	100	100	Cancelled	25	25	25
85	2023-06-01	13:00:00	15:46:00	3	67	100	100	100	Cancelled	25	25	25
86	2023-06-02	13:00:00	15:46:00	3	67	100	100	100	Cancelled	25	25	25
87	2023-06-03	13:00:00	15:46:00	3	67	100	100	100	Cancelled	25	25	25
88	2023-06-04	13:00:00	15:46:00	3	67	100	100	100	Cancelled	25	25	25
89	2023-05-29	10:00:00	13:00:00	3	6	123	123	122	Cancelled	25	25	25
90	2023-05-29	01:00:00	04:00:00	3	6	111	111	111	Cancelled	25	25	25
95	2023-05-29	16:55:00	20:55:00	4	8	200	150	100	Booking opened	25	25	25
98	2023-05-29	21:05:00	01:05:00	4	8	200	150	100	Booking opened	25	25	25
101	2023-05-30	14:10:00	17:10:00	2	69	400	300	200	Booking opened	25	25	25
103	2023-06-01	08:15:00	10:50:00	1	133	233	133	432	Cancelled	25	25	25
102	2023-05-31	14:35:00	17:35:00	5	67	100	100	100	Cancelled	25	25	25
93	2023-05-30	12:45:00	16:45:00	4	8	200	150	100	Cancelled	25	25	25
94	2023-05-31	12:45:00	16:45:00	4	8	200	150	100	Cancelled	25	25	25
96	2023-05-30	16:55:00	20:55:00	4	8	200	150	100	Cancelled	25	25	25
97	2023-05-31	16:55:00	20:55:00	4	8	200	150	100	Cancelled	25	25	25
99	2023-05-30	21:05:00	01:05:00	4	8	200	150	100	Cancelled	25	25	25
100	2023-05-31	21:05:00	01:05:00	4	8	200	150	100	Cancelled	25	25	25
104	2023-06-02	08:15:00	10:50:00	1	133	233	133	432	Cancelled	25	25	25
105	2023-06-03	08:15:00	10:50:00	1	133	233	133	432	Cancelled	25	25	25
106	2023-06-04	08:15:00	10:50:00	1	133	233	133	432	Cancelled	25	25	25
107	2023-06-05	08:15:00	10:50:00	1	133	233	133	432	Cancelled	25	25	25
108	2023-06-06	08:15:00	10:50:00	1	133	233	133	432	Cancelled	25	25	25
109	2023-06-07	08:15:00	10:50:00	1	133	233	133	432	Cancelled	25	25	25
110	2023-06-08	08:15:00	10:50:00	1	133	233	133	432	Cancelled	25	25	25
111	2023-06-09	08:15:00	10:50:00	1	133	233	133	432	Cancelled	25	25	25
112	2023-06-01	11:00:00	13:35:00	1	133	233	133	432	Cancelled	25	25	25
113	2023-06-02	11:00:00	13:35:00	1	133	233	133	432	Cancelled	25	25	25
114	2023-06-03	11:00:00	13:35:00	1	133	233	133	432	Cancelled	25	25	25
115	2023-06-04	11:00:00	13:35:00	1	133	233	133	432	Cancelled	25	25	25
116	2023-06-05	11:00:00	13:35:00	1	133	233	133	432	Cancelled	25	25	25
117	2023-06-06	11:00:00	13:35:00	1	133	233	133	432	Cancelled	25	25	25
118	2023-06-07	11:00:00	13:35:00	1	133	233	133	432	Cancelled	25	25	25
119	2023-06-08	11:00:00	13:35:00	1	133	233	133	432	Cancelled	25	25	25
120	2023-06-09	11:00:00	13:35:00	1	133	233	133	432	Cancelled	25	25	25
121	2023-06-01	13:45:00	16:20:00	1	133	233	133	432	Cancelled	25	25	25
122	2023-06-02	13:45:00	16:20:00	1	133	233	133	432	Cancelled	25	25	25
123	2023-06-03	13:45:00	16:20:00	1	133	233	133	432	Cancelled	25	25	25
124	2023-06-04	13:45:00	16:20:00	1	133	233	133	432	Cancelled	25	25	25
125	2023-06-05	13:45:00	16:20:00	1	133	233	133	432	Cancelled	25	25	25
126	2023-06-06	13:45:00	16:20:00	1	133	233	133	432	Cancelled	25	25	25
127	2023-06-07	13:45:00	16:20:00	1	133	233	133	432	Cancelled	25	25	25
128	2023-06-08	13:45:00	16:20:00	1	133	233	133	432	Cancelled	25	25	25
129	2023-06-09	13:45:00	16:20:00	1	133	233	133	432	Cancelled	25	25	25
130	2023-06-01	16:30:00	19:05:00	1	133	233	133	432	Cancelled	25	25	25
131	2023-06-02	16:30:00	19:05:00	1	133	233	133	432	Cancelled	25	25	25
132	2023-06-03	16:30:00	19:05:00	1	133	233	133	432	Cancelled	25	25	25
133	2023-06-04	16:30:00	19:05:00	1	133	233	133	432	Cancelled	25	25	25
134	2023-06-05	16:30:00	19:05:00	1	133	233	133	432	Cancelled	25	25	25
135	2023-06-06	16:30:00	19:05:00	1	133	233	133	432	Cancelled	25	25	25
136	2023-06-07	16:30:00	19:05:00	1	133	233	133	432	Cancelled	25	25	25
137	2023-06-08	16:30:00	19:05:00	1	133	233	133	432	Cancelled	25	25	25
138	2023-06-09	16:30:00	19:05:00	1	133	233	133	432	Cancelled	25	25	25
139	2023-06-01	19:15:00	21:50:00	1	133	233	133	432	Cancelled	25	25	25
140	2023-06-02	19:15:00	21:50:00	1	133	233	133	432	Cancelled	25	25	25
141	2023-06-03	19:15:00	21:50:00	1	133	233	133	432	Cancelled	25	25	25
142	2023-06-04	19:15:00	21:50:00	1	133	233	133	432	Cancelled	25	25	25
143	2023-06-05	19:15:00	21:50:00	1	133	233	133	432	Cancelled	25	25	25
144	2023-06-06	19:15:00	21:50:00	1	133	233	133	432	Cancelled	25	25	25
145	2023-06-07	19:15:00	21:50:00	1	133	233	133	432	Cancelled	25	25	25
146	2023-06-08	19:15:00	21:50:00	1	133	233	133	432	Cancelled	25	25	25
147	2023-06-09	19:15:00	21:50:00	1	133	233	133	432	Cancelled	25	25	25
148	2023-06-01	22:00:00	00:35:00	1	133	233	133	432	Cancelled	25	25	25
149	2023-06-02	22:00:00	00:35:00	1	133	233	133	432	Cancelled	25	25	25
150	2023-06-03	22:00:00	00:35:00	1	133	233	133	432	Cancelled	25	25	25
151	2023-06-04	22:00:00	00:35:00	1	133	233	133	432	Cancelled	25	25	25
152	2023-06-05	22:00:00	00:35:00	1	133	233	133	432	Cancelled	25	25	25
153	2023-06-06	22:00:00	00:35:00	1	133	233	133	432	Cancelled	25	25	25
154	2023-06-07	22:00:00	00:35:00	1	133	233	133	432	Cancelled	25	25	25
155	2023-06-08	22:00:00	00:35:00	1	133	233	133	432	Cancelled	25	25	25
156	2023-06-09	22:00:00	00:35:00	1	133	233	133	432	Cancelled	25	25	25
158	2023-06-01	08:30:00	11:30:00	7	71	300	200	100	Booking opened	25	25	25
159	2023-06-02	08:30:00	11:30:00	7	71	300	200	100	Booking opened	25	25	25
157	2023-05-31	08:30:00	11:30:00	7	71	300	200	100	Booking opened	30	30	30
\.


--
-- Data for Name: theater; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.theater (theater_id, name, door_no, street, district, city, state, pin_code, manager_id, wallet, "isAvailable") FROM stdin;
12	Sri Murugan Theater	\N	\N	Thirupur	\N	\N	\N	3	900	1
9	Karpagam Cinimas	\N	\N	Erode	\N	\N	\N	6	610	1
1	KG Cinimas	57/1	Anna street	Coimbatore	Racecourse	Tamilnadu	641659	3	7537	1
14	PVR	\N	\N	Selam	\N	\N	\N	28	0	1
2	Miraj Cinimas	7	-	Coimbatore	Ondiputhur	Tamilnadu	641234	3	278	1
\.


--
-- Data for Name: ticket; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.ticket (ticket_id, booking_id, seat_number, seat_type, seat_prize, refund, discount) FROM stdin;
99	42	A1	VIP	350	262	0
100	42	B1	Premium	250	212	0
101	42	D1	Normal	150	142	0
102	43	A1	VIP	350	0	0
103	44	B1	Premium	250	0	0
104	45	I1	Normal	135	0	0
105	45	I2	Normal	135	0	0
106	45	I3	Normal	135	0	0
107	45	I4	Normal	135	0	0
108	45	I5	Normal	135	0	0
109	46	A7	VIP	262	0	0
76	36	A1	VIP	350	262	0
77	36	B1	Premium	250	212	0
78	36	C1	Premium	250	212	0
79	36	D1	Normal	150	142	0
110	46	A8	VIP	262	0	0
111	46	A9	VIP	262	0	0
112	46	A10	VIP	262	0	0
113	46	A11	VIP	262	0	0
63	32	A14	VIP	200	0	0
64	32	A15	VIP	200	0	0
114	46	A12	VIP	262	0	0
115	46	A13	VIP	262	0	0
116	46	A14	VIP	262	0	0
117	46	A15	VIP	262	0	0
118	47	D5	Normal	135	0	0
119	47	D6	Normal	135	0	0
120	47	D7	Normal	135	0	0
65	33	A15	VIP	350	262	0
66	33	B15	Premium	250	212	0
67	33	C15	Premium	250	212	0
68	33	D14	Normal	150	142	0
69	33	D15	Normal	150	142	0
56	30	A15	VIP	1000	730	0
57	30	B15	Premium	500	305	0
58	30	C15	Premium	500	305	0
59	30	D15	Normal	200	118	0
80	37	A1	VIP	1000	730	0
81	37	B1	Premium	500	305	0
82	37	C1	Premium	500	305	0
83	37	D1	Normal	200	118	0
84	37	E1	Normal	200	118	0
74	35	I1	Normal	150	142	0
75	35	I2	Normal	150	142	0
85	38	A1	VIP	350	262	0
86	38	B1	Premium	250	212	0
87	38	D1	Normal	150	142	0
88	39	A1	VIP	350	0	0
89	39	B1	Premium	250	0	0
90	39	D1	Normal	150	0	0
91	40	A1	VIP	350	262	0
92	40	B1	Premium	250	212	0
93	40	D1	Normal	150	142	0
94	41	A1	VIP	350	262	0
95	41	B1	Premium	250	212	0
96	41	C1	Premium	250	212	0
97	41	D1	Normal	150	142	0
98	41	E1	Normal	150	142	0
121	47	D8	Normal	135	0	0
122	47	D9	Normal	135	0	0
123	47	D10	Normal	135	0	0
124	48	E9	Normal	150	0	0
125	49	A1	VIP	106	0	0
126	49	A2	VIP	106	0	0
127	49	A3	VIP	106	0	0
128	49	A4	VIP	106	0	0
139	55	A1	VIP	100	100	0
140	55	A2	VIP	100	100	0
141	55	A3	VIP	100	100	0
142	55	A4	VIP	100	100	0
143	56	A1	VIP	100	100	0
144	56	A2	VIP	100	100	0
145	57	A1	VIP	100	100	0
146	57	A2	VIP	100	100	0
138	54	F15	Normal	100	100	0
147	58	A1	VIP	100	100	0
148	59	A1	VIP	100	100	0
149	60	A1	VIP	100	0	0
150	61	A13	VIP	100	75	0
152	63	A13	VIP	100	75	0
151	62	A1	VIP	100	100	0
136	53	A14	VIP	106	106	0
137	53	A15	VIP	106	106	0
134	51	A1	VIP	106	106	0
135	51	A2	VIP	106	106	0
129	50	G1	Normal	90	90	0
130	50	G2	Normal	90	90	0
131	50	G3	Normal	90	90	0
132	50	G5	Normal	90	90	0
133	50	G6	Normal	90	90	0
154	65	C5	Premium	111	83	0
153	64	C1	Premium	111	111	0
155	66	A3	VIP	999	749	0
156	67	C1	Premium	999	999	0
157	68	A1	VIP	90	0	10
158	68	A2	VIP	90	0	10
159	68	A3	VIP	90	0	10
160	68	A4	VIP	90	0	10
161	68	A5	VIP	90	0	10
162	69	D13	Premium	100	0	0
163	69	E13	Premium	100	0	0
164	70	G1	Premium	80	0	20
165	70	H1	Normal	80	0	20
166	70	I1	Normal	80	0	20
167	70	I2	Normal	80	0	20
168	70	I3	Normal	80	0	20
169	71	A13	VIP	100	75	0
170	72	A1	VIP	160	0	20
171	72	A2	VIP	160	0	20
172	72	A3	VIP	160	0	20
173	72	B2	VIP	160	0	20
174	72	B3	VIP	160	0	20
179	74	A8	VIP	80	0	20
180	74	A9	VIP	80	0	20
181	74	A10	VIP	80	0	20
182	74	A11	VIP	80	0	20
183	74	A12	VIP	80	0	20
184	74	A13	VIP	80	0	20
185	74	B13	VIP	80	0	20
186	75	B1	VIP	75	0	25
187	75	C1	VIP	75	0	25
188	75	D1	Premium	75	0	25
189	75	E1	Premium	75	0	25
190	75	E3	Premium	75	0	25
191	75	F1	Premium	75	0	25
192	75	F2	Premium	75	0	25
193	75	F3	Premium	75	0	25
175	73	A1	VIP	100	75	0
176	73	A2	VIP	100	75	0
177	73	A3	VIP	100	75	0
178	73	A4	VIP	100	75	0
194	76	A13	VIP	200	150	0
195	76	A14	VIP	200	150	0
196	77	A1	VIP	300	0	25
197	77	A4	VIP	300	0	25
198	77	A5	VIP	300	0	25
199	77	A7	VIP	300	0	25
200	77	C11	VIP	300	0	25
201	77	F12	Premium	225	0	25
202	77	G12	Premium	225	0	25
203	77	I8	Normal	150	0	25
204	77	I12	Normal	150	0	25
205	77	L12	Normal	150	0	25
206	78	I3	Normal	200	0	0
207	78	I4	Normal	200	0	0
208	79	A1	VIP	300	225	0
209	79	B1	VIP	300	225	0
210	80	A1	VIP	300	270	0
211	80	C1	Premium	200	180	0
212	80	F1	Normal	100	90	0
213	81	D1	Premium	200	0	0
214	81	D2	Premium	200	0	0
215	82	C1	VIP	100	0	0
216	82	C2	VIP	100	0	0
217	82	C3	VIP	100	0	0
218	82	C4	VIP	100	0	0
\.


--
-- Data for Name: user_role; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.user_role (user_role_id, role) FROM stdin;
1	admin
2	user
3	manager
\.


--
-- Data for Name: users; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.users (name, email, password, mobilenumber, user_role_id, user_id, wallet, "isActive") FROM stdin;
Admin	admin@gmail.com	123	6382778409	1	1	0	1
Prabu	prabu@gmail.com	123	9842325889	3	3	0	1
Naveen Kumar	naveen@gmail.com	123	\N	2	29	0	1
PVR - Manager	ajithkumar6382pmp@gmail.com	\N	\N	3	28	0	1
Ajith	ajithkumar6382pmp@gmail.com	123	9842325889	2	2	5136	1
Nithish	nithish@gmail.com	123	9876543210	2	4	0	1
Karpagam Cinimas - Manager	karapagamainthan@gmail.com	\N	\N	3	6	0	1
\.


--
-- Name: language_language_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.language_language_id_seq', 1, false);


--
-- Name: movie_language_mapping_movie_language_mapping_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.movie_language_mapping_movie_language_mapping_id_seq', 150, true);


--
-- Name: movie_movie_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.movie_movie_id_seq', 36, true);


--
-- Name: offer_offer_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.offer_offer_id_seq', 6, true);


--
-- Name: screen_theater_screen_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.screen_theater_screen_id_seq', 7, true);


--
-- Name: seat_seat_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.seat_seat_id_seq', 4956, true);


--
-- Name: show_show_id_seq1; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.show_show_id_seq1', 159, true);


--
-- Name: theater_theater_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.theater_theater_id_seq', 16, true);


--
-- Name: ticket_ticket_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.ticket_ticket_id_seq', 82, true);


--
-- Name: ticket_ticket_id_seq1; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.ticket_ticket_id_seq1', 218, true);


--
-- Name: users_user_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.users_user_id_seq', 34, true);


--
-- Name: booking booking_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.booking
    ADD CONSTRAINT booking_pkey PRIMARY KEY (booking_id);


--
-- Name: language language_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.language
    ADD CONSTRAINT language_pkey PRIMARY KEY (language_id);


--
-- Name: movie_language_mapping movie_language_mapping_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.movie_language_mapping
    ADD CONSTRAINT movie_language_mapping_pkey PRIMARY KEY (movie_language_mapping_id);


--
-- Name: movie_language_mapping movie_language_mapping_ukey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.movie_language_mapping
    ADD CONSTRAINT movie_language_mapping_ukey UNIQUE (movie_id, language_id);


--
-- Name: movie movie_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.movie
    ADD CONSTRAINT movie_pkey PRIMARY KEY (movie_id);


--
-- Name: offer offer_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.offer
    ADD CONSTRAINT offer_pkey PRIMARY KEY (offer_id);


--
-- Name: screen screen_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.screen
    ADD CONSTRAINT screen_pkey PRIMARY KEY (screen_id);


--
-- Name: seating_arrangement seat_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.seating_arrangement
    ADD CONSTRAINT seat_pkey PRIMARY KEY (seating_id);


--
-- Name: show show_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.show
    ADD CONSTRAINT show_pkey PRIMARY KEY (show_id);


--
-- Name: theater theater_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.theater
    ADD CONSTRAINT theater_pkey PRIMARY KEY (theater_id);


--
-- Name: ticket ticket_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ticket
    ADD CONSTRAINT ticket_pkey PRIMARY KEY (ticket_id);


--
-- Name: user_role user_role_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.user_role
    ADD CONSTRAINT user_role_pkey PRIMARY KEY (user_role_id);


--
-- Name: users users_pk; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pk PRIMARY KEY (user_id, email);


--
-- Name: users users_uk; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_uk UNIQUE (user_id);


--
-- Name: users users_uk2; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_uk2 UNIQUE (email, user_role_id);


--
-- Name: offer booking_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.offer
    ADD CONSTRAINT booking_fkey FOREIGN KEY (offer_id) REFERENCES public.offer(offer_id) NOT VALID;


--
-- Name: booking booking_fkey1; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.booking
    ADD CONSTRAINT booking_fkey1 FOREIGN KEY (show_id) REFERENCES public.show(show_id);


--
-- Name: booking booking_fkey2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.booking
    ADD CONSTRAINT booking_fkey2 FOREIGN KEY (user_id) REFERENCES public.users(user_id) NOT VALID;


--
-- Name: movie_language_mapping movie_language_mapping_fkey1; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.movie_language_mapping
    ADD CONSTRAINT movie_language_mapping_fkey1 FOREIGN KEY (movie_id) REFERENCES public.movie(movie_id) NOT VALID;


--
-- Name: movie_language_mapping movie_language_mapping_fkey2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.movie_language_mapping
    ADD CONSTRAINT movie_language_mapping_fkey2 FOREIGN KEY (language_id) REFERENCES public.language(language_id) NOT VALID;


--
-- Name: seating_arrangement seat_fkey1; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.seating_arrangement
    ADD CONSTRAINT seat_fkey1 FOREIGN KEY (screen_id) REFERENCES public.screen(screen_id);


--
-- Name: show show_fkey1; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.show
    ADD CONSTRAINT show_fkey1 FOREIGN KEY (screen_id) REFERENCES public.screen(screen_id) NOT VALID;


--
-- Name: show show_fkey2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.show
    ADD CONSTRAINT show_fkey2 FOREIGN KEY (movie_language_mapping_id) REFERENCES public.movie_language_mapping(movie_language_mapping_id) NOT VALID;


--
-- Name: theater theater_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.theater
    ADD CONSTRAINT theater_fkey FOREIGN KEY (manager_id) REFERENCES public.users(user_id) NOT VALID;


--
-- Name: ticket ticket_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ticket
    ADD CONSTRAINT ticket_fkey FOREIGN KEY (booking_id) REFERENCES public.booking(booking_id);


--
-- Name: users user_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT user_fkey FOREIGN KEY (user_role_id) REFERENCES public.user_role(user_role_id) NOT VALID;


--
-- PostgreSQL database dump complete
--

