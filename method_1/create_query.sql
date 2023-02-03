-- PostgreSQL

CREATE TABLE public.sales_t (
	ID BIGINT NOT NULL PRIMARY KEY,
	CREATE_DATE TIMESTAMP NOT NULL,
	PAID_PRICE NUMERIC(10, 2) NOT NULL
);

CREATE TABLE public.store_t (
	ID BIGINT NOT NULL PRIMARY KEY,
	NAME VARCHAR(250) NOT NULL,
	LIVE_DATE TIMESTAMP NOT NULL,
	STATUS BOOL NOT NULL
);

CREATE TABLE public.customer_t (
	ID BIGINT NOT NULL PRIMARY KEY,
	NAME VARCHAR(250) NOT NULL,
	CITY VARCHAR NOT NULL,
	CREATE_DATE TIMESTAMP NOT NULL
);


CREATE TABLE public.sales_tx_t (
	ID BIGINT NOT NULL PRIMARY KEY,
	STATUS BOOL NOT NULL,
	SALES_ID BIGINT REFERENCES public.sales_t(ID),
	STORE_ID BIGINT REFERENCES public.store_t(ID)
);

CREATE TABLE public.test (
	transaction_id bigint NOT null primary key,
	transaction_status bool not null,
	store_name VARCHAR(250) NOT NULL,
	sales_date timestamp not NULL,
	paid_price NUMERIC(10, 2) NOT NULL,
	CREATE_DATE TIMESTAMP NOT NULL
);

/*********************************************/

create table accounting_unit_data_mart (
	CREATE_DATE TIMESTAMP NOT null,
	PAID_PRICE NUMERIC(10, 2) NOT NULL
);

select
	st.paid_price,
	CAST(st.create_date as DATE)
from 
	sales_tx_t stt left join sales_t st 
	on
		stt.sales_id = st.id
where
	stt.status = true;

/*********************************************/


/*********************************************/
create table marketing_unit_data_mart (
	store_name VARCHAR(250) NOT NULL,
	store_status BOOL NOT null,
	sales_date TIMESTAMP NOT null,
	sales_price NUMERIC(10, 2) NOT NULL
);

select
	st.name store_name,
	st.status store_status,
	st2.create_date sales_date,
	st2.paid_price sales_price
from 
	sales_tx_t stt left join store_t st
	on
		stt.store_id = st.id
	left join sales_t st2 
	on
		stt.sales_id = st2.id 
where
	stt.status = true
/*********************************************/


