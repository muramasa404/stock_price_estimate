
 
truncate table stock.tb_stock_day_price
truncate table stock.tb_inv_net_buy_day


-----------------------------------

CREATE TABLE stock.tb_stock_code (
	STOCK_CD  VARCHAR(20),
	STOCK_NM  VARCHAR(100),
	MRKT_DIV  VARCHAR(20),
	STOCK_DESC_1 VARCHAR(100),
	STOCK_DESC_2 VARCHAR(100),
	STOCK_DESC_3 VARCHAR(100)
    
);

select  *
from stock.tb_stock_code

------------------------------------------------


drop table stock.tb_work_day
  
 CREATE TABLE stock.tb_work_day (
    WORK_DAY TEXT(8),
    WORK_YN TEXT(2),

    WORK_DIV TEXT(10),
    WORK_SEQ BIGINT,    
    PRIMARY KEY ( WORK_DAY )
) 


desc stock.tb_work_day


select  *  from stock.tb_work_day 
where 1=1 
and work_day = '20250418'


select  count(1)  from stock.tb_work_day 
where 1=1 


-----------------------------------------------------

drop table stock.tb_stock_day_price 

CREATE TABLE stock.tb_stock_day_price (
    BASE_DT VARCHAR(8),
    STOCK_CD VARCHAR(10),
    STOCK_NM VARCHAR(50),
    MRKT_DIV VARCHAR(10),
    STOCK_MNG VARCHAR(50),
    CLOSE_PRICE INT,
    PRICE_GAP INT,
    PRICE_GAP_RATE FLOAT,
    OPEN_PRICE INT,
    HIGH_PRICE INT,
    LOW_PRICE INT,
    TRADE_QTY BIGINT,
    TRADE_AMT BIGINT,
    MRKT_CAPITAL BIGINT,
    ISSUE_STOCK_QTY BIGINT
) 

desc stock.tb_stock_day_price


select *
from stock.tb_stock_day_price


select base_dt, count(1)
from stock.tb_stock_day_price
group by base_dt 
order by 1

delete 
from stock.tb_stock_day_price
where base_dt = '20250418'

---------------------------------------

일자	종가	대비	등락률	시가	고가	저가	거래량


drop table stock.tb_stock_day_trx

CREATE TABLE stock.tb_stock_day_trx (
    BASE_DT VARCHAR(8),
    STOCK_CD VARCHAR(10),
    STOCK_NM VARCHAR(50),
    DOD INT ,
    DOD_RATE FLOAT,
    
    OPEN_PRICE INT,
    HIGH_PRICE INT,
    LOW_PRICE INT,
    TRADE_QTY BIGINT,
    
    PRIMARY KEY (BASE_DT, STOCK_CD )

) 

desc stock.tb_stock_day_trx



-------------


TRUNCATE table stock.tb_stock_code

drop table stock.tb_inv_net_buy_day

CREATE TABLE stock.tb_inv_net_buy_day (
    BASE_DT   VARCHAR(8),
	INV_DIV   VARCHAR(20),
	STOCK_CD  VARCHAR(20),
	STOCK_NM  VARCHAR(100),
	TRADE_SEL_QTY BIGINT,
	TRADE_BUY_QTY BIGINT,
	TRADE_NET_BUY_QTY BIGINT,
	TRADE_SEL_AMT BIGINT,
	TRADE_BUY_AMT BIGINT,
	TRADE_NET_BUY_AMT BIGINT,
	
	RANK_AMT INT,
	RANK_CNT INT,
	
	PRIMARY KEY (BASE_DT, INV_DIV, STOCK_CD )
);


desc stock.tb_inv_net_buy_day


select base_dt, count(1)
from stock.tb_inv_net_buy_day
group by base_dt 
order by 1



select *
from stock.tb_inv_net_buy_day
where 1=1
#and stock_cd = '108490'
and base_dt >= '20250516'


order by 1



-------------------------------------------------------


CREATE TABLE stock.tb_stock_inv_trx_m (
    BASE_DT VARCHAR(8),
    STOCK_CD VARCHAR(10),
    STOCK_NM VARCHAR(50),

    D1A_RANK_AMT INT,
    D1A_RANK_CNT INT,  
 
    D1B_RANK_AMT INT,
    D1B_RANK_CNT INT, 
    
    D1A_TRADE_NET_BUY_QTY BIGINT,
    D1B_TRADE_NET_BUY_QTY BIGINT,    

    D2A_TRADE_NET_BUY_QTY BIGINT,
    D2B_TRADE_NET_BUY_QTY BIGINT,  
    
    D3A_TRADE_NET_BUY_QTY BIGINT,
    D3B_TRADE_NET_BUY_QTY BIGINT,  
    
    D4A_TRADE_NET_BUY_QTY BIGINT,
    D4B_TRADE_NET_BUY_QTY BIGINT,  
    
    D5A_TRADE_NET_BUY_QTY BIGINT,
    D5B_TRADE_NET_BUY_QTY BIGINT,  
    
    D6A_TRADE_NET_BUY_QTY BIGINT,
    D6B_TRADE_NET_BUY_QTY BIGINT, 

    D7A_TRADE_NET_BUY_QTY BIGINT,
    D7B_TRADE_NET_BUY_QTY BIGINT,
    
    PRIMARY KEY (BASE_DT, STOCK_CD )
  
 ) 
 
 desc stock.tb_stock_inv_trx_m 
 
 
select  *  
from stock.tb_stock_inv_trx_m 
where 1=1
and stock_cd = '000150'

and base_dt >= '20250501'


 select  base_dt, count(1)  
 from stock.tb_stock_inv_trx_m
 group by base_dt

 
 select  stock_cd, stock_nm, count(1)  
 from stock.tb_stock_inv_trx_m
 where 1=1
 and base_dt >= '20250502'
 and stock_cd = '000660'
 group by  stock_cd, stock_nm
 order by 3 desc 
 
 -------------------------------------------------
 
 
 drop table stock.tb_stock_inv_trx_cnt

  
 CREATE TABLE stock.tb_stock_inv_trx_cnt (
    BASE_DT VARCHAR(8),
    STOCK_CD VARCHAR(10),
    STOCK_NM VARCHAR(50),

    INST_CNT INT,
    INST_CON_CNT INT,
    FORE_CNT  INT ,
    FORE_CON_CNT INT,
    BUY_CON_CNT INT,
    
    AVG_TRX_QTY BIGINT, 
    D1_TRX_QTY BIGINT,     
    D7_AVG_TRX_RATE float, 
    
    BUY_GRADE  VARCHAR(10),
    PRIMARY KEY (BASE_DT, STOCK_CD )
) 


ALTER TABLE stock.tb_stock_inv_trx_cnt  MODIFY COLUMN AVG_TRX_QTY float;

desc stock.tb_stock_inv_trx_cnt

select  *
from stock.tb_stock_inv_trx_cnt
where 1=1
-- and stock_cd = '000660'

and base_dt >= '20250502'



 select  base_dt, count(1)  
 from stock.tb_stock_inv_trx_cnt
 where 1=1
 and base_dt >= '20250701'
 group by base_dt


 ------------------------------
 
with check_con_cnt as 
( 
	 select  
	      stock_cd 
	     ,stock_nm
	     ,count(1) as cnt
	 from 
	      stock.tb_stock_inv_trx_cnt 
	 where 1=1
	 and base_dt >= '20250601'
	 and buy_con_cnt >= 3
	 group by 
	     stock_cd
	    ,stock_nm 
) 
select  *  
from
    check_con_cnt 
 where 1=1
 and cnt > 2

 
 -----------------------------------
 
desc stock.tb_stock_trx_idx

SELECT CONSTRAINT_NAME
FROM information_schema.TABLE_CONSTRAINTS
WHERE TABLE_SCHEMA = 'stock'
  AND TABLE_NAME = 'tb_stock_trx_idx'
  AND CONSTRAINT_TYPE = 'PRIMARY KEY';


select  max( base_dt)
from 
      stock.tb_stock_trx_idx

      
      
select  BASE_DT, COUNT(1)
from 
      stock.tb_stock_trx_idx
where 1=1
and base_dt >= '20250619'
group by BASE_DT 


      
      
select  *
from 
      stock.tb_stock_trx_idx
where 1=1
and base_dt >= '20250618'

SELECT BASE_DT, STOCK_CD, STOCK_NM, CLOSE_PRICE, TRADE_QTY
FROM tb_stock_day_price
WHERE BASE_DT BETWEEN '20250619' AND '20250619'
        



SELECT 
     A.*
	,B.RSI
	,B.OBV
FROM 
     STOCK.TB_STOCK_INV_TRX_CNT A 
	,STOCK.TB_STOCK_TRX_IDX  B
WHERE 1=1
AND A.BASE_DT = :base_date
AND B.BASE_DT = A.BASE_DT
AND B.STOCK_CD = A.STOCK_CD


select  *
from STOCK.TB_STOCK_TRX_IDX  B
where 1=1
and base_dt = '20250623'
and stock_cd = '000150'


-----------------------------------------

      
ALTER TABLE stock.tb_stock_trx_idx
DROP PRIMARY KEY;

ALTER TABLE stock.tb_stock_trx_idx
ADD PRIMARY KEY (BASE_DT, STOCK_CD);


drop table stock.tb_stock_trx_idx 

CREATE TABLE stock.tb_stock_trx_idx  (
 
	BASE_DT	varchar(8),
	STOCK_CD	varchar(10),
	STOCK_NM	varchar(50),
	CLOSE_PRICE	int(11),
	RSI	float,
	OBV	decimal(20,4),

   PRIMARY KEY (base_dt, stock_cd )
)


------------------------


select  *
from stock.tb_stock_day_price

 
--------------------------------------------------

drop table stock.tb_stock_mrkt_m

CREATE TABLE stock.tb_stock_mrkt_d (
 
     BASE_DT VARCHAR(8),
    
	,kospi	VARCHAR(20)
	,kospi_change	VARCHAR(20)
	,kospi_rate	VARCHAR(20)
	,kosdaq	VARCHAR(20)
	,kosdaq_change	VARCHAR(20)
	,kosdaq_rate	VARCHAR(20)
	,kospi200	VARCHAR(20)
	,kospi200_change	VARCHAR(20)
	,kospi200_rate	VARCHAR(20)

,PRIMARY KEY (base_dt ))


--------------------------------------------------

CREATE TABLE stock.tb_today_spec_stock (

BASE_DT 
stock_cd	VARCHAR (PK, Auto Increment)
date	DATE
stock_name	VARCHAR(100)
current_price	VARCHAR(20)
change	VARCHAR(20)
rate	VARCHAR(20)


--------------------------------------------------


SELECT stock_cd, base_dt, close_price
FROM stock.tb_stock_day_price
WHERE base_dt between '20250408' and '20250414' 
group by stock_cd, base_dt, close_price
having count(1) > 1


select  *
from stock.tb_stock_day_price
where 1=1 
and stock_cd = '000660'
and base_dt >= '20250502'
    

	select  
	       
			 base_dt
			,stock_cd
			,stock_nm
			,inst_cnt
			,inst_con_cnt
			,fore_cnt
			,fore_con_cnt
			,buy_con_cnt
			,avg_trx_qty
			,d1_trx_qty
			,d7_avg_trx_rate
			,buy_grade
	from 
	     stock.tb_stock_inv_trx_cnt 
	where 1=1
	and base_dt = '20250408'
	and buy_grade in  ( 'S')

	
	select 
	     work_day
	    ,work_seq 
    from 
         stock.tb_work_day 
   	where 1=1
	and base_dt = '20250408' 

	
-------------------------------------


with trx_base_dt as 
( 
   select 
         a.base_dt
        ,b.work_seq
        ,b.work_seq + 1  as next_work_seq
   from       
      stock.tb_stock_inv_trx_cnt  a 
     ,stock.tb_work_day  b 
   where 1=1
   and b.work_day = a.base_dt 
   group by work_day 
           ,work_seq
) 
,trx_next_base_dt as 
(
   select  
          b1.* 
         ,b2.work_day  as next_base_dt 
     from  
           trx_base_dt  b1 
          ,stock.tb_work_day  b2
     where 1=1
     and b1.next_work_seq = b2.work_seq 
) 
-- select  * from trx_next_base_dt 

select  
      a.*
     ,c.*
 from 
      trx_next_base_dt  x 
     ,stock.tb_stock_inv_trx_cnt  a 
     ,stock.tb_stock_day_price  c 
 where 1=1
 -- and x.base_dt >= '20250401'
 
 and a.base_dt  = x.base_dt 
 and c.base_dt  = x.next_base_dt 
 and c.stock_cd = a.stock_cd


----------------------------------


with trx_base_dt as
    (
        select
            a.base_dt
            ,b.work_seq
            ,b.work_seq + 1 as next_work_seq
        from
            stock.tb_stock_inv_trx_cnt a
            ,stock.tb_work_day b
        where 1=1
            and a.base_dt = '20250514'
            and b.work_day = a.base_dt
        group by work_day
            ,work_seq
    )
    ,trx_next_base_dt as
    (
        select
            b1.*
            ,b2.work_day as next_base_dt
        from
            trx_base_dt b1
            ,stock.tb_work_day b2
        where 1=1
            and b1.next_work_seq = b2.work_seq
    )
    select
        a.BASE_DT
        ,a.STOCK_CD
        ,a.STOCK_NM
        ,a.INST_CNT
        ,a.INST_CON_CNT
        ,a.FORE_CNT
        ,a.FORE_CON_CNT
        ,a.BUY_CON_CNT
        ,a.AVG_TRX_QTY
        ,a.D1_TRX_QTY
        ,a.D7_AVG_TRX_RATE
        ,a.BUY_GRADE
        ,c.BASE_DT as NEXT_BASE_DT
        ,c.MRKT_DIV
        ,c.CLOSE_PRICE
        ,c.PRICE_GAP
        ,c.PRICE_GAP_RATE
        ,c.OPEN_PRICE
        ,c.HIGH_PRICE
        ,c.LOW_PRICE
        ,c.TRADE_QTY
        ,c.TRADE_AMT
        ,c.MRKT_CAPITAL
        ,c.ISSUE_STOCK_QTY
    from
        trx_next_base_dt x
        ,stock.tb_stock_inv_trx_cnt a
        ,stock.tb_stock_day_price c
    where 1=1
        and a.base_dt = x.base_dt
        and c.base_dt = x.next_base_dt
        and c.stock_cd = a.stock_cd

        
        
---------------------------------------------------

SHOW VARIABLES LIKE 'collation%';

SHOW TABLE STATUS WHERE Name = 'stock.tb_stock_day_price';


SHOW VARIABLES LIKE 'character_set%';

SHOW VARIABLES LIKE 'collation%';


ALTER DATABASE stock CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;





