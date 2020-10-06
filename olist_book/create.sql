DROP TABLE IF EXISTS tb_seller_book;
CREATE TABLE tb_seller_book
--USING DELTA
--OPTIONS(PATH='database/tb_sellers_book')
--PARTITIONED BY (nr_partition_year, nr_partition_month, nr_partition_day)
AS
{query}