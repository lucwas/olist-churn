SELECT *
FROM tb_seller_book

WHERE nr_partition_year = STRFTIME('%Y','{dt_ref}')
AND nr_partition_month =  STRFTIME('%m','{dt_ref}')
AND nr_partition_day =    STRFTIME('%d','{dt_ref}')