drop table if exists tb_abt_churn;
create table tb_abt_churn AS

with tb_safras_ref as(
    select *
    
    from tb_seller_book
    where  nr_partition_year <= '2018'
    and (nr_partition_day = '01' or nr_partition_day = '16' )
    and DATE(dt_ref, '+' || CAST(91 - seller_recencia_ciclo AS TEXT) || ' days')  <='2018-01-01'
)


select t1.*,
        case when t2.seller_id is null then 1 else 0 end as flag_churn

from tb_safras_ref as t1

left join tb_seller_book as t2
on t1.seller_id = t2.seller_id
and DATE(t1.dt_ref, '+91 days')  = t2.dt_ref;