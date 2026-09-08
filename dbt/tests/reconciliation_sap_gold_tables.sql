

with SAP_reconciliation as (

    select
        date_trunc('day', order_date)::date as revenue_date,
        'SAP' as source_system,
        sum(net_value) as total_revenue,
        count(*) as transaction_count
    from {{ ref('fct_sap_sales_orders') }}
    group by 1

),

daily_revenue as (

    select
        revenue_date, total_revenue
    from {{ ref('fct_daily_revenue_summary') }}
    where source_system = 'SAP'

)

select
    SAP_reconciliation.revenue_date,
    SAP_reconciliation.total_revenue as detail_total,
    daily_revenue.total_revenue as summary_total
from SAP_reconciliation
join daily_revenue
    on SAP_reconciliation.revenue_date = daily_revenue.revenue_date
where SAP_reconciliation.total_revenue != daily_revenue.total_revenue

