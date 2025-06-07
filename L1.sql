-- L1 google sheets
-- L1_status
create or replace view `omega-branch-455615-n1.L1.L1_status` 
as 
select 
CAST(id_status AS INT64) as product_status_id
,lower(status_name) as product_status_name
,date(timestamp(date_update,"Europe/Prague")) as product_status_update_date 
from `omega-branch-455615-n1.L0_google_sheets.status`
where 
id_status is not null 
and status_name is not null
 qualify row_number() over(partition by product_status_id) =1;



--L1_product
create or replace view `omega-branch-455615-n1.L1.L1_product`
as select 
cast(id_product as INT) as product_id
,lower(name) as product_name
,lower(type) as product_type
,lower(category) as product_category
,cast(is_vat_applicable as boolean) as product_vat
,DATE(TIMESTAMP(date_update), "Europe/Prague") as date_update
from `omega-branch-455615-n1.L0_google_sheets.all_products`
where 
id_product is not null 
 qualify row_number() over(partition by id_product) =1;



--L1_branch
create or replace view `omega-branch-455615-n1.L1.L1_branch`
as select 
cast(id_branch as INT) as branch_id
,lower(branch_name) as branch_name
,DATE(TIMESTAMP(date_update), "Europe/Prague") as product_status_update_date
from `omega-branch-455615-n1.L0_google_sheets.branch`
where 
id_branch <> "NULL"
and branch_name is not null
 qualify row_number() over(partition by id_branch) =1;





--L1_accounting system
--L1_invoice
create or replace view `omega-branch-455615-n1.L1.L1_invoice` as 
select 
cast(id_invoice as INT64) as invoice_id
,cast(id_branch as INT64) as branch_id
,cast(id_invoice_old as INT64) as invoice_previous_id
,cast(invoice_id_contract as INT64) as contract_id
,date(date,"Europe/Prague") as date_issue
,date(scadent,"Europe/Prague") as due_date
,date(date_paid,"Europe/Prague") as paid_date
,date(start_date,"Europe/Prague") as start_date
,date(end_date,"Europe/Prague") as end_date
,date(date_insert,"Europe/Prague") as insert_date
,date(date_update,"Europe/Prague") as update_date
,cast(value as FLOAT64) as amount_w_vat
,cast(payed as FLOAT64) as amount_payed
,lower(number) as invoice_number
,cast(value_storno as FLOAT64) as return_w_vat
,cast(flag_paid_currier as BOOLEAN) as flag_paid_currier
,cast(status as INT) as invoice_status
,if(status<100,TRUE,FALSE) as flag_invoice_issued
,invoice_type as invoice_type_id --invoice_type: 1-invoice, 3- credit_note, 2- return, 4-other
,case
 when invoice_type=1 then "invoice"
 when invoice_type=2 then "return"
 when invoice_type=3 then "credit_note"
 when invoice_type=4 then "other"
 end as invoice_type
FROM `omega-branch-455615-n1.L0_accounting_system.invoice`
where 
id_invoice is not null 
 qualify row_number() over(partition by id_invoice) =1;



--invoices_load
create or replace view `omega-branch-455615-n1.L1.L1_invoice_load` as 
select 
cast(id_load as INT) as invoice_load_id
,cast(id_contract as INT) as contract_id
,cast(id_package as INT) as product_purchase_id
,cast(id_package_template as INT) as product_id
,cast(id_invoice as INT) as invoice_id
,cast(notlei as FLOAT64) as price_wo_vat_usd
,lower(currency) as currency
,cast(tva as INT) as vat_rate
,cast(value as FLOAT64) as price_w_vat_usd
,cast(payed as FLOAT64) as paid_w_vat_usd
, CASE
    WHEN LOWER(um) IN (
      "mesiac", "mesice", "měsíc", "měsíce", "měsice", "mesiace") THEN "month"
    WHEN LOWER(um) = "kus" THEN "item"
    WHEN LOWER(um) = "den" THEN "day"
    WHEN um = "0" THEN NULL
    ELSE um
  END AS unit
,cast(quantity as FLOAT64) as quantity
,DATE(TIMESTAMP(start_date), "Europe/Prague") as start_date
,DATE(TIMESTAMP(end_date), "Europe/Prague") as end_date
,DATE(TIMESTAMP(date_insert), "Europe/Prague") as date_insert
,DATE(TIMESTAMP(date_update), "Europe/Prague") as date_update
FROM `omega-branch-455615-n1.L0_accounting_system.invoices_load`
where 
id_load is not null 
 qualify row_number() over(partition by id_load) =1;




--L1_crm
--L1_product_purchase
CREATE OR REPLACE VIEW `omega-branch-455615-n1.L1.L1_product_purchase` AS
SELECT 
  CAST(id_package AS INT64) AS product_purchase_id,
  CAST(id_contract AS INT64) AS contract_id,
  CAST(id_package_template AS INT64) AS product_id,
  DATE(TIMESTAMP(date_insert), "Europe/Prague") AS create_date,
  DATE(TIMESTAMP(start_date), "Europe/Prague") AS product_valid_from,
  DATE(TIMESTAMP(end_date), "Europe/Prague") AS product_valid_to,
  CAST(fee AS FLOAT64) AS price_wo_vat,
  DATE(TIMESTAMP(purchase.date_update), "Europe/Prague") AS update_date,
  CAST(package_status AS INT64) AS product_status_id,
  CASE
    WHEN LOWER(measure_unit) IN ("mesiac", "mesice", "měsíc", "měsíce", "měsice", "mesiace") THEN "month"
    WHEN LOWER(measure_unit) = "kus" THEN "item"
    WHEN LOWER(measure_unit) = "den" THEN "day"
    WHEN measure_unit = "0" THEN NULL
    ELSE measure_unit
  END AS unit,
  CAST(id_branch AS INT64) AS branch_id,
  DATE(TIMESTAMP(load_date), "Europe/Prague") AS load_date,
  product.product_name,
  product.product_type,
  product.product_category,
  status.product_status_name
FROM `omega-branch-455615-n1.L0_crm.product_purchases` AS purchase
LEFT JOIN `omega-branch-455615-n1.L1.L1_product` AS product
  ON purchase.id_package_template = product.product_id
LEFT JOIN `omega-branch-455615-n1.L1.L1_status` AS status
  ON purchase.package_status = status.product_status_id
where 
id_package is not null 
 qualify row_number() over(partition by id_package) =1;


--L1_contract
create or replace view `omega-branch-455615-n1.L1.L1_contract`
as select 
cast(id_contract as INT) as contract_id
,cast(id_branch as INT) as branch_id
,DATE(TIMESTAMP(date_contract_valid_from), "Europe/Prague") contract_valid_from
,DATE(TIMESTAMP(date_contract_valid_to), "Europe/Prague") contract_valid_to
,DATE(TIMESTAMP(date_registered), "Europe/Prague") as registered_date
,DATE(TIMESTAMP(date_signed), "Europe/Prague") as signed_date
,DATE(TIMESTAMP(activation_process_date), "Europe/Prague") as activation_process_date
,DATE(TIMESTAMP(prolongation_date), "Europe/Prague") as prolongation_date
,lower(registration_end_reason) as registration_end_reason
,cast(flag_prolongation as BOOLEAN) as flag_prolongation
,cast(flag_send_inv_email as BOOLEAN) as flag_send_email
,lower(contract_status) as contract_status
,DATE(TIMESTAMP(load_date), "Europe/Prague") as load_date
from `omega-branch-455615-n1.L0_crm.contracts` 
where 
id_contract is not null 
 qualify row_number() over(partition by id_contract) =1;




