--L3
--L3_branch
create or replace view `omega-branch-455615-n1.L3.L3_branch`
as select 
branch_id
,branch_name
from `omega-branch-455615-n1.L2.L2_branch`;


-- L3_product_purchase
CREATE OR REPLACE VIEW `omega-branch-455615-n1.L3.L3_product_purchase` AS
SELECT 
product_purchase_id
,product_id
,flag_unlimited_product
,unit
,product_name
,product_type
,product_valid_from
,product_valid_to
FROM `omega-branch-455615-n1.L2.L2_product_purchase` ;


--L3_invoice
create or replace view `omega-branch-455615-n1.L3.L3_invoice` as 
select 
invoice_id
,product_purchase_id
,product_id
,paid_date
,contract_id
,amount_w_vat
,return_w_vat
,amount_w_vat - return_w_vat as total_paid
FROM `omega-branch-455615-n1.L2.L2_invoice` ;


--L3_contract
create or replace view `omega-branch-455615-n1.L3.L3_contract`
as select 
contract_id
,branch_id
,contract_valid_from
,contract_valid_to
,registration_end_reason
,prolongation_flag
,contract_status 
,DATE_DIFF(contract_valid_to, contract_valid_from, DAY) as contract_duration
, case
when DATE_DIFF(contract_valid_to, contract_valid_from, DAY) < 365 then 'up to 1 year'
when DATE_DIFF(contract_valid_to, contract_valid_from, DAY) between 366 and 730 then 'up to 2 years'
else 'more than 2 years'
end as contract_duration_period
,EXTRACT(YEAR FROM contract_valid_from) as start_year_of_contract
from `omega-branch-455615-n1.L2.L2_contract`;




