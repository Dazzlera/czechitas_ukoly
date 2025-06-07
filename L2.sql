--L2
--L2_product
create or replace view `omega-branch-455615-n1.L2.L2_product`
as select 
product_id
,product_name
,product_type
,product_category
from `omega-branch-455615-n1.L1.L1_product`;


--L2_branch
create or replace view `omega-branch-455615-n1.L2.L2_branch`
as select 
branch_id
,branch_name
from `omega-branch-455615-n1.L1.L1_branch`;


--L2_contract
create or replace view `omega-branch-455615-n1.L2.L2_contract`
as select 
contract_id
,branch_id
,contract_valid_from
,contract_valid_to
,registered_date
,registration_end_reason
,prolongation_date
,flag_prolongation as prolongation_flag
,contract_status
,activation_process_date
,signed_date
,flag_send_email
from `omega-branch-455615-n1.L1.L1_contract`;


--L2_invoice
create or replace view `omega-branch-455615-n1.L2.L2_invoice` as 
select 
i.invoice_id
,i.invoice_previous_id
,i.contract_id
,i.invoice_type
,case
when i.amount_w_vat<0 then 0
else i.amount_w_vat
 end as amount_w_vat
,i.return_w_vat
,i.amount_w_vat/1.2 as amount_wo_vat
,i.flag_invoice_issued
,i.invoice_status
--,invoice_order
,i.date_issue
,i.due_date
,i.paid_date
,i.start_date
,i.end_date
,i.insert_date
,i.update_date
,il.product_purchase_id
FROM `omega-branch-455615-n1.L1.L1_invoice` i
left join `omega-branch-455615-n1.L1.L1_invoice_load` il on i.invoice_id=il.invoice_id;



--L2_product_purchase
CREATE OR REPLACE VIEW `omega-branch-455615-n1.L2.L2_product_purchase` AS
SELECT 
product_purchase_id
,product_id
,contract_id
,product_category
,product_status_name
,price_wo_vat
,price_wo_vat * 1.2 AS price_w_vat
,case 
when (product_valid_to is null or product_valid_from is null) then "unlimited"
end as flag_unlimited_product
,product_valid_from
,product_valid_to
,unit
,product_name
,product_type
,create_date
,update_date
FROM `omega-branch-455615-n1.L1.L1_product_purchase` ;



