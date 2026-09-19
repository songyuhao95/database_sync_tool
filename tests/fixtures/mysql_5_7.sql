-- cdc transaction=430c326c-ab91-11f1-a23b-0242ac160004:42 target=mysql-5.7
START TRANSACTION;
INSERT INTO `CDC_test`.`cdc_contract` (`id`, `tenant`, `message`, `amount`, `bytes`, `note`, `changed_at`) VALUES (?, ?, ?, ?, ?, ?, ?);
UPDATE `CDC_test`.`cdc_contract` SET `id` = ?, `tenant` = ?, `message` = ?, `amount` = ?, `bytes` = ?, `note` = ?, `changed_at` = ? WHERE `tenant` = ? AND `id` = ?;
DELETE FROM `CDC_test`.`cdc_contract` WHERE `tenant` = ? AND `id` = ?;
COMMIT;
