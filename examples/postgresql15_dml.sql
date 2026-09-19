-- Connect to CDC_test on 192.168.0.10:54321 as postgresql_writer.
-- Start cdc-pg-tail first. Execute these statements individually to see three transactions.
INSERT INTO public.cdc_pg15_demo (id, message, amount, metadata)
VALUES (90001, 'PostgreSQL 15 CDC test', 123456789012345678.123456, '{"hello": "world"}');

UPDATE public.cdc_pg15_demo
SET message = 'PostgreSQL 15 updated', enabled = FALSE, metadata = NULL, changed_at = now()
WHERE id = 90001;

DELETE FROM public.cdc_pg15_demo WHERE id = 90001;

-- A rolled-back change must not produce a ChangeEvent.
BEGIN;
INSERT INTO public.cdc_pg15_demo (id, message) VALUES (90002, 'rollback test');
ROLLBACK;
