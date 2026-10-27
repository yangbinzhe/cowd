-- Test-only, installed solely in the pre-registered disposable R326 database.
-- Select the original durable event ID; a Session string is never an event ID.
CREATE SEQUENCE r326_receipt_fault_hits MAXVALUE 100000 NO CYCLE;
CREATE FUNCTION r326_memory_receipt_fault() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.target = 'memory.entry' AND NEW.error IS NULL
     AND NEW.status IN ('promoted', 'refreshed', 'duplicate')
     AND NEW.event_id = (SELECT event_id FROM growth_events WHERE session_id = 'r326-growth-A')
  THEN
    PERFORM nextval('r326_receipt_fault_hits');
    RAISE EXCEPTION USING ERRCODE='P0326', MESSAGE='owned R326 Memory receipt cut';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER r326_receipt_fault BEFORE INSERT OR UPDATE ON growth_promotions
FOR EACH ROW EXECUTE FUNCTION r326_memory_receipt_fault();
