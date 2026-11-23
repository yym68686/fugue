package schemamigrate

// The intervening relation/function/event rows are constants belonging to
// one migration, never caller input. Bit flags are PostgreSQL's ROW=1,
// BEFORE=2, INSERT=4, DELETE=8, UPDATE=16, TRUNCATE=32. Compare the actual
// trigger (not a version marker); exact existing attachments need no DDL.
// Function bodies are still installed in the same transaction, preserving OIDs.
const beforeTriggerReplayStart = `DO $$
DECLARE s record;
BEGIN
 FOR s IN
`
const beforeTriggerReplayEnd = `
 LOOP
  IF EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid=s.rel::regclass AND tgname=s.trig
      AND tgfoid=to_regprocedure(s.fn || '()') AND tgtype=s.bits AND tgenabled::text=s.mode
      AND NOT tgisinternal AND tgconstraint=0 AND tgparentid=0 AND tgnargs=0
      AND tgattr=''::int2vector AND tgqual IS NULL AND tgoldtable IS NULL AND tgnewtable IS NULL) THEN
   CONTINUE;
  END IF;
  EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I',s.trig,s.rel);
  EXECUTE format('CREATE TRIGGER %I BEFORE %s ON %I FOR EACH %s EXECUTE FUNCTION %I()',s.trig,s.events,s.rel,s.granularity,s.fn);
  IF s.mode='A' THEN
   EXECUTE format('ALTER TABLE %I ENABLE ALWAYS TRIGGER %I',s.rel,s.trig);
  END IF;
 END LOOP;
END $$;
`
