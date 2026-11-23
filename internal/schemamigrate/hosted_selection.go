package schemamigrate

// The current command to the consumers of ONE hostname. Its fence never
// advances another host, even if they share the same project, app or zone.
// Keep the chosen executable image here: a preparation offer is replaceable,
// but replacing it cannot erase the program needed to withdraw live routes.
const HostedSelectionsSQL = `CREATE TABLE IF NOT EXISTS fugue_hosted_selections (
 hostname text PRIMARY KEY, scope_key text NOT NULL, project_id text NOT NULL,
 offer_sequence bigint NOT NULL CHECK(offer_sequence>0), draft_id text NOT NULL,
 draft_digest text NOT NULL, program_digest text NOT NULL,
 program bytea NOT NULL CHECK(octet_length(program) BETWEEN 1 AND 4194304),
 fence bigint NOT NULL CHECK(fence>0), decision text NOT NULL, authority_digest text NOT NULL,
 business_digest text NOT NULL,
 owner_digest text NOT NULL,path_digest text NOT NULL,prepared_digest text NOT NULL,forward_digest text NOT NULL,
 baseline_path_digest text NOT NULL CHECK(baseline_path_digest ~ '^sha256:[a-f0-9]{64}$'),
 phase text NOT NULL CHECK(phase IN ('prepare','forward','withdraw')),
 members jsonb NOT NULL CHECK(jsonb_typeof(members)='object' AND members<>'{}'::jsonb),
 previous_decision text NOT NULL, previous_members jsonb NOT NULL CHECK(jsonb_typeof(previous_members)='object'),
 skipped_forward_from text NOT NULL CHECK((phase='withdraw' AND fence>=3 AND skipped_forward_from ~ '^sha256:[a-f0-9]{64}$')
 OR (phase<>'withdraw' AND skipped_forward_from='')),
 CHECK((fence=1 AND previous_decision='' AND previous_members='{}'::jsonb) OR
 (fence>1 AND previous_decision ~ '^sha256:[a-f0-9]{64}$' AND previous_members<>'{}'::jsonb)),
 selected_at timestamptz NOT NULL, valid_until timestamptz NOT NULL CHECK(valid_until>selected_at),
 CHECK(draft_digest ~ '^sha256:[a-f0-9]{64}$' AND decision ~ '^sha256:[a-f0-9]{64}$'
 AND authority_digest ~ '^sha256:[a-f0-9]{64}$' AND business_digest ~ '^sha256:[a-f0-9]{64}$'
 AND owner_digest ~ '^sha256:[a-f0-9]{64}$' AND path_digest ~ '^sha256:[a-f0-9]{64}$'
 AND prepared_digest ~ '^sha256:[a-f0-9]{64}$' AND forward_digest ~ '^sha256:[a-f0-9]{64}$'),
 CHECK(program_digest='sha256:'||encode(sha256(program),'hex'))
)`

// Only actual bounded observations for the current head are kept. Previous
// facts cannot follow a host through a withdraw, even with identical bytes.
const HostedSelectionReceiptsSQL = `CREATE TABLE IF NOT EXISTS fugue_hosted_selection_receipts (
 hostname text NOT NULL REFERENCES fugue_hosted_selections(hostname), part_key text NOT NULL,
 fence bigint NOT NULL CHECK(fence>0), decision text NOT NULL, content_digest text NOT NULL,
 evidence_digest text NOT NULL, passing boolean NOT NULL, observed_at timestamptz NOT NULL,
 valid_until timestamptz NOT NULL CHECK(valid_until>observed_at),
 PRIMARY KEY(hostname,part_key),
 CHECK(part_key ~ '^sha256:[a-f0-9]{64}$' AND decision ~ '^sha256:[a-f0-9]{64}$'
 AND content_digest ~ '^sha256:[a-f0-9]{64}$' AND evidence_digest ~ '^sha256:[a-f0-9]{64}$')
)`

// Discover only active decisions for one's exact full assignment; no scan of
// application domains, no cross-tenant staging directory, and no offset drift.
const HostedMemberDirectorySQL = `CREATE TABLE IF NOT EXISTS fugue_hosted_member_directory (
 member_id text NOT NULL,kind text NOT NULL,set_id text NOT NULL,child_id text NOT NULL,
 hostname text COLLATE "C" NOT NULL REFERENCES fugue_hosted_selections(hostname),
 PRIMARY KEY(member_id,kind,set_id,child_id,hostname)
)`
