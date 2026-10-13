package store

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"

	"fugue/internal/platformconfig"
)

func TestChunkedCompilerInputVerifiesNewStoredBytesBeforeCommit(t *testing.T) {
	state := boundedContentTestStore(t)
	for _, mutation := range []string{"changed_bytes", "changed_bytes_and_chunk_hash", "missing_chunk"} {
		t.Run(mutation, func(t *testing.T) {
			snapshot, digest := chunkedInputFixture(t)
			raw, err := platformconfig.EncodeRuntimeSnapshot(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			changed := append([]byte(nil), raw[:inputChunkBytes]...)
			changed[0] = '!'
			checksum := sha256.Sum256(changed)
			body := "NEW.data=set_byte(NEW.data,0,33);"
			if mutation == "changed_bytes_and_chunk_hash" {
				body += fmt.Sprintf("NEW.chunk_hash='sha256:%s';", hex.EncodeToString(checksum[:]))
			}
			if mutation == "missing_chunk" {
				body = "RETURN NULL;"
			}
			statement := fmt.Sprintf(`CREATE FUNCTION fugue_test_corrupt_input() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.content_hash='%s' AND NEW.ordinal=0 THEN %s END IF; RETURN NEW; END $$; CREATE TRIGGER fugue_test_corrupt_input BEFORE INSERT ON fugue_platform_input_chunks FOR EACH ROW EXECUTE FUNCTION fugue_test_corrupt_input();`, digest, body)
			if _, err := state.db.Exec(statement); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if _, err := state.db.Exec(`DROP TRIGGER fugue_test_corrupt_input ON fugue_platform_input_chunks; DROP FUNCTION fugue_test_corrupt_input();`); err != nil {
					t.Error(err)
				}
			})
			if err := state.EnsurePlatformCompilerInput(snapshot, digest); err == nil {
				t.Fatal("altered stored representation was accepted")
			}
			var leaked int
			if err := state.db.QueryRow(`SELECT (SELECT count(*) FROM fugue_platform_input_chunks WHERE content_hash=$1)+(SELECT count(*) FROM fugue_platform_input_manifests WHERE content_hash=$1)`, digest).Scan(&leaked); err != nil || leaked != 0 {
				t.Fatal("failed readback committed partial input", leaked, err)
			}
		})
	}
}
