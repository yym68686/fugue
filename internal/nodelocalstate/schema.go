package nodelocalstate

// SchemaJSON is the portable JSON Schema for disaster-recovery exports and
// offline tooling. Runtime publication additionally applies Validate and
// ValidateTransition, which enforce ordering, identity disjointness, and the
// one-step base/target/LKG rules that JSON Schema cannot express alone.
const SchemaJSON = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://fugue.pro/schemas/platform-state/nodelocal-membership-v1.json",
  "title": "Fugue NodeLocal membership desired state",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "contract_kind",
    "contract_version",
    "generation",
    "base_generation",
    "previous_generation",
    "rollback_generation",
    "compatibility",
    "transition",
    "desired",
    "enforcement",
    "audit"
  ],
  "properties": {
    "contract_kind": {"const": "node_local_dns_membership"},
    "contract_version": {"const": "nodelocal-membership-v1"},
    "generation": {"$ref": "#/$defs/generation"},
    "base_generation": {"$ref": "#/$defs/generation"},
    "previous_generation": {"$ref": "#/$defs/generation"},
    "rollback_generation": {"$ref": "#/$defs/generation"},
    "compatibility": {
      "type": "object",
      "additionalProperties": false,
      "required": ["controller_floor", "workload_contract_digest"],
      "properties": {
        "controller_floor": {"const": "nodelocal-membership-v1"},
        "workload_contract_digest": {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"}
      }
    },
    "transition": {
      "type": "object",
      "additionalProperties": false,
      "required": ["type"],
      "properties": {
        "type": {"enum": ["noop", "add_one_shadow", "workload_contract_update", "enforcement_update"]},
        "node": {"$ref": "#/$defs/node"}
      }
    },
    "desired": {
      "type": "object",
      "additionalProperties": false,
      "required": ["active_mode", "preserved_mode", "active_shadow", "preserved_iptables"],
      "properties": {
        "active_mode": {"const": "shadow"},
        "preserved_mode": {"const": "iptables"},
        "active_shadow": {"type": "array", "items": {"$ref": "#/$defs/node"}},
        "preserved_iptables": {"type": "array", "items": {"$ref": "#/$defs/node"}}
      }
    },
    "enforcement": {
      "type": "object",
      "additionalProperties": false,
      "required": ["mode"],
      "properties": {"mode": {"enum": ["report_only", "enforced"]}}
    },
    "audit": {
      "type": "object",
      "additionalProperties": false,
      "required": ["actor_type", "actor_id", "reason", "change_id"],
      "properties": {
        "actor_type": {"type": "string", "minLength": 1, "maxLength": 128},
        "actor_id": {"type": "string", "minLength": 1, "maxLength": 128},
        "reason": {"type": "string", "minLength": 8, "maxLength": 512},
        "change_id": {"type": "string", "minLength": 1, "maxLength": 128}
      }
    }
  },
  "$defs": {
    "generation": {"type": "string", "pattern": "^nodelocal-[0-9]{4,}$"},
    "node": {
      "type": "object",
      "additionalProperties": false,
      "required": ["fugue_node_id", "kubernetes_name", "expected_kubernetes_uid"],
      "properties": {
        "fugue_node_id": {"type": "string", "minLength": 1, "maxLength": 128},
        "kubernetes_name": {"type": "string", "minLength": 1, "maxLength": 253, "pattern": "^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$"},
        "expected_kubernetes_uid": {"type": "string", "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"}
      }
    }
  }
}`
