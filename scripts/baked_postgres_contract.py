"""Immutable build contract shared by the baked operand and controller verifiers."""

import re

MANAGER_IMAGE = "ghcr.io/cloudnative-pg/cloudnative-pg@sha256:68074486205a33ed41928761e22ad48278c690feebe8316727a1c6b3380f9e5e"
POSTGRES_IMAGE = "ghcr.io/cloudnative-pg/postgresql:18.3-system-trixie@sha256:0f29b435fb501ee534cd0c555d122a6b8e90de477de8e8381c82c5e10d9a9de4"
MANAGER_SHA256 = {
    "manager_amd64": "b77f8f908fdc909122fe24fa164f8247c0689c4ca8c01ccef07fc50e7be8f6f1",
    "manager_arm64": "97fa01dc1c6abcd7f64a51d16f2e4ef9c7bb8d4d842e01adc9d07060e6b84157",
}
SCHEMA = "fugue.cnpg.baked-manager.v1"


def immutable_image(value):
    return isinstance(value, str) and re.fullmatch(r"[^\s@]+:[^/:\s@]+@sha256:[0-9a-f]{64}", value) is not None


def validate(catalog):
    expected = {"schema", "defaultImage", "managerImage", "managerSHA256", "images"}
    if not isinstance(catalog, dict) or set(catalog) != expected or catalog["schema"] != SCHEMA:
        raise ValueError("invalid baked-manager catalog")
    if catalog["managerImage"] != MANAGER_IMAGE or catalog["managerSHA256"] != MANAGER_SHA256:
        raise ValueError("baked-manager inventory changed; an explicit database upgrade plan is required")
    images = catalog["images"]
    if not isinstance(images, list) or len(images) > 256 or any(not immutable_image(i) for i in images) or len(set(images)) != len(images):
        raise ValueError("baked images must be unique tagged immutable references")
    if catalog["defaultImage"] and catalog["defaultImage"] not in images:
        raise ValueError("default baked image has not been verified")
    return catalog


def build_catalog(proof, previous_receipt):
    if (not immutable_image(proof.get("image")) or proof.get("manager_image") != MANAGER_IMAGE or
            proof.get("manager_sha256") != MANAGER_SHA256 or proof.get("postgres_base") != POSTGRES_IMAGE or
            proof.get("platforms") != ["linux/amd64", "linux/arm64"] or
            proof.get("readonly_startup_verified") is not True or proof.get("upgrade_path_verified") is not True):
        raise ValueError("baked image has not passed the complete artifact verification")
    previous = previous_receipt.get("baked_postgres_contract")
    images = list(validate(previous)["images"]) if previous else []
    if proof["image"] not in images:
        images.append(proof["image"])
    return validate({"schema": SCHEMA, "defaultImage": proof["image"], "managerImage": MANAGER_IMAGE,
                     "managerSHA256": MANAGER_SHA256.copy(), "images": images})
