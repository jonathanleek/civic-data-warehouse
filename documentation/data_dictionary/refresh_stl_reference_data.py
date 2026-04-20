from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen


BASE_DIR = Path(__file__).resolve().parent
MANIFEST_PATH = BASE_DIR / "reference_manifest.json"


@dataclass(frozen=True)
class DownloadSpec:
    category: str
    name: str
    url: str
    relative_path: str


DOWNLOADS = [
    DownloadSpec(
        category="source_field_definitions",
        name="2017 - 2023 Parcels field definitions",
        url="https://www.stlouis-mo.gov/customcf/endpoints/metadata/fields-download.cfm?distributionID=189&format=csv",
        relative_path="source_field_definitions/parcels_2017_2023_fields.csv",
    ),
    DownloadSpec(
        category="source_field_definitions",
        name="Parcel Joining Data field definitions",
        url="https://www.stlouis-mo.gov/customcf/endpoints/metadata/fields-download.cfm?distributionID=195&format=csv",
        relative_path="source_field_definitions/parcel_joining_data_fields.csv",
    ),
    DownloadSpec(
        category="controlled_vocabularies",
        name="Neighborhood",
        url="https://www.stlouis-mo.gov/customcf/endpoints/metadata/vocabulary-elements-download.cfm?id=1&format=csv",
        relative_path="controlled_vocabularies/neighborhood.csv",
    ),
    DownloadSpec(
        category="controlled_vocabularies",
        name="Assessor Class Code",
        url="https://www.stlouis-mo.gov/customcf/endpoints/metadata/vocabulary-elements-download.cfm?id=23&format=csv",
        relative_path="controlled_vocabularies/assessor_class_code.csv",
    ),
    DownloadSpec(
        category="controlled_vocabularies",
        name="Assessor Land Use",
        url="https://www.stlouis-mo.gov/customcf/endpoints/metadata/vocabulary-elements-download.cfm?id=24&format=csv",
        relative_path="controlled_vocabularies/assessor_land_use.csv",
    ),
    DownloadSpec(
        category="controlled_vocabularies",
        name="Parcel Attribute Type",
        url="https://www.stlouis-mo.gov/customcf/endpoints/metadata/vocabulary-elements-download.cfm?id=39&format=csv",
        relative_path="controlled_vocabularies/parcel_attribute_type.csv",
    ),
    DownloadSpec(
        category="controlled_vocabularies",
        name="Zoning Code",
        url="https://www.stlouis-mo.gov/customcf/endpoints/metadata/vocabulary-elements-download.cfm?id=53&format=csv",
        relative_path="controlled_vocabularies/zoning_code.csv",
    ),
    DownloadSpec(
        category="controlled_vocabularies",
        name="Building Exterior Wall Type",
        url="https://www.stlouis-mo.gov/customcf/endpoints/metadata/vocabulary-elements-download.cfm?id=58&format=csv",
        relative_path="controlled_vocabularies/building_exterior_wall_type.csv",
    ),
    DownloadSpec(
        category="controlled_vocabularies",
        name="Multi Parcel Ind",
        url="https://www.stlouis-mo.gov/customcf/endpoints/metadata/vocabulary-elements-download.cfm?id=69&format=csv",
        relative_path="controlled_vocabularies/multi_parcel_ind.csv",
    ),
    DownloadSpec(
        category="controlled_vocabularies",
        name="Number Of Units Source",
        url="https://www.stlouis-mo.gov/customcf/endpoints/metadata/vocabulary-elements-download.cfm?id=96&format=csv",
        relative_path="controlled_vocabularies/number_of_units_source.csv",
    ),
]


def fetch(url: str) -> bytes:
    request = Request(
        url,
        headers={
            "User-Agent": "civic-data-warehouse-data-dictionary/1.0",
        },
    )
    with urlopen(request, timeout=30) as response:
        return response.read()


def sha256_hex(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def main() -> None:
    timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    manifest: dict[str, object] = {
        "generated_at_utc": timestamp,
        "files": [],
    }

    for spec in DOWNLOADS:
        target = BASE_DIR / spec.relative_path
        target.parent.mkdir(parents=True, exist_ok=True)

        content = fetch(spec.url)
        target.write_bytes(content)

        manifest["files"].append(
            {
                "category": spec.category,
                "name": spec.name,
                "path": spec.relative_path.replace("\\", "/"),
                "source_url": spec.url,
                "sha256": sha256_hex(content),
                "bytes": len(content),
            }
        )
        print(f"downloaded {spec.relative_path}")

    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {MANIFEST_PATH.relative_to(BASE_DIR)}")


if __name__ == "__main__":
    main()
