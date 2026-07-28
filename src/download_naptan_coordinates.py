from __future__ import annotations

import csv
import io
import re
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


STOP_CATALOGUE_FILE = Path(
    "data/interim/stagecoach_southeast_stops.csv"
)

RAW_NAPTAN_DIRECTORY = Path(
    "data/raw/naptan"
)

OUTPUT_FILE = Path(
    "data/interim/naptan_stagecoach_stops.csv"
)

SUMMARY_FILE = Path(
    "outputs/metrics/naptan_coordinate_summary.csv"
)

UNMATCHED_FILE = Path(
    "outputs/metrics/naptan_unmatched_stops.csv"
)

API_BASE_URL = (
    "https://naptan.api.dft.gov.uk/v1/access-nodes"
)


def first_available(
    row: dict[str, str],
    possible_names: list[str],
) -> str:
    """Return the first available non-empty CSV field."""

    for name in possible_names:
        value = row.get(name)

        if value is not None and value.strip():
            return value.strip()

    return ""


def valid_coordinate(
    value: str,
    minimum: float,
    maximum: float,
) -> str:
    """Return a validated coordinate or an empty string."""

    if not value:
        return ""

    try:
        numeric_value = float(value)

        if minimum <= numeric_value <= maximum:
            return str(numeric_value)

    except (TypeError, ValueError):
        pass

    return ""


def extract_atco_area_code(
    stop_ref: str,
) -> str:
    """
    Extract the first three numerical characters from an
    ATCO stop reference.
    """

    match = re.match(r"^(\d{3})", stop_ref)

    if match:
        return match.group(1)

    return ""


def download_area_csv(
    area_code: str,
) -> bytes:
    """Download one ATCO area's NaPTAN data as CSV."""

    query = urlencode(
        {
            "dataFormat": "CSV",
            "atcoAreaCodes": area_code,
        }
    )

    request_url = f"{API_BASE_URL}?{query}"

    request = Request(
        request_url,
        headers={
            "User-Agent": (
                "ST5011CEM-Bus-Timetable-Project/1.0"
            )
        },
    )

    try:
        with urlopen(
            request,
            timeout=180,
        ) as response:
            response_data = response.read()

    except HTTPError as error:
        raise RuntimeError(
            f"NaPTAN returned HTTP {error.code} "
            f"for ATCO area {area_code}."
        ) from error

    except URLError as error:
        raise RuntimeError(
            f"Could not connect to NaPTAN for "
            f"ATCO area {area_code}: {error.reason}"
        ) from error

    if not response_data:
        raise RuntimeError(
            f"NaPTAN returned an empty response "
            f"for ATCO area {area_code}."
        )

    return response_data


def parse_naptan_rows(
    csv_bytes: bytes,
) -> list[dict[str, str]]:
    """Parse a downloaded NaPTAN CSV response."""

    csv_text = csv_bytes.decode(
        "utf-8-sig",
        errors="replace",
    )

    reader = csv.DictReader(
        io.StringIO(csv_text)
    )

    return list(reader)


def main() -> None:
    print("\n" + "=" * 70)
    print("NAPTAN STOP COORDINATE DOWNLOAD")
    print("=" * 70)

    if not STOP_CATALOGUE_FILE.exists():
        raise FileNotFoundError(
            "The extracted stop catalogue was not found: "
            f"{STOP_CATALOGUE_FILE}"
        )

    with STOP_CATALOGUE_FILE.open(
        "r",
        newline="",
        encoding="utf-8-sig",
    ) as input_file:
        catalogue_rows = list(
            csv.DictReader(input_file)
        )

    catalogue_by_ref: dict[
        str,
        dict[str, str],
    ] = {}

    for row in catalogue_rows:
        stop_ref = row.get(
            "stop_ref",
            "",
        ).strip()

        if stop_ref:
            catalogue_by_ref[stop_ref] = row

    if not catalogue_by_ref:
        raise RuntimeError(
            "The stop catalogue contains no stop references."
        )

    area_codes = sorted(
        {
            extract_atco_area_code(stop_ref)
            for stop_ref in catalogue_by_ref
            if extract_atco_area_code(stop_ref)
        }
    )

    if not area_codes:
        raise RuntimeError(
            "No valid ATCO area codes could be extracted."
        )

    print(
        f"Timetable stop references: "
        f"{len(catalogue_by_ref):,}"
    )

    print(
        "ATCO area codes identified: "
        + ", ".join(area_codes)
    )

    RAW_NAPTAN_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )

    matched_records: dict[
        str,
        dict[str, str],
    ] = {}

    total_downloaded_rows = 0

    for area_code in area_codes:
        print(
            f"\nDownloading ATCO area {area_code}..."
        )

        response_bytes = download_area_csv(
            area_code
        )

        raw_file = (
            RAW_NAPTAN_DIRECTORY
            / f"naptan_area_{area_code}.csv"
        )

        raw_file.write_bytes(response_bytes)

        area_rows = parse_naptan_rows(
            response_bytes
        )

        total_downloaded_rows += len(
            area_rows
        )

        area_matches = 0

        for naptan_row in area_rows:
            stop_ref = first_available(
                naptan_row,
                [
                    "ATCOCode",
                    "AtcoCode",
                    "atcoCode",
                ],
            )

            if stop_ref not in catalogue_by_ref:
                continue

            timetable_row = (
                catalogue_by_ref[stop_ref]
            )

            latitude = valid_coordinate(
                first_available(
                    naptan_row,
                    ["Latitude", "latitude"],
                ),
                -90.0,
                90.0,
            )

            longitude = valid_coordinate(
                first_available(
                    naptan_row,
                    ["Longitude", "longitude"],
                ),
                -180.0,
                180.0,
            )

            matched_records[stop_ref] = {
                "stop_ref": stop_ref,
                "timetable_common_name": (
                    timetable_row.get(
                        "common_name",
                        "",
                    )
                ),
                "naptan_common_name": (
                    first_available(
                        naptan_row,
                        ["CommonName", "commonName"],
                    )
                ),
                "locality_name": (
                    first_available(
                        naptan_row,
                        [
                            "LocalityName",
                            "localityName",
                        ],
                    )
                ),
                "town": first_available(
                    naptan_row,
                    ["Town", "town"],
                ),
                "latitude": latitude,
                "longitude": longitude,
                "has_coordinates": (
                    "1"
                    if latitude and longitude
                    else "0"
                ),
                "stop_type": first_available(
                    naptan_row,
                    ["StopType", "stopType"],
                ),
                "status": first_available(
                    naptan_row,
                    ["Status", "status"],
                ),
                "administrative_area_code": (
                    first_available(
                        naptan_row,
                        [
                            "AdministrativeAreaCode",
                            "administrativeAreaCode",
                        ],
                    )
                ),
                "atco_area_code": area_code,
            }

            area_matches += 1

        print(
            f"Downloaded rows: {len(area_rows):,}"
        )
        print(
            f"Timetable stop matches: "
            f"{area_matches:,}"
        )
        print(
            f"Raw file saved: {raw_file}"
        )

    output_records = sorted(
        matched_records.values(),
        key=lambda row: row["stop_ref"],
    )

    unmatched_refs = sorted(
        set(catalogue_by_ref)
        - set(matched_records)
    )

    coordinate_records = sum(
        row["has_coordinates"] == "1"
        for row in output_records
    )

    matched_count = len(output_records)
    catalogue_count = len(catalogue_by_ref)

    matched_percentage = (
        100.0
        * matched_count
        / catalogue_count
    )

    coordinate_coverage = (
        100.0
        * coordinate_records
        / catalogue_count
    )

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_fields = [
        "stop_ref",
        "timetable_common_name",
        "naptan_common_name",
        "locality_name",
        "town",
        "latitude",
        "longitude",
        "has_coordinates",
        "stop_type",
        "status",
        "administrative_area_code",
        "atco_area_code",
    ]

    with OUTPUT_FILE.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as output:
        writer = csv.DictWriter(
            output,
            fieldnames=output_fields,
        )

        writer.writeheader()
        writer.writerows(output_records)

    UNMATCHED_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with UNMATCHED_FILE.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as output:
        writer = csv.writer(output)
        writer.writerow(
            [
                "stop_ref",
                "timetable_common_name",
            ]
        )

        for stop_ref in unmatched_refs:
            writer.writerow(
                [
                    stop_ref,
                    catalogue_by_ref[
                        stop_ref
                    ].get(
                        "common_name",
                        "",
                    ),
                ]
            )

    summary_rows = [
        (
            "timetable_stop_references",
            catalogue_count,
        ),
        (
            "atco_area_codes_requested",
            len(area_codes),
        ),
        (
            "atco_area_code_list",
            "|".join(area_codes),
        ),
        (
            "total_naptan_rows_downloaded",
            total_downloaded_rows,
        ),
        (
            "matched_stop_references",
            matched_count,
        ),
        (
            "unmatched_stop_references",
            len(unmatched_refs),
        ),
        (
            "matched_stop_percentage",
            round(matched_percentage, 4),
        ),
        (
            "matched_stops_with_coordinates",
            coordinate_records,
        ),
        (
            "coordinate_coverage_percentage",
            round(coordinate_coverage, 4),
        ),
    ]

    SUMMARY_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with SUMMARY_FILE.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as output:
        writer = csv.writer(output)
        writer.writerow(["metric", "value"])
        writer.writerows(summary_rows)

    print("\n" + "-" * 70)
    print("NaPTAN integration preparation results")
    print("-" * 70)

    print(
        f"NaPTAN rows downloaded: "
        f"{total_downloaded_rows:,}"
    )

    print(
        f"Matched timetable stops: "
        f"{matched_count:,}/{catalogue_count:,}"
    )

    print(
        f"Matched percentage: "
        f"{matched_percentage:.2f}%"
    )

    print(
        f"Stops with coordinates: "
        f"{coordinate_records:,}"
    )

    print(
        f"Coordinate coverage: "
        f"{coordinate_coverage:.2f}%"
    )

    print(
        f"Unmatched stops: "
        f"{len(unmatched_refs):,}"
    )

    print("\nOutputs:")
    print(f"Matched stop dataset: {OUTPUT_FILE}")
    print(f"Summary: {SUMMARY_FILE}")
    print(f"Unmatched stops: {UNMATCHED_FILE}")
    print("=" * 70)
    print(
        "NaPTAN coordinate download completed successfully."
    )


if __name__ == "__main__":
    main()