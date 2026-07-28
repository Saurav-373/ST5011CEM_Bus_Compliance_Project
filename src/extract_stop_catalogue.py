from __future__ import annotations

import csv
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional


TIMETABLE_ROOT = Path("data/raw/timetable")

OUTPUT_FILE = Path(
    "data/interim/stagecoach_southeast_stops.csv"
)

SUMMARY_FILE = Path(
    "outputs/metrics/stop_catalogue_summary.csv"
)


def local_name(tag: str) -> str:
    """Remove the namespace from an XML element tag."""

    return tag.split("}")[-1]


def first_text(
    element: ET.Element,
    candidate_names: list[str],
) -> str:
    """Return the first non-empty descendant value."""

    for child in element.iter():
        if local_name(child.tag) in candidate_names:
            if child.text and child.text.strip():
                return child.text.strip()

    return ""


def normalise_coordinate(
    value: str,
    minimum: float,
    maximum: float,
) -> str:
    """Validate a latitude or longitude value."""

    if not value:
        return ""

    try:
        numeric_value = float(value)

        if minimum <= numeric_value <= maximum:
            return str(numeric_value)

    except (TypeError, ValueError):
        pass

    return ""


def completeness_score(
    record: dict[str, str],
) -> int:
    """Score records so the most complete duplicate is retained."""

    useful_fields = [
        "common_name",
        "locality_name",
        "latitude",
        "longitude",
    ]

    return sum(
        bool(record.get(field_name))
        for field_name in useful_fields
    )


def extract_annotated_stop(
    element: ET.Element,
    source_file: Path,
) -> Optional[dict[str, str]]:
    """
    Extract an existing NaPTAN stop represented by
    AnnotatedStopPointRef.
    """

    stop_ref = first_text(
        element,
        ["StopPointRef"],
    )

    if not stop_ref:
        return None

    common_name = first_text(
        element,
        ["CommonName"],
    )

    locality_name = first_text(
        element,
        [
            "LocalityName",
            "LocalityQualifier",
            "NptgLocalityRef",
        ],
    )

    return {
        "stop_ref": stop_ref,
        "common_name": common_name,
        "locality_name": locality_name,
        "latitude": "",
        "longitude": "",
        "has_coordinates": "0",
        "source_type": "annotated_reference",
        "source_file": source_file.name,
    }


def extract_local_stop(
    element: ET.Element,
    source_file: Path,
) -> Optional[dict[str, str]]:
    """Extract a locally defined StopPoint."""

    stop_ref = first_text(
        element,
        [
            "AtcoCode",
            "StopPointRef",
        ],
    )

    if not stop_ref:
        return None

    common_name = first_text(
        element,
        ["CommonName"],
    )

    locality_name = first_text(
        element,
        [
            "LocalityName",
            "LocalityQualifier",
            "NptgLocalityRef",
        ],
    )

    latitude = normalise_coordinate(
        first_text(
            element,
            ["Latitude"],
        ),
        -90.0,
        90.0,
    )

    longitude = normalise_coordinate(
        first_text(
            element,
            ["Longitude"],
        ),
        -180.0,
        180.0,
    )

    has_coordinates = (
        "1"
        if latitude and longitude
        else "0"
    )

    return {
        "stop_ref": stop_ref,
        "common_name": common_name,
        "locality_name": locality_name,
        "latitude": latitude,
        "longitude": longitude,
        "has_coordinates": has_coordinates,
        "source_type": "local_definition",
        "source_file": source_file.name,
    }


def retain_best_record(
    selected_stops: dict[str, dict[str, str]],
    record: dict[str, str],
) -> bool:
    """
    Store a new stop or replace an existing duplicate with a
    more complete version.

    Returns True when the reference was already present.
    """

    stop_ref = record["stop_ref"]

    if stop_ref not in selected_stops:
        selected_stops[stop_ref] = record
        return False

    existing_record = selected_stops[stop_ref]

    if (
        completeness_score(record)
        > completeness_score(existing_record)
    ):
        selected_stops[stop_ref] = record

    return True


def main() -> None:
    print("\n" + "=" * 68)
    print("TRANXCHANGE STOP CATALOGUE EXTRACTION")
    print("=" * 68)

    if not TIMETABLE_ROOT.exists():
        raise FileNotFoundError(
            f"Timetable folder not found: {TIMETABLE_ROOT}"
        )

    xml_files = sorted(
        TIMETABLE_ROOT.rglob("*.xml")
    )

    if not xml_files:
        raise FileNotFoundError(
            "No XML timetable files were found under "
            f"{TIMETABLE_ROOT}"
        )

    print(f"XML files found: {len(xml_files):,}")

    selected_stops: dict[
        str,
        dict[str, str],
    ] = {}

    annotated_elements_read = 0
    local_stop_elements_read = 0
    duplicate_stop_references = 0
    unreadable_files = 0

    for file_number, xml_file in enumerate(
        xml_files,
        start=1,
    ):
        try:
            for _, element in ET.iterparse(
                xml_file,
                events=("end",),
            ):
                element_name = local_name(
                    element.tag
                )

                record: Optional[
                    dict[str, str]
                ] = None

                if (
                    element_name
                    == "AnnotatedStopPointRef"
                ):
                    annotated_elements_read += 1

                    record = extract_annotated_stop(
                        element,
                        xml_file,
                    )

                elif element_name == "StopPoint":
                    local_stop_elements_read += 1

                    record = extract_local_stop(
                        element,
                        xml_file,
                    )

                else:
                    continue

                if record is not None:
                    was_duplicate = retain_best_record(
                        selected_stops,
                        record,
                    )

                    if was_duplicate:
                        duplicate_stop_references += 1

                element.clear()

        except ET.ParseError as error:
            unreadable_files += 1

            print(
                f"Warning: could not parse "
                f"{xml_file.name}: {error}"
            )

        if (
            file_number % 20 == 0
            or file_number == len(xml_files)
        ):
            print(
                f"Processed {file_number:,}/"
                f"{len(xml_files):,} XML files"
            )

    stop_records = sorted(
        selected_stops.values(),
        key=lambda row: row["stop_ref"],
    )

    stops_with_coordinates = sum(
        row["has_coordinates"] == "1"
        for row in stop_records
    )

    stops_without_coordinates = (
        len(stop_records)
        - stops_with_coordinates
    )

    annotated_unique_stops = sum(
        row["source_type"]
        == "annotated_reference"
        for row in stop_records
    )

    local_unique_stops = sum(
        row["source_type"]
        == "local_definition"
        for row in stop_records
    )

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fieldnames = [
        "stop_ref",
        "common_name",
        "locality_name",
        "latitude",
        "longitude",
        "has_coordinates",
        "source_type",
        "source_file",
    ]

    with OUTPUT_FILE.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as output:
        writer = csv.DictWriter(
            output,
            fieldnames=fieldnames,
        )

        writer.writeheader()
        writer.writerows(stop_records)

    SUMMARY_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    coordinate_coverage = (
        round(
            100.0
            * stops_with_coordinates
            / len(stop_records),
            4,
        )
        if stop_records
        else 0.0
    )

    summary_rows = [
        (
            "xml_files_found",
            len(xml_files),
        ),
        (
            "unreadable_xml_files",
            unreadable_files,
        ),
        (
            "annotated_stop_elements_read",
            annotated_elements_read,
        ),
        (
            "local_stop_elements_read",
            local_stop_elements_read,
        ),
        (
            "total_stop_elements_read",
            annotated_elements_read
            + local_stop_elements_read,
        ),
        (
            "unique_stop_references",
            len(stop_records),
        ),
        (
            "duplicate_stop_references",
            duplicate_stop_references,
        ),
        (
            "unique_annotated_stops",
            annotated_unique_stops,
        ),
        (
            "unique_local_stops",
            local_unique_stops,
        ),
        (
            "stops_with_coordinates",
            stops_with_coordinates,
        ),
        (
            "stops_without_coordinates",
            stops_without_coordinates,
        ),
        (
            "coordinate_coverage_percentage",
            coordinate_coverage,
        ),
    ]

    with SUMMARY_FILE.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as output:
        writer = csv.writer(output)
        writer.writerow(
            ["metric", "value"]
        )
        writer.writerows(summary_rows)

    print("\nExtraction results:")
    print(
        "Annotated stop elements read: "
        f"{annotated_elements_read:,}"
    )
    print(
        "Local stop elements read: "
        f"{local_stop_elements_read:,}"
    )
    print(
        "Unique stop references: "
        f"{len(stop_records):,}"
    )
    print(
        "Duplicate stop references: "
        f"{duplicate_stop_references:,}"
    )
    print(
        "Stops with coordinates: "
        f"{stops_with_coordinates:,}"
    )
    print(
        "Stops without coordinates: "
        f"{stops_without_coordinates:,}"
    )
    print(
        "Coordinate coverage: "
        f"{coordinate_coverage:.2f}%"
    )

    print("\nFirst five extracted stops:")

    for record in stop_records[:5]:
        print(
            record["stop_ref"],
            "|",
            record["common_name"],
            "|",
            record["locality_name"],
            "|",
            record["source_type"],
        )

    print("\nOutputs:")
    print(f"Stop catalogue: {OUTPUT_FILE}")
    print(f"Extraction summary: {SUMMARY_FILE}")
    print("=" * 68)
    print(
        "Stop catalogue extraction completed successfully."
    )


if __name__ == "__main__":
    main()