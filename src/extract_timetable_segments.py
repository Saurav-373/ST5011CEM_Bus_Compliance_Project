from __future__ import annotations

import csv
import re
import xml.etree.ElementTree as ET
from pathlib import Path


XML_FOLDER = Path(
    "data/raw/timetable/stagecoach_southeast_18511_extracted"
)

OUTPUT_FILE = Path(
    "data/interim/stagecoach_southeast_segments.csv"
)


def local_name(tag: str) -> str:
    """Remove the XML namespace from a tag."""
    return tag.split("}")[-1]


def find_direct_child(
    element: ET.Element,
    child_name: str,
) -> ET.Element | None:
    """Find a direct child by its local XML name."""

    for child in element:
        if local_name(child.tag) == child_name:
            return child

    return None


def child_text(
    element: ET.Element,
    child_name: str,
) -> str:
    """Return the text of a direct child or an empty string."""

    child = find_direct_child(element, child_name)

    if child is not None and child.text:
        return child.text.strip()

    return ""


def descendant_text(
    element: ET.Element | None,
    descendant_name: str,
) -> str:
    """Return the first matching descendant text."""

    if element is None:
        return ""

    for descendant in element.iter():
        if (
            local_name(descendant.tag) == descendant_name
            and descendant.text
        ):
            return descendant.text.strip()

    return ""


def parse_duration_seconds(value: str) -> int | None:
    """Convert an ISO duration such as PT2M30S into seconds."""

    if not value:
        return None

    pattern = re.fullmatch(
        r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?",
        value.strip(),
    )

    if not pattern:
        return None

    hours = int(pattern.group(1) or 0)
    minutes = int(pattern.group(2) or 0)
    seconds = int(pattern.group(3) or 0)

    return (hours * 3600) + (minutes * 60) + seconds


def build_timing_link_lookup(
    root: ET.Element,
) -> dict[str, dict[str, str]]:
    """Create a lookup for JourneyPatternTimingLink records."""

    lookup: dict[str, dict[str, str]] = {}

    for element in root.iter():
        if local_name(element.tag) != "JourneyPatternTimingLink":
            continue

        link_id = element.attrib.get("id", "")
        from_element = find_direct_child(element, "From")
        to_element = find_direct_child(element, "To")

        lookup[link_id] = {
            "from_sequence": (
                from_element.attrib.get("SequenceNumber", "")
                if from_element is not None
                else ""
            ),
            "from_stop_ref": descendant_text(
                from_element,
                "StopPointRef",
            ),
            "from_timing_status": descendant_text(
                from_element,
                "TimingStatus",
            ),
            "from_activity": descendant_text(
                from_element,
                "Activity",
            ),
            "to_sequence": (
                to_element.attrib.get("SequenceNumber", "")
                if to_element is not None
                else ""
            ),
            "to_stop_ref": descendant_text(
                to_element,
                "StopPointRef",
            ),
            "to_timing_status": descendant_text(
                to_element,
                "TimingStatus",
            ),
            "to_activity": descendant_text(
                to_element,
                "Activity",
            ),
            "route_link_ref": child_text(
                element,
                "RouteLinkRef",
            ),
        }

    return lookup


def main() -> None:
    xml_files = sorted(XML_FOLDER.rglob("*.xml"))

    if not xml_files:
        raise FileNotFoundError(
            f"No XML files found in {XML_FOLDER}"
        )

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "source_file",
        "vehicle_journey_code",
        "service_ref",
        "line_ref",
        "journey_pattern_ref",
        "departure_time",
        "vehicle_timing_link_id",
        "journey_pattern_timing_link_ref",
        "from_sequence",
        "from_stop_ref",
        "from_timing_status",
        "from_activity",
        "to_sequence",
        "to_stop_ref",
        "to_timing_status",
        "to_activity",
        "route_link_ref",
        "runtime_iso",
        "runtime_seconds",
        "runtime_minutes",
    ]

    extracted_rows = 0
    positive_runtimes = 0
    zero_runtimes = 0
    invalid_runtimes = 0
    missing_timing_link_matches = 0
    parse_errors = 0

    with OUTPUT_FILE.open(
        mode="w",
        newline="",
        encoding="utf-8-sig",
    ) as output:
        writer = csv.DictWriter(
            output,
            fieldnames=fieldnames,
        )
        writer.writeheader()

        for file_number, xml_file in enumerate(
            xml_files,
            start=1,
        ):
            try:
                root = ET.parse(xml_file).getroot()
                timing_lookup = build_timing_link_lookup(root)

                for journey in root.iter():
                    if local_name(journey.tag) != "VehicleJourney":
                        continue

                    vehicle_journey_code = child_text(
                        journey,
                        "VehicleJourneyCode",
                    )
                    service_ref = child_text(
                        journey,
                        "ServiceRef",
                    )
                    line_ref = child_text(
                        journey,
                        "LineRef",
                    )
                    journey_pattern_ref = child_text(
                        journey,
                        "JourneyPatternRef",
                    )
                    departure_time = child_text(
                        journey,
                        "DepartureTime",
                    )

                    for timing_link in journey.iter():
                        if (
                            local_name(timing_link.tag)
                            != "VehicleJourneyTimingLink"
                        ):
                            continue

                        timing_reference = child_text(
                            timing_link,
                            "JourneyPatternTimingLinkRef",
                        )

                        runtime_iso = child_text(
                            timing_link,
                            "RunTime",
                        )

                        runtime_seconds = parse_duration_seconds(
                            runtime_iso
                        )

                        if runtime_seconds is None:
                            invalid_runtimes += 1
                            runtime_minutes = ""
                        else:
                            runtime_minutes = round(
                                runtime_seconds / 60,
                                3,
                            )

                            if runtime_seconds == 0:
                                zero_runtimes += 1
                            else:
                                positive_runtimes += 1

                        segment = timing_lookup.get(
                            timing_reference,
                            {},
                        )

                        if not segment:
                            missing_timing_link_matches += 1

                        writer.writerow(
                            {
                                "source_file": xml_file.name,
                                "vehicle_journey_code": (
                                    vehicle_journey_code
                                ),
                                "service_ref": service_ref,
                                "line_ref": line_ref,
                                "journey_pattern_ref": (
                                    journey_pattern_ref
                                ),
                                "departure_time": departure_time,
                                "vehicle_timing_link_id": (
                                    timing_link.attrib.get("id", "")
                                ),
                                "journey_pattern_timing_link_ref": (
                                    timing_reference
                                ),
                                "from_sequence": segment.get(
                                    "from_sequence",
                                    "",
                                ),
                                "from_stop_ref": segment.get(
                                    "from_stop_ref",
                                    "",
                                ),
                                "from_timing_status": segment.get(
                                    "from_timing_status",
                                    "",
                                ),
                                "from_activity": segment.get(
                                    "from_activity",
                                    "",
                                ),
                                "to_sequence": segment.get(
                                    "to_sequence",
                                    "",
                                ),
                                "to_stop_ref": segment.get(
                                    "to_stop_ref",
                                    "",
                                ),
                                "to_timing_status": segment.get(
                                    "to_timing_status",
                                    "",
                                ),
                                "to_activity": segment.get(
                                    "to_activity",
                                    "",
                                ),
                                "route_link_ref": segment.get(
                                    "route_link_ref",
                                    "",
                                ),
                                "runtime_iso": runtime_iso,
                                "runtime_seconds": (
                                    runtime_seconds
                                    if runtime_seconds is not None
                                    else ""
                                ),
                                "runtime_minutes": runtime_minutes,
                            }
                        )

                        extracted_rows += 1

                if file_number % 20 == 0:
                    print(
                        f"Processed {file_number} "
                        f"of {len(xml_files)} XML files..."
                    )

            except ET.ParseError as error:
                parse_errors += 1
                print(
                    f"Could not parse {xml_file.name}: {error}"
                )

    print("\n" + "=" * 60)
    print("TIMETABLE EXTRACTION COMPLETE")
    print("=" * 60)
    print(f"XML files processed: {len(xml_files):,}")
    print(f"Rows extracted: {extracted_rows:,}")
    print(f"Positive runtimes: {positive_runtimes:,}")
    print(f"Zero runtimes: {zero_runtimes:,}")
    print(f"Invalid runtimes: {invalid_runtimes:,}")
    print(
        "Missing timing-link matches: "
        f"{missing_timing_link_matches:,}"
    )
    print(f"XML parse errors: {parse_errors:,}")
    print(f"Output file: {OUTPUT_FILE}")
    print("=" * 60)


if __name__ == "__main__":
    main()