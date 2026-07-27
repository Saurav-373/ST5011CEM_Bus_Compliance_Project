from pathlib import Path
import re
import xml.etree.ElementTree as ET


XML_FOLDER = Path(
    "data/raw/timetable/stagecoach_southeast_18511_extracted"
)


def local_name(tag: str) -> str:
    """Remove the XML namespace from a tag."""
    return tag.split("}")[-1]


def parse_duration_seconds(value: str) -> int | None:
    """Convert an ISO duration such as PT2M30S into seconds."""

    if not value:
        return None

    pattern = re.fullmatch(
        r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?",
        value.strip()
    )

    if not pattern:
        return None

    hours = int(pattern.group(1) or 0)
    minutes = int(pattern.group(2) or 0)
    seconds = int(pattern.group(3) or 0)

    return (hours * 3600) + (minutes * 60) + seconds


def find_direct_child(element, child_name: str):
    """Find a direct child using its local XML name."""

    for child in element:
        if local_name(child.tag) == child_name:
            return child

    return None


def main() -> None:
    xml_files = list(XML_FOLDER.rglob("*.xml"))

    vehicle_journeys = 0
    journeys_with_departure_time = 0
    vehicle_timing_links = 0

    positive_runtimes = 0
    zero_runtimes = 0
    missing_runtimes = 0
    invalid_runtimes = 0

    positive_examples: list[str] = []

    for xml_file in xml_files:
        try:
            root = ET.parse(xml_file).getroot()

            for element in root.iter():
                if local_name(element.tag) != "VehicleJourney":
                    continue

                vehicle_journeys += 1

                departure = find_direct_child(
                    element,
                    "DepartureTime"
                )

                if departure is not None and departure.text:
                    journeys_with_departure_time += 1

                for child in element.iter():
                    if local_name(child.tag) != "VehicleJourneyTimingLink":
                        continue

                    vehicle_timing_links += 1

                    runtime = find_direct_child(child, "RunTime")

                    if runtime is None or not runtime.text:
                        missing_runtimes += 1
                        continue

                    seconds = parse_duration_seconds(runtime.text)

                    if seconds is None:
                        invalid_runtimes += 1
                    elif seconds == 0:
                        zero_runtimes += 1
                    else:
                        positive_runtimes += 1

                        if len(positive_examples) < 5:
                            positive_examples.append(runtime.text.strip())

        except ET.ParseError as error:
            print(f"Could not parse {xml_file.name}: {error}")

    print("\n" + "=" * 60)
    print("VEHICLE JOURNEY RUNTIME PROFILE")
    print("=" * 60)
    print(f"XML files: {len(xml_files):,}")
    print(f"Vehicle journeys: {vehicle_journeys:,}")
    print(
        "Journeys with departure time: "
        f"{journeys_with_departure_time:,}"
    )
    print(f"Vehicle journey timing links: {vehicle_timing_links:,}")
    print(f"Positive runtimes: {positive_runtimes:,}")
    print(f"Zero runtimes: {zero_runtimes:,}")
    print(f"Missing runtimes: {missing_runtimes:,}")
    print(f"Invalid runtimes: {invalid_runtimes:,}")

    if positive_examples:
        print("\nExample positive runtimes:")
        for value in positive_examples:
            print(f"- {value}")

    print("=" * 60)


if __name__ == "__main__":
    main()