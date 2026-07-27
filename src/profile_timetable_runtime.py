from pathlib import Path
import xml.etree.ElementTree as ET
from datetime import timedelta


XML_FOLDER = Path(
    "data/raw/timetable/stagecoach_southeast_18511_extracted"
)


def parse_duration_seconds(value: str) -> int | None:
    """Convert an ISO duration such as PT2M30S into seconds."""

    if not value or not value.startswith("PT"):
        return None

    value = value[2:]

    hours = 0
    minutes = 0
    seconds = 0

    try:
        if "H" in value:
            hour_text, value = value.split("H", 1)
            hours = int(hour_text)

        if "M" in value:
            minute_text, value = value.split("M", 1)
            minutes = int(minute_text)

        if "S" in value:
            second_text = value.replace("S", "")
            seconds = int(second_text)

        return int(
            timedelta(
                hours=hours,
                minutes=minutes,
                seconds=seconds
            ).total_seconds()
        )

    except ValueError:
        return None


def main() -> None:
    xml_files = list(XML_FOLDER.rglob("*.xml"))

    total_links = 0
    missing_runtime = 0
    zero_runtime = 0
    positive_runtime = 0
    invalid_runtime = 0

    positive_values: list[int] = []

    for xml_file in xml_files:
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()

            for element in root.iter():
                if element.tag.endswith("JourneyPatternTimingLink"):
                    total_links += 1

                    runtime_element = next(
                        (
                            child
                            for child in element
                            if child.tag.endswith("RunTime")
                        ),
                        None,
                    )

                    if runtime_element is None or not runtime_element.text:
                        missing_runtime += 1
                        continue

                    runtime_seconds = parse_duration_seconds(
                        runtime_element.text.strip()
                    )

                    if runtime_seconds is None:
                        invalid_runtime += 1
                    elif runtime_seconds == 0:
                        zero_runtime += 1
                    else:
                        positive_runtime += 1
                        positive_values.append(runtime_seconds)

        except ET.ParseError as error:
            print(f"Could not parse {xml_file.name}: {error}")

    print("\n" + "=" * 55)
    print("TIMETABLE RUNTIME PROFILE")
    print("=" * 55)
    print(f"XML files: {len(xml_files):,}")
    print(f"Total timing links: {total_links:,}")
    print(f"Positive runtimes: {positive_runtime:,}")
    print(f"Zero runtimes: {zero_runtime:,}")
    print(f"Missing runtimes: {missing_runtime:,}")
    print(f"Invalid runtimes: {invalid_runtime:,}")

    if positive_values:
        print(f"Minimum positive runtime: {min(positive_values)} seconds")
        print(f"Maximum positive runtime: {max(positive_values)} seconds")

    print("=" * 55)


if __name__ == "__main__":
    main()