from fetch import fetchStations, AirQualitySchema
from pydantic import ValidationError


def test_fetch_stations():
    rows = fetchStations()

    assert isinstance(rows, list), f"Expected list, got {type(rows).__name__}"
    assert len(rows) > 0, "No stations returned"

    print(f"Total stations: {len(rows)}")

    first = rows[0]
    print(f"Sample keys : {list(first.keys())}")

    errors = []
    for i, row in enumerate(rows):
        try:
            AirQualitySchema(**row)
        except ValidationError as e:
            errors.append((i, e))

    valid = len(rows) - len(errors)
    print(f"Valid rows  : {valid}/{len(rows)}")

    if errors:
        print(f"\nFirst 3 validation errors:")
        for idx, err in errors[:3]:
            print(f"  Row {idx}: {err.error_count()} issue(s)")
            for e in err.errors():
                print(f"    - {e['loc']}: {e['msg']}")

    print(f"\nSample record:")
    sample = AirQualitySchema(**rows[0]) if not errors else AirQualitySchema(**rows[valid - 1])
    for field, value in sample.model_dump().items():
        print(f"  {field:25s}: {value}")


if __name__ == "__main__":
    test_fetch_stations()
    print("\nAll checks passed.")
