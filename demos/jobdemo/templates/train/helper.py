#!/usr/bin/env python3
"""Example of a helper script for the train job."""

import json
import time


def main():
    """Main program of helper script for job."""
    print("loading kwargs file")
    with open("kwargs.json") as f:
        kwargs = json.load(f)

    print("training model with seed", kwargs["seed"])
    data = []
    for fn in kwargs["examples"]:
        print("Reading", fn)
        with open(fn) as f:
            data.append(f.read())
    with open("model.json", "w") as f:
        json.dump(data, f)

    time.sleep(kwargs["pause"])

    print("writing result file")
    with open("result.json", "w") as f:
        json.dump("model.json", f)


if __name__ == "__main__":
    main()
