#!/usr/bin/env python3

import json
import time
from pathlib import Path


def main():
    """Main program of job."""
    print("loading kwargs file")
    with open("kwargs.json") as f:
        kwargs = json.load(f)

    print("loading models")
    for model in kwargs["models"]:
        # Just checking the presence of the file.
        # A Realistic example would load and use it.
        if not Path(model).is_file():
            raise AssertionError(f"Path {model} is not a file.")

    print("sampling new configurations")
    configs = []
    for i in range(kwargs["sample_size"]):
        fn_out = f"config_{i:03d}.txt"
        with open(fn_out, "w") as f:
            f.write(f"Bla bla {i} at temperature {kwargs['temperature']}\n")
        configs.append(fn_out)

    time.sleep(kwargs["pause"])

    print("writing result file")
    with open("result.json", "w") as f:
        json.dump(configs, f)


if __name__ == "__main__":
    main()
