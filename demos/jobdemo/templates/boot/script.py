#!/usr/bin/env python3

import json
import time


def main():
    """Main program of job."""
    print("loading kwargs file")
    with open("kwargs.json") as f:
        kwargs = json.load(f)

    print("looping")
    configs = []
    for i in range(kwargs["boot_size"]):
        fn_out = f"config_{i:03d}.txt"
        with open(fn_out, "w") as f:
            f.write(f"Bla bla {i}\n")
        configs.append(fn_out)

    time.sleep(kwargs["pause"])

    print("writing result file")
    with open("result.json", "w") as f:
        json.dump(configs, f)


if __name__ == "__main__":
    main()
