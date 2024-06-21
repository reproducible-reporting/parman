# Parman extends Python concurrent.futures to facilitate parallel workflows.
# Copyright (C) 2023 Toon Verstraelen
#
# This file is part of Parman.
#
# Parman is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 3
# of the License, or (at your option) any later version.
#
# Parman is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, see <http://www.gnu.org/licenses/>
#
# --
"""Remove all jobs in a results directory that have no result.json."""

import argparse
import shutil
from pathlib import Path


def main():
    """Main program."""
    args = parse_args()
    root = Path(args.results)
    to_remove = []
    for fn_kwargs in root.glob("**/kwargs.json"):
        jobdir = fn_kwargs.parent
        fn_result = jobdir / "result.json"
        if not fn_result.exists():
            to_remove.append(jobdir)
    for jobdir in to_remove:
        print(f"Removing {jobdir}")
        if not args.dry_run and jobdir.exists():
            shutil.rmtree(jobdir)


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        prog="parman-clean-results",
        description="Remove results directories lacking a result.json file.",
    )
    parser.add_argument("results", help="The results directory.")
    parser.add_argument("-n", "--dry-run", default=False, action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    main()
