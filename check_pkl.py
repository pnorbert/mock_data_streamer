#!/usr/bin/env python3
import argparse
import pickle
import warnings


warnings.filterwarnings(
    "ignore",
    message=r"numpy\.core\.numeric is deprecated",
    category=DeprecationWarning,
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Print the contents of a consumer pickle stream."
    )
    parser.add_argument(
        "pickle_file",
        nargs="?",
        default="received.pkl",
        help="pickle stream to inspect (default: received.pkl)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    step = 0

    with open(args.pickle_file, "rb") as stream:
        while True:
            try:
                variables = pickle.load(stream)
            except EOFError:
                break

            if not isinstance(variables, dict):
                raise TypeError(
                    f"Step {step} is {type(variables).__name__}, not a dictionary"
                )

            print(f"Step {step}")
            print(f"  keys: {list(variables)}")
            print("  arrays:")
            for name, value in variables.items():
                size = getattr(value, "size", 1)
                shape = getattr(value, "shape", None)
                if shape == ():
                    print(f"    {name}: value={value.item()}, scalar")
                    continue
                length = len(value)
                print(f"    {name}: len={length}, size={size}, shape={shape}")
            step += 1

    print(f"Total steps: {step}")


if __name__ == "__main__":
    main()
