#!/usr/bin/env python3
import argparse
import json
import logging

logging.basicConfig(filename='main.log', level=logging.DEBUG)


PARSER = argparse.ArgumentParser()
PARSER.add_argument('-DU', '--DailyUpdate', action='store_true',  default=False,
                    help='Daily Update based on RESA new files')

def main(args):

    if args.DailyUpdate:
        from src.updaters.daily_updater.main import main as daily_updater
        daily_updater()
    else:
        required = ['DailyUpdate']
        raise RuntimeError(
            f"One of the following flags is required: "
            f"{', '.join('--' + r for r in required)} "
        )

if __name__ == '__main__':
    args = PARSER.parse_args()
    main(args)