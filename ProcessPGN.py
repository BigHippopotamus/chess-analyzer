# This module converts PGNs into a csv file

import sys
import chess.pgn
import re

from multiprocessing import Pool
from itertools import repeat

PROCESSES = 16
PER_PROCESS = 500000
CHECKPOINTS = 10


def map_result(result: str):
    if result == '1-0':
        return 'White'
    elif result == '0-1':
        return 'Black'
    else:
        return 'Draw'


def pgn_to_csv(filename, num):
    stdout_bak = sys.stdout

    with open(f'Games/games{num}.csv', 'w') as f, open(filename, 'r') as pgn:
        for _ in range(PER_PROCESS * num * 20):
            next(pgn)

        print(f'Process {num}: started', file=sys.stderr)

        sys.stdout = f

        for checkpoint in range(CHECKPOINTS):
            for i in range(PER_PROCESS // CHECKPOINTS):
                game = chess.pgn.read_game(pgn)
                headers = game.headers
                if '?' in [headers['White'], headers['Black']]:
                    continue

                game_pgn = str(game).splitlines()[-1]

                game_mate = 1 if headers['Termination'] == 'Normal' else 0

                print(headers['White'],
                      headers['Black'],
                      map_result(headers['Result']),
                      f'\"{headers["Opening"]}\"',
                      re.sub(r' +', ' ',
                          re.sub(r' { (\[%eval [^\]]*\])? *(\[%clk [^\]]*\])? }( \d+\.{3})?', ' ',
                                 f'\"{game_pgn}\"')),
                      game_mate,
                      sep=','
                      )
            print(f'Process {num}: {100 / CHECKPOINTS * (checkpoint + 1)}% finished', file=sys.stderr)

    sys.stdout = stdout_bak


if __name__ == '__main__':
    filename = input()
    process_number = [x for x in range(PROCESSES)]

    with Pool() as pool:
        out = pool.starmap(pgn_to_csv, zip(repeat(filename), process_number))

    print('FINISHED')
