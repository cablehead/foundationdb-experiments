import subprocess
import shlex


def test_run():
    args = shlex.split('curl -s https://www.nasdaq.com/symbol/tsla')
    p = subprocess.Popen(args, stdout=subprocess.PIPE)
    while True:
        s = p.stdout.read(100)
        if not s:
            break
    assert p.wait() == 0
