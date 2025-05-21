from pathlib import Path
import os

test = Path("/home/mark/Test/Test")
print(test)
for root, dirs, files in test.walk(top_down=False):
    for d in dirs:
        os.chmod((root / d), 0o777)
        (root / d).rmdir()
    for f in files:
        os.chmod((root / f), 0o777)
        (root / f).unlink()
test.rmdir()