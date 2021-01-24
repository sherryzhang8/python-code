{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "#!/bin/python3\n",
    "\n",
    "import math\n",
    "import os\n",
    "import random\n",
    "import re\n",
    "import sys\n",
    "\n",
    "#\n",
    "# Complete the 'countingValleys' function below.\n",
    "#\n",
    "# The function is expected to return an INTEGER.\n",
    "# The function accepts following parameters:\n",
    "#  1. INTEGER steps\n",
    "#  2. STRING path\n",
    "#\n",
    "\n",
    "def countingValleys(steps, path):\n",
    "    # Write your code here\n",
    "    valley = 0\n",
    "    sea_level = 0\n",
    "    is_valley = False\n",
    "    for i in range(steps):\n",
    "        if path[i] == 'D':\n",
    "            if sea_level == 0:\n",
    "                is_valley = True\n",
    "            sea_level -= 1\n",
    "        elif path[i] == 'U':\n",
    "            sea_level += 1\n",
    "            if sea_level == 0 and is_valley:\n",
    "                valley += 1\n",
    "    return valley\n",
    "\n",
    "if __name__ == '__main__':\n",
    "    fptr = open(os.environ['OUTPUT_PATH'], 'w')\n",
    "\n",
    "    steps = int(input().strip())\n",
    "\n",
    "    path = input()\n",
    "\n",
    "    result = countingValleys(steps, path)\n",
    "\n",
    "    fptr.write(str(result) + '\\n')\n",
    "\n",
    "    fptr.close()"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.8.3"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
