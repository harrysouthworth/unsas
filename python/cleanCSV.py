import sys
import os

def main(pwd):
  print(pwd)
  infiles = os.listdir(pwd)

  os.chdir(pwd)

  print("Replacing any quoted newlines in:")
  for file in infiles:
    ex = os.path.splitext(file)[1][1:].strip()
    if ex == "csv":
      print("  " + file)
      stripQuotedNewlines(file)


def stripQuotedNewlines(infile):
  # Replace newlines that appear between double-quotes with single spaces
  import csv
  with open(infile, "r") as input, open(infile + ".cleaned.csv", "w") as output:
    w = csv.writer(output)
    for record in csv.reader(input):
      w.writerow(tuple(s.replace("\n", " ") for s in record))
    input.close()
    output.close()
    os.rename(infile + ".cleaned.csv", infile)

main(sys.argv[1])

