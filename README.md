Turn SAS data files into .csv files
===================================

This code provides a wrapper to the Parso library of GGA
software (https://github.com/epam/parso).

The version tagged 1.0 will find all .sas7bdat files in the
named directory, create a new directory called 'csv' and
dump all the converted data into there. It fails if a
directory called csv already exists. This version works with
an old, pre-GitHub, version of Parso!

The way to call is
> unsas.sh $PWD

If it doesn't work, there are missing libraries and/or the
build path isn't correctly specified. I'm new to Java and
to configuring Eclipse for it, so it's been a bit hit and
miss.

I'm grateful to the nobel and enlightened Dr Matt Salts for
all his help with this.
