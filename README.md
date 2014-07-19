Turn SAS data files into .csv files
===================================

This code provides a wrapper to the Parso library of GGA
software (http://www.ggasoftware.com/opensource/parso).

The version tagged 1.0 will find all .sas7bdat files in the
named directory, create a new directory called 'csv' and
dump all the converted data into there. It fails if a
directory called csv already exists.

The way to call is
> unsas.sh $PWD

If it doesn't work, there are missing libraries and/or the
build path isn't correctly specified. I'm new to Java and
to configuring Eclipse for it, so it's been a bit hit and
miss.
