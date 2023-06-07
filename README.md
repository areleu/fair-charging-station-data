# FAIR Charging Station Data

This repository produces two clean datasets for Charging station data based on
the data provided by the german BNetzA. More info on the source can be found at
their [Charging Station Website
(German)](https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/start.html). Just in case it is not clear, no data is provided in this repository, you have to download it yourself. Run The download script once, it will throw an error on where to store the data to use this tool properly.

## Execution order

0. download
1. clean
2. annotate
3. normalise
4. evaluate

## Annotated CSV

The source files are in xlsx, which is a limited format. The provider offers csv
files, but it has formatting errors as it seems that it is the output of using
excel directly to save as csv.

The annotate function of this repository will produce a clean dataset with
minimal modification of the source material.

## Normalised CSV

The normalised data contains the source material structured in such a way that
can be better handled with relational databases. The charging stations and the
connection sockets are separated in two different tables.
