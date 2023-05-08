# FAIR Charging Station Data

This repository produces two clean datasets for Charging station data based on
the data provided by the german BNetzA. More info on the source can be found at
their [Charging Station Website
(German)](https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/start.html)

## Annotated CSV

The source files are in xlsx, which is a limited format. The provider offers csv
files, but it has formatting errors as it seems that it is the output of using
excel directly to save as csv.

The annotate function of this repository will produce a clean dataset with
minimal modification of the source material.

## Normalised CSV

The normalised data contains the source material structured in such a way that
can be better handled with relational databases.
## RDF graph

TBA