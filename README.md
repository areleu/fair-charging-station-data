[![DOI](https://zenodo.org/badge/705543008.svg)](https://zenodo.org/doi/10.5281/zenodo.10201023)
# FAIR Charging Station Data

This repository produces two clean datasets for Charging station data based on
the data provided by the german BNetzA. More info on the source can be found at
their [Charging Station Website
(German)](https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/start.html). Just in case it is not clear, no data is provided in this repository, you have to download it yourself. Run The download script once, it will throw an error on where to store the data to use this tool properly.

## Execution

This script should theoretically work with any version of python able to run pandas and frictionless. If it is not obvious, you have to install the requirements.txt in your python environment.

```bash
pip install -r requirements.txt
```

or simply

```bash
pip install pandas requests frictionless omi openpyxl jsonschema_rs
```

Each script has to be run with the directory where you want to have the data as current working directory. You run them with python normally, for example:

```bash
python src/clean.py
```
To get the proper data run the scripts in the following order:

0. load (Data has to be downloaded manually, sorry but the BNetzA website is not fond of automatic requests.)
1. clean
2. annotate
3. normalise
4. rename
5. evaluate
6. publish

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

## Renamed CSV

These files contain the output of the previous scripts but with column names translated to English and deprived of special characters.

## Caveats

The cleaning script will remove duplicate entries, this was not decided lightly as it can be the case that two columns are in the same place with the exact same characteristics. It is not possible, with our resources to validate or deny this, but these duplicate entries seem to be more of a input error than actual multiple columns with similar characteristics.

It is the case that columns with different characteristics share a place, these are kept.

## Data Sources


- [BNetzA Data](https://www.bundesnetzagentur.de/SharedDocs/Downloads/DE/Sachgebiete/Energie/Unternehmen_Institutionen/E_Mobilitaet/Ladesaeulenregister.xlsx) 
- [OEP Metadata schema](https://raw.githubusercontent.com/OpenEnergyPlatform/oemetadata/develop/metadata/latest/schema.json) used in evaluate.py
