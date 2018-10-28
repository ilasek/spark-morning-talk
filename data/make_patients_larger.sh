#!/usr/bin/env bash
head -n 1 patients.csv > patients_large.csv
tail -n +2 patients.csv{,}{,}{,}{,}{,}{,}{,} >> patients_large.csv
