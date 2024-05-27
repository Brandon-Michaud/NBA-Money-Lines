#!/bin/bash
for rotation in {0..9}
do
    python Model/base.py @Model/Arguments/exp.txt @Model/Arguments/model.txt --rotation $rotation
done
