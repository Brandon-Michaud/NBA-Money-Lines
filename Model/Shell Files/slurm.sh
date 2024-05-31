#!/bin/bash
#
#SBATCH --partition=disc_dual_a100_students,gpu,gpu_a100,normal
#SBATCH --cpus-per-task=64
#SBATCH --mem=80G
#SBATCH --output=Outputs/o_%j_stdout.txt
#SBATCH --error=Outputs/o_%j_stderr.txt
#SBATCH --time=00:30:00
#SBATCH --job-name=hw8
#SBATCH --mail-user=brandondmichaud@ou.edu
#SBATCH --mail-type=ALL
#SBATCH --chdir=/home/cs504319/NBA-Money-Lines

. /home/fagg/tf_setup.sh
conda activate dnn_2024_02

python Model/base.py @Model/Arguments/exp.txt @Model/Arguments/model.txt