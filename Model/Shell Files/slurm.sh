#!/bin/bash
#
#SBATCH --gres=gpu:1
#SBATCH --partition=disc_dual_a100_students,gpu,gpu_a100
#SBATCH --cpus-per-task=64
#SBATCH --mem=80G
#SBATCH --output=outputs/%j_stdout.txt
#SBATCH --error=outputs/%j_stderr.txt
#SBATCH --time=06:00:00
#SBATCH --job-name=hw8
#SBATCH --mail-user=brandondmichaud@ou.edu
#SBATCH --mail-type=ALL
#SBATCH --chdir=/home/NBA-Money-Lines

. /home/fagg/tf_setup.sh
conda activate tf
module load cuDNN/8.9.2.26-CUDA-12.2.0

python Model/base.py @Model/Arguments/exp.txt @Model/Arguments/model.txt