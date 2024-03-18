@echo off
set INPUT_DIR=C:\path\to\your\input\directory
set OUTPUT_DIR=C:\path\to\your\output\directory

python convert_to_parquet.py "%INPUT_DIR%" "%OUTPUT_DIR%"
pause
