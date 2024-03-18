# csv_parquet

# Large CSV to Parquet Converter and Merger

This repository contains Python scripts designed to efficiently handle the conversion of large CSV files (over 50GB) to Parquet format and to merge these Parquet files for optimized storage and faster query performance. These tools are particularly useful for managing and analyzing big data sets with tools like Pandas in Python.

## Features

- **Convert CSV to Parquet**: Convert large CSV files to a more efficient Parquet format, reducing file size and improving read/write performance.
- **Merge Parquet Files**: Combine multiple Parquet files into a single file, organized by subdirectories named according to your specification.

## Prerequisites

Before you begin, ensure you have met the following requirements:

- Python 3.9 or later installed on your system.
- Pandas library installed. This can be done via pip with `pip install pandas`.

## Installation

To use these scripts, follow these steps:

1. Clone the repository to your local machine:
   ```
   git clone https://github.com/ahad1361/csv_parquet.git
   ```
2. Navigate to the cloned directory:
   ```
   cd csv_parque
   ```

## Usage

The repository includes `.bat` files for Windows users and `.sh` files for Unix/Linux/Mac users to facilitate easy usage. Follow the instructions below corresponding to your operating system.

### For Windows

1. Open the `.bat` file you wish to use (`convert_to_parquet.bat` for conversion, `merge_parquet.bat` for merging) in a text editor.
2. Edit the `INPUT_DIR` and `OUTPUT_DIR` variables to specify your input directory (where your CSV files are located) and your output directory (where you want your Parquet files to be saved), respectively.
3. Save the file and double-click it to run.

### For Unix/Linux/Mac

1. Open the terminal.
2. Make the script executable with `chmod +x convert_to_parquet.sh` or `chmod +x merge_parquet.sh`.
3. Edit the `INPUT_DIR` and `OUTPUT_DIR` variables in your chosen `.sh` file to specify your input and output directories.
4. Run the script with `./convert_to_parquet.sh` or `./merge_parquet.sh`.

## Testing

This project includes a comprehensive set of unit tests to ensure that the conversion and merging functionalities work as expected. The tests are located in the `test_file_conversion.py` file.

Before running the tests, ensure you have all the necessary requirements installed by running:

```
pip install pandas tqdm
```

To run the tests, navigate to the project directory and execute the following command:

### For Windows:
```
python -m unittest test_file_conversion.py
```

### For Unix/Linux/Mac:
If you're using a Unix-like operating system, the command remains the same:
```
python3 -m unittest test_file_conversion.py
```

This command will automatically discover and run all the tests defined in `test_file_conversion.py`, and you will see the output indicating whether the tests passed or if there were any failures.

Running these tests is a crucial step in ensuring the reliability and correctness of the scripts before using them in a production environment, especially when working with large datasets.




## Contributing

Contributions are what make the open-source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make these tools better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement". Don't forget to give the project a star! Thanks again!

## License

Distributed under the MIT License. See `LICENSE` for more information.


