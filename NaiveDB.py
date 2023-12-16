import csv
import os
from collections import defaultdict
from tabulate import tabulate
import heapq

class NaiveDB:
    def __init__(self):
        pass
        
    def create_table(self, table_name, columns):
        """
        The function creates a new CSV table with the given name and columns if it doesn't already
        exist.
        
        :param table_name: The name of the table you want to create. It should be a string value
        :param columns: The "columns" parameter is a list of column names for the table. Each element in
        the list represents a column name. For example, if you want to create a table with three columns
        named "Name", "Age", and "City", you would pass the following list as the "columns"
        :return: The function does not return anything.
        """
        csv_file = f"{table_name}.csv"
        if os.path.exists(csv_file):
            print(f"Table '{table_name}' already exists.")
            return
        with open(csv_file, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=columns)
            writer.writeheader()
        print(f"Table '{table_name}' created with columns: {columns}")

    def insert(self, table_name, values):
        """
        The `insert` function inserts a new record into a specified table, checking for duplicate IDs
        before insertion.
        
        :param table_name: The name of the table where the record will be inserted
        :param values: The 'values' parameter is a list of values representing the record to be inserted
        into the table. The first value in the list is assumed to be the ID of the record
        :return: The function does not return anything.
        """
        
        csv_file = f"{table_name}.csv"
        if not os.path.exists(csv_file):
            print(f"Table '{table_name}' does not exist.")
            return

        id_column = "id" 
        target_id = values[0]  

        if self.duplicateIDCheck(table_name, id_column, target_id):
            print(f"ID '{target_id}' already exists in the CSV file. Insertion prevented.")
            return

        with open(csv_file, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(values)

        print(f"Record inserted into table '{table_name}'.")
    
    def duplicateIDCheck(self, table_name, id_column, target_id):
        """
        Check if a given ID already exists in the specified table.

        Parameters:
        - table_name (str): The name of the table.
        - id_column (str): The name of the ID column.
        - target_id (str): The ID to check for duplicates.

        Returns:
        - bool: True if a duplicate ID is found, False otherwise.
        """
        csv_file = f"{table_name}.csv"
        if not os.path.exists(csv_file):
            return False  

        with open(csv_file, 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                if id_column in row and row[id_column] == target_id:
                    return True  
        return False  

    def read_csv_in_chunks(self, csv_file, chunk_size=500):
        """
        The function reads a CSV file in chunks and yields each chunk as a list of dictionaries
        representing rows in the file.
        
        :param csv_file: The path to the CSV file that you want to read in chunks
        :param chunk_size: The `chunk_size` parameter determines the number of rows to read in each
        chunk. In this case, the default value is set to 500, meaning that the CSV file will be read in
        chunks of 500 rows at a time, defaults to 500 (optional)
        :return: a generator object that yields chunks of rows from the CSV file. Each chunk is a list
        of dictionaries representing rows in the CSV file.
        """
       
        if not os.path.exists(csv_file):
            print(f"Table '{csv_file}' does not exist.")
            return
        with open(csv_file, 'r') as file:
            csv_reader = csv.DictReader(file)
            rows = []
            for row in csv_reader:
                rows.append(row)
                if len(rows) == chunk_size:
                    yield rows
                    rows = []
            if rows:
                yield rows

    def select(self, table_name, columns=None,chunk_size=500):
        """
        The `select` function selects and prints rows from a CSV table with optional column filtering.
        
        :param table_name: The name of the CSV table you want to select rows from
        :param columns: The `columns` parameter is a list of column names that you want to select from
        the CSV table. If this parameter is not provided or is set to `None`, all columns will be
        selected
        :param chunk_size: The `chunk_size` parameter determines the number of rows to print in each
        chunk. It is used to control the size of the output when printing the selected rows from the CSV
        table. By default, the `chunk_size` is set to 500, meaning that 500 rows will be printed at,
        defaults to 500 (optional)
        :return: The function does not return any value. It prints the selected rows from the CSV table.
        """
        
        csv_file = f"{table_name}.csv"
        if not os.path.exists(csv_file):
            print(f"Table '{table_name}' does not exist.")
            return

        with open(csv_file, 'r') as file:
            csv_reader = csv.DictReader(file)

            if columns:
                if not set(columns).issubset(csv_reader.fieldnames):
                    print("One or more specified columns do not exist in the table.")
                    return
                if columns:
                    print("\t".join(columns))

                rows = []
                for i, row in enumerate(csv_reader):
                    if not set(columns).issubset(row.keys()):
                        continue
                    values = [row[col] for col in columns]
                    rows.append("\t".join(values))

                    if (i + 1) % chunk_size == 0:
                        print("\n".join(rows))
                        rows = []

                if rows:
                    print("\n".join(rows))
            else:
                
                rows = []
                for i, row in enumerate(csv_reader):
                    rows.append("\t".join(row.values()))

                    if (i + 1) % chunk_size == 0:
                        print("\n".join(rows))
                        rows = []

                if rows:
                    print("\n".join(rows))

    # To perform querying on the ordered file select table name as ordered
    def order_by(self, table_name, order_column, output_file="ordered.csv", chunk_size=500):
        """
        The `order_by` function orders a CSV table by a specified column using external merge sort.
        
        :param table_name: The name of the CSV table that you want to order
        :param order_column: The `order_column` parameter is a string that specifies the column by which
        to order the table. The table will be sorted based on the values in this column
        :param output_file: The `output_file` parameter is a string that specifies the name of the
        output file where the ordered data will be stored. The default value is "ordered.csv", but you
        can provide a different file name if desired, defaults to ordered.csv (optional)
        :param chunk_size: The `chunk_size` parameter determines the number of rows to process in each
        chunk during the external merge sort. It specifies how many rows will be read from the input CSV
        file at a time and sorted before being written to temporary files. This parameter helps manage
        memory usage and performance during the sorting process, defaults to 500 (optional)
        :return: The function does not return any value. It prints messages and saves the ordered data
        to a specified output file.
        """
        csv_file = f"{table_name}.csv"
        if not os.path.exists(csv_file):
            print(f"Table '{table_name}' does not exist.")
            return

        temp_folder = "sorted_chunks"  
        os.makedirs(temp_folder, exist_ok=True)

        def int_or_original(x):
            try:
                return int(x)
            except (ValueError, TypeError):
                return str(x)

        try:
            # Phase 1: Divide and sort chunks
            chunk_count = 0
            fieldnames = None  
            temp_files = [] 

            with open(csv_file, 'r') as file:
                csv_reader = csv.DictReader(file)
                chunk = []
                for row in csv_reader:
                    if fieldnames is None:
                        fieldnames = csv_reader.fieldnames
                    chunk.append(row)

                    if len(chunk) >= chunk_size:
                        chunk.sort(key=lambda x: int_or_original(x.get(order_column)))
                        temp_file_name = os.path.join(temp_folder, f"sorted_chunk_{chunk_count}.csv")
                        with open(temp_file_name, 'w', newline='') as temp_file:
                            writer = csv.DictWriter(temp_file, fieldnames=fieldnames)
                            writer.writeheader()
                            writer.writerows(chunk)
                        temp_files.append(temp_file_name)
                        chunk_count += 1
                        chunk = []

                if chunk:
                    chunk.sort(key=lambda x: int_or_original(x.get(order_column)))
                    temp_file_name = os.path.join(temp_folder, f"sorted_chunk_{chunk_count}.csv")
                    with open(temp_file_name, 'w', newline='') as temp_file:
                        writer = csv.DictWriter(temp_file, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows(chunk)
                    temp_files.append(temp_file_name)

            # Phase 2: Merge sorted chunks using external merge sort
            temp_file_iters = [csv.DictReader(open(temp_file, 'r')) for temp_file in temp_files]

            # Merge sorted chunks into a list
            # merged_rows = list(heapq.merge(*temp_file_iters, key=lambda x: int_or_original(x[order_column])))

            # Clear the output file if it exists and write the ordered data
            if os.path.exists(output_file):
                os.remove(output_file)
            with open(output_file, 'w', newline='') as output_csv:
                output_writer = csv.DictWriter(output_csv, fieldnames=fieldnames)
                output_writer.writeheader()

                # Merge and write rows directly to the output file using on the fly execution
                for row in heapq.merge(*temp_file_iters, key=lambda x: int_or_original(x[order_column])):
                    output_writer.writerow(row)

            
            with open(output_file, 'r') as output_csv:
                output_reader = csv.DictReader(output_csv)
                for row in output_reader:
                    print(row)

            print(f"Data ordered by '{order_column}' and saved to {output_file}.")

        except Exception as e:
            print(f"An error occurred: {str(e)}")       

    def delete(self, table_name, condition):
        """
        The `delete` function deletes rows from a CSV table based on a specified condition by creating a
        new CSV file without the rows that match the condition and then replacing the original dataset
        with the new data.
        
        :param table_name: The `table_name` parameter is a string that represents the name of the CSV
        table from which rows will be deleted
        :param condition: The `condition` parameter is a function that takes a row as input and returns
        a boolean value indicating whether to delete the row. This function is used to determine which
        rows should be deleted from the CSV table
        :return: The `delete` method does not return anything.
        """
        csv_file = f"{table_name}.csv"
        if not os.path.exists(csv_file):
            print(f"Table '{table_name}' does not exist.")
            return

        new_csv_file = "output.csv"  

        for chunk in self.read_csv_in_chunks(csv_file, chunk_size=500):
            with open(new_csv_file, 'a', newline='') as file:
                columns = chunk[0].keys() if chunk else []  
                writer = csv.DictWriter(file, fieldnames=columns)
                if file.tell() == 0:  
                    writer.writeheader()

                for row in chunk:
                    if not condition(row):
                        writer.writerow(row)  
        with open(csv_file, 'w', newline='') as file:
            file.truncate(0)
        
        
        for chunk in self.read_csv_in_chunks(new_csv_file, chunk_size=500):
            with open(csv_file, 'a', newline='') as file:
                columns = chunk[0].keys() if chunk else []  
                writer = csv.DictWriter(file, fieldnames=columns)
                if file.tell() == 0:  
                    writer.writeheader()
                writer.writerows(chunk)        
        # Replace the original dataset with the new data
        os.remove(new_csv_file)

    def group_by(self, table_name, group_column, output_file="grouped.csv", chunk_size=500):
        """
        The `group_by` function groups rows from a CSV table based on a specified column and saves the
        grouped data to a new CSV file.
        
        :param table_name: The name of the CSV table that you want to group
        :param group_column: The `group_column` parameter is a string that specifies the column by which
        to group the data in the CSV table
        :param output_file: The `output_file` parameter is a string that specifies the name of the
        output file where the grouped data will be saved. The default value is "output1.csv", but you
        can provide a different name if desired, defaults to output1.csv (optional)
        :param chunk_size: The `chunk_size` parameter determines the number of rows to process in each
        chunk. It is used to divide the data into smaller portions for more efficient processing. In
        this case, each chunk contains `chunk_size` number of rows from the CSV table, defaults to 500
        (optional)
        :return: The function does not return any value. It prints the grouped data and a message
        indicating that the grouped data has been saved to the specified output file.
        """
        csv_file = f"{table_name}.csv"
        if not os.path.exists(csv_file):
            print(f"Table '{table_name}' does not exist.")
            return

        temp_folder = "grouped_chunks"
        os.makedirs(temp_folder, exist_ok=True)  

        grouped_chunks = defaultdict(list)

        try:
            with open(csv_file, 'r') as file:
                csv_reader = csv.DictReader(file)
                current_chunk = []

                for row in csv_reader:
                    # Normalize the group column value to ensure consistent grouping
                    group_value = row[group_column].strip()
                    row[group_column] = group_value
                    current_chunk.append(row)

                    if len(current_chunk) >= chunk_size:
                        for row in current_chunk:
                            key = row[group_column]
                            grouped_chunks[key].append(row)
                        current_chunk = []

                for row in current_chunk:
                    key = row[group_column]
                    grouped_chunks[key].append(row)

            sorted_files = []

            for group_key, group_rows in grouped_chunks.items():
                # Sort the chunk by the group key
                group_rows.sort(key=lambda x: x[group_column])

                temp_file_name = os.path.join(temp_folder, f"temp_{group_key}.csv")
                with open(temp_file_name, 'w', newline='') as temp_file:
                    writer = csv.DictWriter(temp_file, fieldnames=group_rows[0].keys())
                    writer.writeheader()
                    writer.writerows(group_rows)

                sorted_files.append(temp_file_name)

            with open(output_file, 'w', newline='') as output_csv:
                output_writer = csv.DictWriter(output_csv, fieldnames=grouped_chunks[next(iter(grouped_chunks))][0].keys())
                output_writer.writeheader()

                for temp_file_name in sorted_files:
                    with open(temp_file_name, 'r') as temp_file:
                        csv_reader = csv.DictReader(temp_file)
                        for row in csv_reader:
                            output_writer.writerow(row)
            with open(output_file, 'r') as output_csv:
                output_reader = csv.DictReader(output_csv)
                for row in output_reader:
                    print(row)

            print(f"Grouped data saved to {output_file}.")

        except Exception as e:
            print(f"An error occurred: {str(e)}")

    def filter(self, table_name, column_name, condition_value,condition_type):
        """
        The function filters rows from a CSV table based on a specified condition and displays the
        filtered data using PrettyTable.
        
        :param table_name: The name of the CSV table you want to filter rows from
        :param column_name: The column by which to filter the data. This is a string value representing
        the name of the column in the CSV table
        :param condition_value: The `condition_value` parameter is the value that you want to filter
        against. It is the value that will be compared to the values in the specified column to
        determine which rows to include in the filtered data
        :param condition_type: The `condition_type` parameter specifies the type of condition to apply
        when filtering the data. It can have one of the following values:
        :return: The function does not return any value.
        """
        csv_file = f"{table_name}.csv"
        if not os.path.exists(csv_file):
            print(f"Table '{table_name}' does not exist.")
            return
        new_csv_file = "filtered.csv" 
        # filter_condition = lambda row: row.get(column_name) == condition_value
        if condition_type == "equalTo":
            filter_condition = lambda row: row.get(column_name) == condition_value
        elif condition_type == "smallerThan":
            filter_condition = lambda row: self.compare_values(row.get(column_name), condition_value, reverse=False)
        elif condition_type == "biggerThan":
            filter_condition = lambda row: self.compare_values(row.get(column_name), condition_value, reverse=True)
        else:
            print(f"Invalid condition type: {condition_type}")
            return
        if os.path.exists(new_csv_file):
            os.remove(new_csv_file)
        for chunk in self.read_csv_in_chunks(csv_file, chunk_size=500):
            with open(new_csv_file, 'a', newline='') as file:
                columns = chunk[0].keys() if chunk else []  # Use the columns from the last chunk, if available
                writer = csv.DictWriter(file, fieldnames=columns)
                if file.tell() == 0:  # Check if it's a new file and write headers
                    writer.writeheader()

                for row in chunk:
                    if filter_condition(row):
                        writer.writerow(row)  # Write each row to the new file if the condition is not met
        
       
        with open(new_csv_file, 'r') as output_csv:
                output_reader = csv.DictReader(output_csv)
                for row in output_reader:
                    print(row)
        # os.remove(new_csv_file)
    
    def compare_values(self, value1, value2, reverse=False):
        """
        Compare two values, handling cases where conversion to int is not possible.

        Parameters:
        - value1: The first value.
        - value2: The second value.
        - reverse (bool): If True, reverse the comparison result.

        Returns:
        - bool: True if the comparison holds, False otherwise.
        """
        try:
            int_value1 = int(value1)
            int_value2 = int(value2)
            result = int_value1 < int_value2
        except ValueError:
            # If conversion to int is not possible, fall back to string comparison
            result = str(value1) < str(value2)

        return not result if reverse else result
   
    def updateTable(self, table_name, condition_col, condition_val, update_col, update_val, chunk_size=500):
        """
        The `updateTable` function updates rows in a CSV table based on a specified condition, using a
        temporary output file for the update process and replacing the original CSV file.
        
        :param table_name: The name of the CSV table that you want to update
        :param condition_col: The `condition_col` parameter is a string that represents the column in
        the CSV table that will be used as the condition for the update operation. This column will be
        checked to determine if a row should be updated or not
        :param condition_val: The `condition_val` parameter is the value that you want to match in the
        `condition_col` column. It is used to determine which rows should be updated in the CSV table
        :param update_col: The `update_col` parameter is a string that represents the column in the CSV
        table that you want to update
        :param update_val: The `update_val` parameter is the new value that you want to set in the
        `update_col` column for the rows that match the specified condition
        :param chunk_size: The `chunk_size` parameter determines the number of rows to process in each
        chunk. It is used to divide the rows in the CSV file into smaller chunks for processing. By
        default, the `chunk_size` is set to 500, meaning that 500 rows will be processed at a time,
        defaults to 500 (optional)
        :return: The function does not return any value.
        """
        
        csv_file = f"{table_name}.csv"
        if not os.path.exists(csv_file):
            print(f"Table '{table_name}' does not exist.")
            return
        
        id_column = "id"  
        if id_column == condition_col:
            if self.duplicateIDCheck(table_name, id_column, update_val):
                print(f"ID '{update_val}' already exists in the CSV file. Update prevented.")
                return

        # Create a temporary output file for updates
        temp_output_file = "temp_updated.csv"

        # Initialize variables for tracking the number of rows processed and updated
        rows_processed = 0
        rows_updated = 0

        with open(csv_file, 'r') as file, open(temp_output_file, 'w', newline='') as temp_output_csv:
            csv_reader = csv.DictReader(file)
            fieldnames = csv_reader.fieldnames
            csv_writer = csv.DictWriter(temp_output_csv, fieldnames=fieldnames)
            csv_writer.writeheader()

            current_chunk = []

            for row in csv_reader:
                # Check the condition and update the row if it matches
                if row[condition_col] == condition_val:
                    row[update_col] = update_val
                    rows_updated += 1

                current_chunk.append(row)

                if len(current_chunk) >= chunk_size:
                    csv_writer.writerows(current_chunk)
                    current_chunk = []

                rows_processed += 1

            # Write any remaining rows in the last chunk
            csv_writer.writerows(current_chunk)

        # Rename the temp output file to the original CSV file to replace it
        os.remove(csv_file)
        os.rename(temp_output_file, csv_file)

        # Print a summary of the update operation
        print(f"Updated {rows_updated} out of {rows_processed} rows in '{table_name}.csv'.")
        
    def join(self, first_table, table2_name, join_column_table1, join_column_table2, chunk_size=500):
        """
        The `join` function takes two CSV tables, joins them based on specified join columns, and prints
        the combined rows.
        
        :param first_table: The name of the first table (CSV file) that you want to join with the second
        table
        :param table2_name: The name of the second table (CSV file) that you want to join with the first
        table
        :param join_column_table1: The join column in the first table. This is the column that will be
        used to match rows between the two tables
        :param join_column_table2: The join_column_table2 parameter is the name of the join column in
        the second table (CSV file). It is used to match the values in this column with the values in
        the join_column_table1 column of the first table
        :param chunk_size: The `chunk_size` parameter determines the number of rows to process in each
        chunk. It specifies how many rows from the first table will be read and processed at a time
        before moving on to the next chunk. The default value is 500, but you can change it to any
        integer value that suits, defaults to 500 (optional)
        :return: The function does not return anything.
        """
        if not os.path.exists(first_table) or not os.path.exists(table2_name):
            print(f"One or more tables does not exist.")
            return
        if os.path.exists('joined.csv'):
            os.remove('joined.csv')

        # Read the data from the first CSV file and store it in chunks
        with open(first_table, 'r') as table1_file:
            csv_reader = csv.DictReader(table1_file)
            current_chunk = []

            for row in csv_reader:
                current_chunk.append(row)

                # If the current chunk size reaches the specified limit
                if len(current_chunk) >= chunk_size:
                    # Process the chunk
                    self.process_chunk(current_chunk, table2_name, join_column_table2)
                    current_chunk = []  # Clear the current chunk

            # Process the last chunk (if not empty)
            if current_chunk:
                self.process_chunk(current_chunk, table2_name, join_column_table2)
        print(f"Combined rows saved to {'joined.csv'}.")

    def process_chunk(self, chunk, table2_name, join_column_table2):
        joined_csv_file = "joined.csv"

        with open(table2_name, 'r') as table2_file:
            csv_reader = csv.DictReader(table2_file)

            row2_first = next(csv_reader)

            with open(joined_csv_file, 'a', newline='') as joined_csv:
                # Check if the file is empty and write the header if needed
                if joined_csv.tell() == 0:
                    fieldnames = [f"table1_{col}" for col in chunk[0].keys()] + [f"table2_{col}" for col in row2_first.keys()]
                    csv_writer = csv.DictWriter(joined_csv, fieldnames=fieldnames)
                    csv_writer.writeheader()

                # Reset the reader to the beginning of table2
                table2_file.seek(0)
                next(csv_reader)  # Skip the header

                for row2 in csv_reader:
                    key = row2[join_column_table2]
                    for row1 in chunk:
                        if key == row1[join_column_table1]:
                            # Create combined row with aliases for common columns
                            combined_row = {}
                            for col1, value1 in row1.items():
                                combined_row[f"table1_{col1}"] = value1
                            for col2, value2 in row2.items():
                                combined_row[f"table2_{col2}"] = value2
                            csv_writer.writerow(combined_row)

                            print(combined_row)

            

    def aggregate(self,csv_file, column_name, operation, chunk_size=500):
        """
        The `aggregate` function aggregates data from a specified column in a CSV file based on the
        specified operation and prints the result.
        
        :param csv_file: The name of the CSV file that contains the data to be aggregated
        :param column_name: The name of the column in the CSV file that you want to aggregate
        :param operation: The "operation" parameter specifies the aggregation operation to perform on
        the data in the specified column. It can take one of the following values:
        :param chunk_size: The `chunk_size` parameter determines the number of rows to process in each
        chunk. It is an optional parameter with a default value of 500. This means that if the parameter
        is not provided when calling the `aggregate` function, it will default to processing 500 rows at
        a time, defaults to 500 (optional)
        :return: The function does not return any value. It prints the result of the aggregation
        operation.
        """
        csv_file = f"{csv_file}.csv"
        if not os.path.exists(csv_file):
            print(f"Table '{csv_file}' does not exist.")
            return
        try:
            operation = operation.lower()
            if operation not in ('average', 'sum', 'minimum', 'maximum'):
                print("Invalid operation. Please choose from 'average', 'sum', 'minimum', or 'maximum'.")
                return

            result = None
            row_count = 0

            with open(csv_file, 'r') as file:
                reader = csv.DictReader(file)

                for row in reader:
                    if column_name in row:
                        value = int(row[column_name])

                        if operation == 'average':
                            result = result + value if result is not None else value
                        elif operation == 'sum':
                            result = result + value if result is not None else value
                        elif operation == 'minimum':
                            result = min(result, value) if result is not None else value
                        elif operation == 'maximum':
                            result = max(result, value) if result is not None else value

                    row_count += 1                                                    

            if result is not None:
                if operation == 'average':
                    result = result / row_count
                print(f"{operation.capitalize()} of {column_name}: {result}")
            else:
                print(f"No data found in the '{column_name}' column of the CSV file.")

        except FileNotFoundError:
            print(f"CSV file '{csv_file}' not found.")
        except ValueError:
            print(f"Error: Could not convert values in the '{column_name}' column to integers.")
  
if __name__ == "__main__":
    emulator = NaiveDB()

    while True:
        print("\nEnter your query in the following format:")
        print("1. CREATE TABLE: newTable|table_name|column1,column2,...")
        print("2. INSERT: addToTable|table_name|col1_val,col2_val,...")
        print("3. SELECT: showColumns|table_name|col1,col2,... or showColumns|table_name|all")
        print("4. ORDER BY: sort|table_name|col_name")
        print("5. UPDATE: set|table_name|col_name|cond_val|update_col|new_val")
        print("6. DELETE: remove|table_name|col_name|col_value")
        print("7. GROUP BY: formGroups|table_name|col_name")
        print("8. FILTER: filter|table_name|col_name|col_value|condition(smallerThan/biggerThan/equalTo)")
        print("9. JOIN: getCommon|table1_name|table2_name|col_name_table1|col_name_table2")
        print("10. AGGREGATE: aggregate|table_name|col_name|operation(maximum/minimum/average/sum)")
        print("11. Type bye to exit")

        query = input("NaiveDB > ")

        parts = query.split("|")
        

        command = parts[0]
        if command == "newTable":
            table_name = parts[1]
            columns = parts[2].split(',')
            emulator.create_table(table_name, columns)
            # print(f"Table '{table_name}' created with columns: {columns}")

        elif command == "addToTable":
            table_name = parts[1]
            values = parts[2].split(',')
            emulator.insert(table_name, values)

        elif command == "showColumns":
            table_name = parts[1]
            columns = parts[2].split(',') if parts[2] != 'all' else []
            emulator.select(table_name, columns)

        elif command == "sort":
            table_name = parts[1]
            column = parts[2]
            emulator.order_by(table_name, column)

        elif command == "set":
            table_name = parts[1]
            condition_col = parts[2]
            condition_val = parts[3]
            update_col = parts[4]
            update_val = parts[5]
            emulator.updateTable(table_name, condition_col, condition_val, update_col, update_val)
            print("Update complete.")

        elif command == "remove":
            table_name = parts[1]
            condition_col = parts[2]
            condition_val = parts[3]
            delete_condition = lambda row: row.get(condition_col) == condition_val
            emulator.delete(table_name, delete_condition)
            print("Delete complete.")

        elif command == "formGroups":
            table_name = parts[1]
            column_name = parts[2]
            emulator.group_by(table_name, column_name)

        elif command == "filter":
            table_name = parts[1]
            column_name = parts[2]
            condition_value = parts[3]
            condition_type = parts[4]
            emulator.filter(table_name, column_name, condition_value, condition_type)

        elif command == "getCommon":
            first_table = parts[1]
            table2_name = parts[2]
            join_column_table1 = parts[3]
            join_column_table2 = parts[4]
            table1 = f"{first_table}.csv"
            table2 = f"{table2_name}.csv"
            emulator.join(table1, table2, join_column_table1, join_column_table2)

        elif command == "aggregate":
            table_name = parts[1]
            column_name = parts[2]
            operation = parts[3]
            emulator.aggregate(table_name, column_name, operation)

        elif command.lower() == "bye":
            break
        

        else:
            print("Invalid command. Please try again.")

