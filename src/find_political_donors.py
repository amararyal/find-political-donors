import io
import math
import datetime
import sys
import time
from collections import defaultdict

# Declare two lists to hold the relevant data for the two problem
donor_data = []
median_by_zip_data = []

#Declare two dictionaries for the purpose of grouping
group_data_by_CMTE_ID_and_date = defaultdict(list)
group_data_by_CMTE_ID_and_zip_code = defaultdict(list)

# Methods
# Method to compute Median
def find_median(data):
    data = sorted(data)
    length = len(data)
    if length == 1:
        return data[0]
    elif length % 2 == 0:
        return round((float(data[int(length / 2)]) + float(data[int(length / 2) - 1])) / 2)
    else:
        return data[int(length / 2)]


# Method to check whether the date is valid or not
def validate_date(date_text):
    correct_date = None
    try:
        new_date = datetime.datetime.strptime(date_text, '%m%d%Y')
        correct_date = True
    except ValueError:
        correct_date = False
    return correct_date


# Method to convert string date to  date object
def convert_to_date_object(date_text):
    try:
        new_date = datetime.datetime.strptime(date_text, '%m%d%Y')
    except ValueError:
        raise ValueError('verify the date before conversion')
    return new_date


# Method to convert the date object to string date
def convert_date_object_to_string(date_object):
    try:
        new_date = date_object.strftime('%m%d%Y')
    except ValueError:
        raise ValueError('verify the date before conversion')
    return new_date


# Method to process the Data (This method is called from main)
def process_data(input_file_name, output_filename_for_median_value_by_zip, output_filename_for_median_value_by_time):
# Open files to write output
    file1 = open(output_filename_for_median_value_by_zip, "w")
    file2 = open(output_filename_for_median_value_by_time, "w")

# Reading Input sequentially to simulate streaming
    i = 0
    with io.open(input_file_name, \
                 'r', encoding='latin-1') as infile:

        for line in infile:  # looping through the lines
            filtered_data = []
            splited = line.split('|')  # Split the data
            i = i + 1
            if i == 10000:
                break
            # If the 'OTHER_ID' field is non-empty and 'CMTE_ID' and 'TRANSACTION_AMOUNT' fields are empty, ignore the line
            if splited[15] != "" or splited[0] == "" or splited[14] == "" or (int(splited[14])) < 0:
                continue

# Filter the required fields and append the resulting data in the list in the form of dictionary
            donor_data.append({
                'cmteId': splited[0],
                'transactionAmount': splited[14],
                'transactionDate': splited[13],
                'zipCode': splited[10][0:5],  # Consider only first 5 digits in the Zip Code
                })
# Problem1: Find  Running Median by Zip and Receipt Code
            # Ignore data if zip field is empty or invalid
            if splited[10] != "" and len(splited[10]) >= 5:
                current_zip = splited[10][0:5]
                current_CMTE_ID = splited[0]
                current_amount=splited[14]
                # group data by cmte_id and zip_code
                group_data_by_CMTE_ID_and_zip_code[(current_CMTE_ID, current_zip)].append(int(current_amount))
                #Filter the data having same zip and cmte_id as the current ones
                filtered_data = [d[1] for d in group_data_by_CMTE_ID_and_zip_code.items() if
                                 d[0][1] == current_zip and d[0][0] == current_CMTE_ID][0]
                file1.write(str(current_CMTE_ID) + "|" + str(current_zip) + "|" + str(
                   int(find_median(filtered_data))) + "|" + str(
                   len(filtered_data)) + "|" + str(sum(filtered_data)) + "\n")
        file1.close()
    # Delete the list to free memory
    del filtered_data


#Problem2: Find  Median by Time
    group_data_by_CMTE_ID_and_date = defaultdict(list)
    for obj in donor_data:
        # Ignore data if date field is empty or invalid
        if (obj['transactionDate'] != "") and (validate_date(obj['transactionDate'])):
            group_data_by_CMTE_ID_and_date[(obj['cmteId'], convert_to_date_object(obj['transactionDate']))].append(int(obj['transactionAmount']))

    # sort the whole data by ReceiptID and then by Date
    group_data_by_CMTE_ID_and_date = sorted(group_data_by_CMTE_ID_and_date.items(), key=lambda x: (x[0][0], x[0][1]))
    for x in group_data_by_CMTE_ID_and_date:
        file2.write(str(x[0][0]) + "|" + str(convert_date_object_to_string(x[0][1])) + "|" + str(
            int(find_median(x[1]))) + "|" + str(len(x[1])) + "|" + str(sum(x[1])) + "\n")
    file2.close()


# Main: Entry point of Program
if __name__ == "__main__":
    start = time.time()
    try:
        input_file_name = sys.argv[1]  # Input File Name
        output_filename_for_median_value_by_zip = sys.argv[2]  # Output File Name for Median Value by zip
        output_filename_for_median_value_by_time = sys.argv[3]  # Output File Name for Median Value by Time
    except IndexError:
        print("Usage: ./src/find_political_donors.py  ./input/itcont.txt  ./output/medianvals_by_zip.txt / ./output/medianvals_by_date.txt")
        sys.exit(1)
    process_data(input_file_name, output_filename_for_median_value_by_zip, output_filename_for_median_value_by_time)
    end = time.time()
    print(end - start)
