import pandas, requests, sys, csv, time
from pprint import pprint

TOKEN = ".TOKEN GOES HERE."
FIELDS_TO_SAVE_FROM_SHIPMENT_RESPONSES = [
    "date_created",
    "logistic",
    "sibling",
    "external_reference",
    "id",
    "status",
    "substatus",
]
STRING_TO_LOOK_BEFORE_STATUS_CODE = '"status"'

def get_shipment(shipment_id):
    start_time = time.time()
    response = requests.get(
        f"https://internal-api.mercadolibre.com/shipments/{shipment_id}?caller.scopes=admin",
        headers={
            "x-Auth-Token": TOKEN,
            "x-Format-New": "true",
            "X-Caller-Scopes": "admin",
        },
    )

    print(f"\t\t--- {(time.time() - start_time)} seconds doing shipments get call for shipment id {shipment_id}---")

    return response


def write_output_file(output_file_name, rows, delimiter_to_use, headers_row):
    with open(output_file_name, "w", newline="") as file_to_write:
        writer = csv.writer(file_to_write, delimiter=delimiter_to_use, quotechar="|", quoting=csv.QUOTE_MINIMAL)
        writer.writerow(headers_row)
        for row in rows:
            writer.writerow(row)


OUTPUT_FILE_COLUMN_HEADERS = [
    "timestamp",
    "date_created",
    "status_in_resource_data",
    "source.site_id",
    "resource_id",
    "id_in_shipment_get",
    "logistic.type",
    "external_reference",
    "resource_data",
]


def print_general_info(data):

    column_values_to_print = []
    for i, data_row in data.iterrows():

        print("\n\nColumn values in input file:\n\n")
        pprint(str(vars(data_row)))

        response = get_shipment(data_row.resource_id)
        print(f"\n\nValues in shipment response:\n\n")
        pprint(str(vars(response)))

        response_dict = response.json()
        print(f"\n\nAll fields in shipment response: {response_dict.keys()}\n\n")

        for k in FIELDS_TO_SAVE_FROM_SHIPMENT_RESPONSES:
            print(f"{k} -> {response_dict[k]}")

        # Append data from the shipment get response

        column_values_to_print.append(response_dict["source"]["site_id"])
        column_values_to_print.append(response_dict["status"])
        column_values_to_print.append(response_dict["substatus"])
        column_values_to_print.append(response_dict["external_reference"])
        column_values_to_print.append(response_dict["logistic"]["type"])
        column_values_to_print.append(response_dict["date_created"])
        column_values_to_print.append(response_dict["id"])
        # Append values from cvs data (audit output file)
        column_values_to_print.append(data_row.resource_id)
        column_values_to_print.append(data_row.timestamp)
        column_values_to_print.append(data_row.resource_data)

        if i == 0:
            break

    print("\n\n--------------------------------\n\n")
    print(OUTPUT_FILE_COLUMN_HEADERS)
    print(column_values_to_print)
    print("\n\n--------------------------------\n\n")


def extract_status_from_resource_data(data_row):
    index = data_row.resource_data.rfind(STRING_TO_LOOK_BEFORE_STATUS_CODE)
    if index == -1:
        return "STATUS NOT FOUND", -1
    index_status_substring_start = index + len(STRING_TO_LOOK_BEFORE_STATUS_CODE) + 1
    status_string = data_row.resource_data[index_status_substring_start : index_status_substring_start + 3]
    return status_string, int(status_string)


def should_ignore_status(status_as_int):
    return status_as_int < 400 or status_as_int > 499


def get_array_rows_from_map(a_map):
    answer = []

    for key in a_map:
        print(f"{key}\t{a_map[key]}")
        row = []
        row.append(key)
        row.append(a_map[key])
        answer.append(row)

    return answer


def write_output_metadata_file(output_file_name, status_counter_map, logistic_type_map, logistic_status_map):
    rows_to_print = []

    print(f"\n\nstatus\tcount")
    rows_to_print.append(["status", "count"])
    rows_to_print += get_array_rows_from_map(status_counter_map)

    print(f"\n\nlogistic_type\tcount")
    rows_to_print.append(["logistic_type", "count"])
    rows_to_print += get_array_rows_from_map(logistic_type_map)

    print(f"\n\nlogistic_status_map\tcount")
    rows_to_print.append(["logistic_status_map", "count"])
    rows_to_print += get_array_rows_from_map(logistic_status_map)

    write_output_file("META_DATA" + output_file_name, rows_to_print, "\t", [output_file_name, "META_DATA"])


def main():
    main_start_time = time.time()
    argument_list = sys.argv

    if len(argument_list) < 3:
        print("Error: Wrong number of arguments.")
        exit()

    if len(argument_list) != 3:
        print("Error: More than one argument.")
        exit()

    if len(argument_list) > 3:
        print(f"Two much arguments, only taking the first two: {argument_list[0:2]}.")

    input_file_name = argument_list[1]
    output_file_name = argument_list[2]

    print(f"Running script {argument_list[0]} with input file {input_file_name} and output file {output_file_name}")

    data = pandas.read_csv(sys.argv[1])

    if len(data) == 0:
        print("Error: No data provided")
        exit()

    print_general_info(data)

    rows_to_print_in_file = []
    status_counter_map = {}
    logistic_type_map = {}
    logistic_type_status_map = {}

    for i, data_row in data.iterrows():
        print(f"\n\nProcessing row {i} : \n\t ", end="")
        response_status_in_input_file, int_response_status_in_input_file = extract_status_from_resource_data(data_row)
        status_counter_map[response_status_in_input_file] = (
            status_counter_map[response_status_in_input_file] + 1
            if response_status_in_input_file in status_counter_map
            else 1
        )
        if should_ignore_status(int_response_status_in_input_file):
            print(
                f"Ignoring row {i} from file {input_file_name} for shipment {data_row.resource_id}. Should ignore status: {int_response_status_in_input_file}.  resource_data: {data_row.resource_data}."
            )
            continue
        print(
            f"Status {response_status_in_input_file} found in row {i} for shipment {data_row.resource_id} resource_data: {data_row.resource_data}."
        )

        column_values_to_print = []
        response = get_shipment(data_row.resource_id)
        response_dict = response.json()

        logistic_type = response_dict["logistic"]["type"]
        logistic_type_map[logistic_type] = (
            logistic_type_map[logistic_type] + 1 if logistic_type in logistic_type_map else 1
        )

        key_logistic_status = (response_status_in_input_file, logistic_type)
        logistic_type_status_map[key_logistic_status] = (
            logistic_type_status_map[key_logistic_status] + 1 if key_logistic_status in logistic_type_status_map else 1
        )

        column_values_to_print.append(data_row.timestamp)
        column_values_to_print.append(response_dict["date_created"])
        column_values_to_print.append(response_status_in_input_file)
        column_values_to_print.append(response_dict["source"]["site_id"])
        column_values_to_print.append(data_row.resource_id)

        column_values_to_print.append(response_dict["id"])
        column_values_to_print.append(logistic_type)
        column_values_to_print.append(response_dict["external_reference"])
        column_values_to_print.append(data_row.resource_data)

        rows_to_print_in_file.append(column_values_to_print)

        print(
            f"\t\t--- Current execution time: {(time.time() - main_start_time)} seconds. {i+1} analyzed rows, {i+1-len(rows_to_print_in_file)} ignored rows, {len(rows_to_print_in_file)} rows to output file."
        )

    write_output_file(output_file_name, rows_to_print_in_file, ";", OUTPUT_FILE_COLUMN_HEADERS)

    write_output_metadata_file(output_file_name, status_counter_map, logistic_type_map, logistic_type_status_map)

    print(
        f"\t\t--- TOTAL execution time: {(time.time() - main_start_time)} seconds. {i+1} analyzed rows, {i+1-len(rows_to_print_in_file)} ignored rows, {len(rows_to_print_in_file)} rows to output file {output_file_name}."
    )


if __name__ == "__main__":
    main()
