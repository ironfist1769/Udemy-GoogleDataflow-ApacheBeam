#####################################################################
# Author: Himanshu Raj Date: 08-09-2024                             #
# Beam_Local_Run_GCS_Saved: We will be running the script locally   #
# on the local engine but we have set up a connection to GCS bucket #
#  where we will saving the processed data.                         #
#####################################################################

import apache_beam as beam

class FilterGold(beam.DoFn):
    def process(self, record):
        if int(record[3]) > 5:
            return [record]

# Function to extract Gold medals with None checks
def extract_gold_medals(record):
    # Return the key-value pair with country and gold medals
    return (record[1], int(record[3]) if record[3] is not None else 0)

# Function to extract Total medals with None checks
def extract_total_medals(record):
    # Return the key-value pair with country and total medals
    return (record[1], int(record[6]) if record[6] is not None else 0)

# Start Beam pipeline
p3 = beam.Pipeline()

# Common source for both tasks
common_source = (
    p3
    | "Import Data" >> beam.io.ReadFromText(r"olympics2024.csv", skip_header_lines=1)
    | "Split by comma" >> beam.Map(lambda record: record.split(','))
    | "Filter Gold > 5" >> beam.ParDo(FilterGold())
)

# First task: Gold Medals
Gold_Medals = (
    common_source
    | "Extract Gold Medals" >> beam.Map(extract_gold_medals)
)

# Second task: Total Medals
Total_Medals = (
    common_source
    | "Extract Total Medals" >> beam.Map(extract_total_medals)
)

# Joining the two PCollections
Joined_Table = (
    {'Gold_Medals': Gold_Medals, 'Total_Medals': Total_Medals}
    | "CoGroup By Country" >> beam.CoGroupByKey()
    | "Flatten and Print Results" >> beam.Map(
        lambda record: (
            record[0],  # Country name
            record[1]['Gold_Medals'][0] if record[1]['Gold_Medals'] else 0,  # Handle empty lists
            record[1]['Total_Medals'][0] if record[1]['Total_Medals'] else 0  # Handle empty lists
        )
    )
    | "Save to GCS" >> beam.io.WriteToText(r"gs://watch-bucket/Medal_tally_output.csv")
)

p3.run()