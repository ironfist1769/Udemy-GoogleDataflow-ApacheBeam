#########################################################################
# Author: Himanshu Raj Date: 08-09-2024                                 #
# Here the file is being generated and loaded into GCS bucket.          #
# The files from GCS bucket is extracted transformed on local machine.  #
# And then the output is loaded back to GCS Bucket.                     #
#########################################################################

import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = {
    'project': 'practice-1-433516' ,
    'runner': 'DataflowRunner',
    'region': 'us-east1',
    'staging_location': 'gs://watch-bucket/temp',
    'temp_location': 'gs://watch-bucket/temp',
    'template_location': 'gs://watch-bucket/template/batch_job_df_gcs' 
    }
    
pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
# Start Beam pipeline
p3 = beam.Pipeline(options=pipeline_options)
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

# Common source for both tasks
common_source = (
    p3
    | "Import Data" >> beam.io.ReadFromText(r"gs://watch-bucket/Product1/olympics2024.csv", skip_header_lines=1)
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
    | "Save to GCS" >> beam.io.WriteToText(r"gs://watch-bucket/Product1/Output/Medal_tally_output.csv")
)

p3.run()