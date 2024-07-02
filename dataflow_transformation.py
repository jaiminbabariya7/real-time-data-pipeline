import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows

class CleanAndEnrichData(beam.DoFn):
    def process(self, element):
        # Access each field from the element
        gender = element.get('gender', '')
        age = element.get('age', 0)
        avenues = element.get('avenues', [])
        mutual_funds = element.get('mutual_funds', [])
        debentures = element.get('debentures', [])
        government_bonds = element.get('government_bonds', [])
        fixed_deposits = element.get('fixed_deposits', [])
        ppf = element.get('ppf', [])
        gold = element.get('gold', [])
        stock_market = element.get('stock_market', [])
        factor = element.get('factor', '')
        objective = element.get('objective', '')
        purpose = element.get('purpose', '')
        duration = element.get('duration', '')
        invest_monitor = element.get('invest_monitor', '')
        expect = element.get('expect', '')
        avenue = element.get('avenue', '')
        savings_objectives = element.get('what are your savings objectives?', '')
        reason_equity = element.get('reason_equity', '')
        reason_mutual = element.get('reason_mutual', '')
        reason_bonds = element.get('reason_bonds', '')
        reason_fd = element.get('reason_fd', '')
        source = element.get('source', '')

        # Perform data cleaning and enrichment
        # Example: Convert age to integer
        try:
            age = int(age)
        except ValueError:
            age = 0  # Handle invalid age values

        # Example: Derive a new field based on existing data
        if age < 30:
            age_group = 'Young'
        elif age < 50:
            age_group = 'Middle-aged'
        else:
            age_group = 'Senior'

        # Create a new enriched data dictionary
        enriched_data = {
            'gender': gender,
            'age': age,
            'age_group': age_group,
            'investments': mutual_funds + debentures + government_bonds + fixed_deposits + ppf + gold + stock_market,
            'factor': factor,
            'objective': objective,
            'purpose': purpose,
            'duration': duration,
            'invest_monitor': invest_monitor,
            'savings_objectives': savings_objectives,
            'reason_equity': reason_equity,
            'reason_mutual': reason_mutual,
            'reason_bonds': reason_bonds,
            'reason_fd': reason_fd,
            'source': source
        }

        yield enriched_data

def run_pipeline():
    options = PipelineOptions(streaming=True)
    with beam.Pipeline(options=options) as pipeline:
        (pipeline
         | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(subscription='projects/finalproject10071998/subscriptions/finance_data-sub')
         | 'Decode' >> beam.Map(lambda x: x.decode('utf-8'))
         | 'Transform' >> beam.ParDo(CleanAndEnrichData())
         | 'Write to CSV' >> beam.io.WriteToText('gs://finance_data_project/transformed_data/data', file_name_suffix='.csv'))
             
if __name__ == '__main__':
    run_pipeline()
