import argparse

import pandas as pd


def normalise_list(list_or_other):
    """Converts all lists to tuples. Additionally, sorts the values inside each list."""
    if type(list_or_other) is list:
        return tuple(sorted(normalise_list(x) for x in list_or_other))
    else:
        return list_or_other


parser = argparse.ArgumentParser()
parser.add_argument('--in-old', required=True)
parser.add_argument('--in-new', required=True)
parser.add_argument('--out-old', required=True)
parser.add_argument('--out-new', required=True)
args = parser.parse_args()

column_order = None
for infile, outfile in ((args.in_new, args.out_new), (args.in_old, args.out_old)):
    # Ingest the JSON dataset.
    df = pd.read_json(infile, lines=True)

    # Sort all lists and convert to tuples (recursively, if necessary).
    df = df.applymap(normalise_list)

    if not column_order:
        # Calculate how many unique elements are there in each column.
        unique_counts = df.nunique()

        # Sort columns in the reverse order of that metric. This way, the columns which are the most diverse (and thus
        # more likely to serve as identifying fields for that evidence string) will appear first in the serialisation,
        # making the subsequent diff operation.
        column_order = sorted(unique_counts.index, key=lambda x: -unique_counts[x])

    # Save the preprocessed dataframe.
    df[column_order].to_json(outfile, orient='records', lines=True)
