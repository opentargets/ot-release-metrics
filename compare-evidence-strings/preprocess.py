import argparse
import sys

import pandas as pd


assert sys.version_info >= (3, 7), ('The script requires Python 3.7+ to work, because it depends on stable dictionary '
                                    'sort order.')


def recursively_normalise(list_dict_or_other):
    """Normalise the contents of list and dictionaries by recursively sorting them."""
    if type(list_dict_or_other) is list:
        return sorted(recursively_normalise(x) for x in list_dict_or_other)
    if type(list_dict_or_other) is dict:
        return {k: recursively_normalise(list_dict_or_other[k]) for k in sorted(list_dict_or_other.keys())}
    else:
        return list_dict_or_other


def normalise_and_serialise(list_dict_or_other):
    """Applies recursive normalisation and then serialisation for nested objects to ensure that they can be hashed."""
    return str(recursively_normalise(list_dict_or_other))


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
    df = df.applymap(normalise_and_serialise)

    if not column_order:
        # Calculate how many unique elements are there in each column.
        unique_counts = df.nunique()

        # Sort columns in the reverse order of that metric. This way, the columns which are the most diverse (and thus
        # more likely to serve as identifying fields for that evidence string) will appear first in the serialisation,
        # making the subsequent diff operation.
        column_order = sorted(unique_counts.index, key=lambda x: -unique_counts[x])

    # Save the preprocessed dataframe.
    df[column_order].to_json(outfile, orient='records', lines=True)
