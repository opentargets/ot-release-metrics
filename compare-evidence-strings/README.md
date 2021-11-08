# Protocol for comparing evidence strings

## Overview
Comparing two sets of evidence strings is an important measure of control in several situations:
* Running the same code on different inputs to see if the data changes make sense;
* Running different versions of code on the same input to see if code changes do not cause regressions;
* In case of Ensembl/VEP version changes: running the _same_ code on the _same_ inputs, but using different VEP releases, to see if it causes any breaking changes.

Since evidence strings are in JSON format, running a naive diff on them will be close to meaningless. This protocol attempts to provide an improvement on that by performing pre- and postprocessing and generating user-friendly output files and summary statistics. The functionality supported by the current version of the protocol:

* Preprocess to make the diffs less noisy
  - Ensure stable sort order
  - Sort keys lexicographically in each evidence string
  - Remove uninformative fields which change frequently and do not constitute a meaningful difference (currently it's "validated_against_schema_version" and "date_asserted")
* Classify the evidence string into categories
  - Using the association fields (currently it's association fields specific to our data), it splits the evidence strings into non-unique (in at least one comparison set) and unique.
  - For unique evidence strings, a one-to-one mapping between old and new sets is established, and they are separated into "deleted", "common", and "new".
* Calculate some derivative statistics
  - Summary for counts in each category (non-unique, deleted, common, new)
  - Frequency of transitions for values of a certain field (currently it's only functional consequences) as a separate table
* Produce the diffs in easy-to-read format
  - Separated by category
  - Using the word diff mode, so that the changes inside an evidence string are highlighted
  - Presented in HTML format which is much easier to read than the console output

Currently the values of fields used for preprocessing, comparison and computing summary metrics are hardcoded for the EVA use case, but if there's interest, in the future iterations of refactoring these values can be made customizable.

## Running the comparison

### Install dependencies
The protocol requires two tools to be present in PATH: [jq](https://stedolan.github.io/jq/) and [aha](https://github.com/theZiz/aha). They can be installed using the commands below:
```bash
# Install JQ — a command line JSON processor"
wget -q -O jq https://github.com/stedolan/jq/releases/download/jq-1.6/jq-linux64
chmod a+x jq

# Install aha — HTML report generator
wget -q https://github.com/theZiz/aha/archive/0.5.zip
unzip -q 0.5.zip && cd aha-0.5 && make &>/dev/null && mv aha ../ && cd .. && rm -rf aha-0.5 0.5.zip

export PATH=$PATH:`pwd`
```

Two sets of evidence strings can be compared by running the command:
```bash
bash compare.sh \
  old_evidence_strings.json \
  new_evidence_strings.json
```

The script will take a few minutes to run and will create a `comparison/` subdirectory in the current working directory. It will contain several intermediate files, and a single final file under the name of **`report.zip`**. (You can download an example of this final file [here](report-example/report.zip).)

## Understanding the results
Copy the `report.zip` to your local machine, unzip, and open `report.html` in any web browser. It contains an index page outlining the major statistics of differences between the evidence strings:

![](report-example/01.index.png)

### Association fields
First two sections describe statistics per input file. The evidence strings are divided into two groups: those where the association fields are unique (the majority), and those where they are not.

For the EVA/ClinVar use case, the association fields are:
1. ClinVar RCV record accession
1. Ontology term specifying which phenotype/trait is contained in the record
1. Allele origin (germline or somatic)
1. Variant ID (RS ID, if present, or the same RCV ID as in the first field)
1. Ensembl gene ID

### Diff for evidence strings with non-unique association fields
If a certain set of association fields occurs more than once for at least one of the input files, the evidence strings falls in the “non-unique” category. They cannot be easily paired between files 1 and 2, so for them only a bulk diff between the two files is produced, which is available through a diff link.

### Diffs and statistics for evidence strings with unique association fields
If a certain set of association fields occurs at most once per each of the files, its evidence strings will be in the “unique” category. For them, it is easy to pair old and new evidence strings together, and to carry out more detailed analysis.

Evidence strings which occur only in the first file are marked as “deleted”, and their list is available by clicking on the total number.

Similarly, evidence strings which occur only in the second file are marked as “added”, with the full list available over a link.

Evidence strings which are present in both files (judging by the association fields) are also counted and have two progressively more restrictive categories.

Evidence strings which have changed some of their fields (but not the association fields) between files 1 and 2 are part of the previous category. They are counted and the diff is available over a link, for example:

![](report-example/02.changed.png)

Evidence strings for which the **functional consequence** specifically has changed are part of the _previous_ category. By clicking on their total count, you will see the frequency of transitions between different functional consequence types (from file 1 to file 2) to see if there are any patterns:

![](report-example/03.consequences.png)

## Future improvements

### Support arbitrary association fields & fields to ignore
Currently, the lists of fields are tailored to ClinVar/EVA use case and will not work for arbitrary evidence string sets. However, this can be easily rewritten as parameters.

### Alternative library for producing diffs
The [diff2html-cli](https://github.com/rtfpessoa/diff2html-cli) is a more advanced library which can be used to replace `aha` in the future.

### json-diff
There is a [json-diff](https://pypi.org/project/json-diff/) module which allows detailed comparison of JSON objects. If this protocol is going to be updated in the future, this module might be helpful. It provides structured overview of differences; however, it has a few limitations:
 * It can only compare individual evidence strings (so they must be sorted and matched beforehand)
 * When a field's value is updated, `json-diff` only reports the new value of the field, but not the old one, for example:
```json
{
    "_update": {
        "evidence": {
            "_update": {
                "gene2variant": {
                    "_update": {
                        "functional_consequence": "http://purl.obolibrary.org/obo/SO_0001575"
                    }
                }
            }
        }
    }
}
```

Here, the change was from SO_0001589 to SO_0001575, but only the second value is reported.