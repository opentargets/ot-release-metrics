#!/bin/bash
set -euo pipefail

echo "Set up environment and parse parameters"
# To ensure that the sort results are consistent, set the sort order locale explicitly.
export LC_COLLATE=C
export LC_ALL=C
# realpath is required to make the paths work after the working directory change.
OLD_EVIDENCE_STRINGS=$(realpath "$1")
NEW_EVIDENCE_STRINGS=$(realpath "$2")
OUTPUT_REPORT=$(realpath "$3")
TMP=$(mktemp -d)
export TMP
echo "Created temporary directory for work: ${TMP}"

echo "Sort the columns in the order of decreasing uniqueness"
python3 preprocess.py \
  --in-old "${OLD_EVIDENCE_STRINGS}" \
  --in-new "${NEW_EVIDENCE_STRINGS}" \
  --out-old ${TMP}/01.keys-sorted.old.json \
  --out-new ${TMP}/01.keys-sorted.new.json

echo "Sort and deduplicate the evidence string sets"
sort -u ${TMP}/01.keys-sorted.old.json > ${TMP}/02.sorted.old.json \
  & sort -u ${TMP}/01.keys-sorted.new.json > ${TMP}/02.sorted.new.json \
  & wait

echo "Separate evidence strings which are exactly the same between the sets"
comm ${TMP}/02.sorted.old.json ${TMP}/02.sorted.new.json > ${TMP}/03.comm
awk -F'\t' 'BEGIN {OFS = FS} {print $1}' ${TMP}/03.comm | { grep -v '^$' || true; } > ${TMP}/04.filtered.old.json
awk -F'\t' 'BEGIN {OFS = FS} {print $2}' ${TMP}/03.comm | { grep -v '^$' || true; } > ${TMP}/04.filtered.new.json
awk -F'\t' 'BEGIN {OFS = FS} {print $3}' ${TMP}/03.comm | { grep -v '^$' || true; } > ${TMP}/04.filtered.common.json

echo "Compute the diff"
{ git diff --no-index -U0 --text ${TMP}/02.sorted.old.json ${TMP}/02.sorted.new.json || true; } \
  | delta --light --max-line-length 0 --max-line-distance 0.2 \
  > ${TMP}/05.diff

echo "Write report header and summary statistics"
cat << EOF > ${TMP}/report.html
<html>
<style type="text/css">
  code { white-space: pre; }
</style>
<code><b><big>Evidence string comparison report</big></b>

<b>File 1</b> - $(echo ${OLD_EVIDENCE_STRINGS})
Total unique evidence strings: <b>$(wc -l <${TMP}/02.sorted.old.json)</b>

<b>File 2</b> - $(echo ${NEW_EVIDENCE_STRINGS})
Total unique evidence strings: <b>$(wc -l <${TMP}/02.sorted.new.json)</b>

<b>Summary counts</b>
Evidence strings which appear in both files and are exactly the same: $(wc -l <${TMP}/04.filtered.common.json)
Evidence strings which only appear in file 1: $(wc -l <${TMP}/04.filtered.old.json)
Evidence strings which only appear in file 2: $(wc -l <${TMP}/04.filtered.new.json)

<b>Diff for the non-common evidence strings</b>
</code>
EOF

echo "Produce the report"
tail -n+5 ${TMP}/05.diff \
  | awk '{if ($0 !~ /@@/) {print $0 "\n"}}' \
  | aha --word-wrap \
  >> ${TMP}/report.html
gzip -9 -c < ${TMP}/report.html > ${OUTPUT_REPORT}
rm -rf "${TMP}"

echo "All done"
