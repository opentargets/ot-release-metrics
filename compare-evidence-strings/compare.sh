#!/bin/bash
set -euo pipefail

echo "Set up environment and parse parameters"
# To ensure that the sort results are consistent, set the sort order locale explicitly.
export LC_COLLATE=C
export LC_ALL=C
# realpath is required to make the paths work after the working directory change.
OLD_EVIDENCE_STRINGS=$(realpath "$1")
NEW_EVIDENCE_STRINGS=$(realpath "$2")
mkdir comparison && cd comparison || exit 1

echo "Sort the columns in the order of decreasing uniqueness"
python3 ../preprocess.py \
  --in-old "${OLD_EVIDENCE_STRINGS}" \
  --in-new "${NEW_EVIDENCE_STRINGS}" \
  --out-old 01.keys-sorted.old.json \
  --out-new 01.keys-sorted.new.json

echo "Sort and deduplicate the evidence string sets"
sort -u 01.keys-sorted.old.json > 02.sorted.old.json \
  & sort -u 01.keys-sorted.new.json > 02.sorted.new.json \
  & wait

echo "Separate evidence strings which are exactly the same between the sets"
comm 02.sorted.old.json 02.sorted.new.json > 03.comm
awk -F'\t' 'BEGIN {OFS = FS} {print $1}' 03.comm | grep -v '^$' > 04.filtered.old.json
awk -F'\t' 'BEGIN {OFS = FS} {print $2}' 03.comm | grep -v '^$' > 04.filtered.new.json
awk -F'\t' 'BEGIN {OFS = FS} {print $3}' 03.comm | grep -v '^$' > 04.filtered.common.json

echo "Write report header and summary statistics"
cat << EOF > report.html
<html>
<style type="text/css">
  code { white-space: pre; }
</style>
<code><b><big>Evidence string comparison report</big></b>

<b>File 1</b> - ${OLD_EVIDENCE_STRINGS}
Total unique evidence strings: <b>$(wc -l <02.sorted.old.json)</b>

<b>File 2</b> - ${NEW_EVIDENCE_STRINGS}
Total unique evidence strings: <b>$(wc -l <02.sorted.new.json)</b>

<b>Summary counts</b>
Evidence strings which appear in both files and are exactly the same: $(wc -l 04.filtered.common.json)
Evidence strings which only appear in file 1: $(wc -l 04.filtered.old.json)
Evidence strings which only appear in file 2: $(wc -l 04.filtered.new.json)

<b>Diff for the non-common evidence strings</b>
</code>
EOF

echo "Compute the diff"
git diff \
  --no-index \
  --text                  `# To force text (not binary) comparison mode`                       \
  -U0                     `# Do not output context (the evidence strings which are unchanged)` \
  --minimal \
  --color=always          `# Use colours even if the output is redirected to a file`           \
  --word-diff=color \
  --word-diff-regex=. \
  -- \
  04.filtered.old.json \
  04.filtered.new.json \
  > 05.diff

echo "Produce the report"
tail -n+5 05.diff \
  | awk '{if ($0 !~ /@@/) {print $0 "\n"}}') \
  | aha --word-wrap \
  >>report.html
gzip -9 report.html
cd ..
exit 0
