#!/bin/bash
set -euo pipefail

echo "Set up environment and parse parameters"
# To ensure that the sort results are consistent, set the sort order locale explicitly.
export LC_COLLATE=C
# The realpath is required to make the paths work after the working directory change.
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
  02.sorted.old.json \
  02.sorted.new.json \
  > 03.diff

echo "Produce the report"
tail -n+5 03.diff \
  | awk '{if ($0 !~ /@@/) {print $0 "\n"}}') \
  | aha --word-wrap \
  >report.html
cd ..
exit 0

# The remaining code still needs to be cleared up.
COLOR_RED='\033[0;31m'
COLOR_GREEN='\033[0;32m'
COLOR_RESET='\033[0m' # No Color
export COLOR_RED COLOR_GREEN COLOR_RESET
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

<b>Statistics for evidence strings changes</b>
Deleted: <b><a href="deleted.html">$(wc -l <08.deleted)</a></b>
Added: <b><a href="added.html">$(wc -l <08.added)</a></b>
Present in both files: <b>$(wc -l <08.common)</b>
  Of them, changed: <b><a href="changed.html">$(awk -F$'\t' '$2 != $3' 08.common | wc -l)</a></b>
    Of them, have a different consequence: <b><a href="consequences-transition-frequency.html">$(wc -l <12.consequences-transitions)</a></b>
</code></html>
EOF

(tail -n+5 09.non-unique-diff | awk '{if ($0 !~ /@@/) {print $0 "\n"}}') > 99.non-unique
(echo -e "${COLOR_RED}"; awk '{print $0 "\n"}' 08.deleted; echo -e "${COLOR_RESET}") > 99.deleted
(echo -e "${COLOR_GREEN}"; awk '{print $0 "\n"}' 08.added; echo -e "${COLOR_RESET}") > 99.added
(tail -n+5 09.unique-diff | awk '{if ($0 !~ /@@/) {print $0 "\n"}}') > 99.changed

parallel 'aha --word-wrap <99.{} > {}.html' ::: non-unique deleted added changed consequences-transition-frequency
rm -rf report.zip
zip report.zip ./*.html

echo "All done"
