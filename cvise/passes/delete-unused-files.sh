#!/usr/bin/env bash
set -eu

# Deletes all files that are not used by the compilation of the target.
#
# Also removes them from the cppmap files.

MAKEFILE_PATH="target.makefile"

command="${1:-del}"
del_begin="${2:-}"
del_end="${3:-}"

tmp_path=$(mktemp -d)
trap 'rm -rf -- "$tmp_path"' EXIT

echo "analyzing dependencies..." >&2
root_compilation_command=$(grep -P -A1 '^[^\t].*\.o:' "$MAKEFILE_PATH" | tail -n1)
stripped_command=$(echo "$root_compilation_command" | \
    sed 's/\S*[-]fmodule\S*//g' | \
    sed 's/[-]o \S*//g' | \
    sed 's/ *@//g')
deps_file="$tmp_path/deps.d"
$stripped_command -E -MD -MF "$deps_file" -o /dev/null || true
used_files=$(cat "$deps_file" | xargs -n1 | grep -vE '^$|:$' | \
    xargs -n1 realpath --no-symlinks --relative-to=. | sort)

all_files=$(find . -type f -not -name '*.pcm' -not -name '*.o' \
    -not -name 'wrapped_clang' -not -name '*makefile' -not -name '*.sh' \
    -not -name '*.sh.input' -not -name '*.txt' -not -name '*.cppmap' | \
    xargs -n1 realpath --no-symlinks --relative-to=. | sort)
all_files_cnt=$(echo "$all_files" | wc -l)
files_to_delete=$(comm -23 <(echo "$all_files") <(echo "$used_files"))
if ! echo "$files_to_delete" | grep -v '^$'; then
    echo "nothing to delete." >&2
    exit 0
fi
if [ "$del_end" != "" ]; then
    files_to_delete=$(echo "$files_to_delete" | sed -n "$del_begin,$del_end p")
fi
files_to_delete_cnt=$(echo "$files_to_delete" | wc -l)
echo "to delete: $files_to_delete_cnt files (out of $all_files_cnt)..." >&2
files_for_stderr=$(echo "$files_to_delete" | paste -sd ',' -)
echo "files for deletion: $files_for_stderr" >&2
if [ "$command" = "dry" ]; then
    exit 0
fi

sed_pattern=$(echo "$files_to_delete" | tr '\n' '|' | sed 's/|$//')
sed_command_file="$tmp_path/sed.txt"
echo "\#header \"($sed_pattern)\"#d" >"$sed_command_file"

process_cppmap() {
    local sed_command_file="$1"
    local cppmap_file="$2"
    echo "editing $cppmap_file..." >&2
    sed -E -i -f "$sed_command_file" "$cppmap_file"
}
export -f process_cppmap

cppmap_files=$(find . -name '*.cppmap')
parallel --no-run-if-empty process_cppmap "$sed_command_file" <<< "$cppmap_files"

echo "deleting $files_to_delete_cnt files..." >&2
echo "$files_to_delete" | xargs --no-run-if-empty rm
