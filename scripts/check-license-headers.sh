#!/bin/bash
# Copyright 2024 The Rucket Authors
# SPDX-License-Identifier: Apache-2.0

# Check that all source files have the required license header

set -e

EXIT_CODE=0

check_rust_files() {
    while IFS= read -r -d '' file; do
        if ! head -n 2 "$file" | grep -q "Copyright 2024 The Rucket Authors"; then
            echo "Missing license header: $file"
            EXIT_CODE=1
        fi
    done < <(find . -name "*.rs" -not -path "./target/*" -print0)
}

check_shell_files() {
    while IFS= read -r -d '' file; do
        # Skip first line if it's a shebang
        if head -n 1 "$file" | grep -q "^#!"; then
            if ! head -n 3 "$file" | tail -n 2 | grep -q "Copyright 2024 The Rucket Authors"; then
                echo "Missing license header: $file"
                EXIT_CODE=1
            fi
        else
            if ! head -n 2 "$file" | grep -q "Copyright 2024 The Rucket Authors"; then
                echo "Missing license header: $file"
                EXIT_CODE=1
            fi
        fi
    done < <(find . -name "*.sh" -not -path "./target/*" -print0)
}

check_toml_files() {
    while IFS= read -r -d '' file; do
        if ! head -n 2 "$file" | grep -q "Copyright 2024 The Rucket Authors"; then
            echo "Missing license header: $file"
            EXIT_CODE=1
        fi
    done < <(find . -name "*.toml" -not -path "./target/*" -not -name "Cargo.lock" -print0)
}

check_yaml_files() {
    while IFS= read -r -d '' file; do
        if ! head -n 2 "$file" | grep -q "Copyright 2024 The Rucket Authors"; then
            echo "Missing license header: $file"
            EXIT_CODE=1
        fi
    done < <(find . \( -name "*.yml" -o -name "*.yaml" \) -not -path "./target/*" -print0 2>/dev/null)
}

echo "Checking license headers..."

check_rust_files
check_shell_files
check_toml_files
check_yaml_files

if [ $EXIT_CODE -eq 0 ]; then
    echo "All files have correct license headers."
else
    echo ""
    echo "Please add the appropriate license header to the files listed above."
    echo ""
    echo "For Rust files:"
    echo "// Copyright 2024 The Rucket Authors"
    echo "// SPDX-License-Identifier: Apache-2.0"
    echo ""
    echo "For shell/TOML/YAML files:"
    echo "# Copyright 2024 The Rucket Authors"
    echo "# SPDX-License-Identifier: Apache-2.0"
fi

exit $EXIT_CODE
