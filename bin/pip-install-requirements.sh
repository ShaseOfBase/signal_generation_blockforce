#!/usr/bin/env bash

# This script installs the requirements for the project by replacing the polakowo/vectorbt.pro
# repository with the one accessible to the user.

# Make sure this script is run from the project root directory.

# The organizations to check for access to the vectorbt.pro repository.
ORGANIZATIONS=("polakowo" "bfcdev")

# Ensure requirements.txt file exists in the current directory
if [ ! -f requirements.txt ]; then
    echo "ERROR: requirements.txt not found"
    echo "This script must be run from the root of the project directory"
    exit 1
fi

# Ensure user's SSH key is set up correctly for access to GitHub
if ssh -T git@github.com >/dev/null 2>&1 -ne 1; then
    echo "ERROR: SSH access to GitHub failed."
    echo "Please ensure your SSH key has been added to GitHub"
    exit 1
fi

# Find the organization that gives the user access to the vectorbt.pro repository
for org in "${ORGANIZATIONS[@]}"; do
    if git ls-remote git@github.com:"${org}"/vectorbt.pro >/dev/null 2>&1 -eq 0; then
        ACCESSIBLE_ORG=${org}
        break
    fi
done

# Install the requirements from the accessible organization
if [ -z "${ACCESSIBLE_ORG}" ]; then
    echo "ERROR: No organizations found that provide access to the vectorbt.pro repository"
    exit 1
fi
echo "INFO: Installing vectorbt.pro from organization ${ACCESSIBLE_ORG}"
pip install -r <(sed "s|polakowo|${ACCESSIBLE_ORG}|g" requirements.txt)