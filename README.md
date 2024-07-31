# Marketo Aute Lead Merge

## Overview

This script is designed to manage leads in Marketo by performing bulk extract operations, finding duplicate leads, and merging them. Please test first and start with smaller batches to ensure accuracy.


## Author

- **Name**: Damon Tavangar
- **Email**: tavangar2017@gmail.com

## License

This project is licensed under the GPL License.

# Features:
- Merge Leads based on email and Custom Field (both has to match) or email nly depending on configuration
- Utilizing Marketo Bulk Extract to download the records into local system before merging
- Every action is logged including:
- - total execution time
  - total API calls made
  - Which lead IDs has been merged
  - Number of Leads downloaded per batch
  - Details on endpoint being called
  - Improved Error output
- Can run DEV and PROD environments with improved Configuration Management
   

## Prerequisite
Create a custom field in Marketo and update the yaml file with the field name, for more details see: https://experienceleague.adobe.com/en/docs/marketo/using/product-docs/administration/field-management/create-a-custom-field-in-marketo

## Version

- **Version**: 1.0.6
- **Status**: Production

## Requirements

- Python 3.6 or higher
- requests` library
- `PyYAML` library

## Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/dtavangar/Marketo-Auto-Merge.git
   cd marketo-Auto-Merge
  
2. Install the required Python packages:
    pip install requests pyyaml

3. Set Environment Variables:
Add the following lines to your shell profile file (e.g., .bashrc, .zshrc, or .profile):
```bash
export CLIENT_ID='your_client_id'
export CLIENT_SECRET='your_client_secret'
export ENV='dev'  # or 'prod' for production
export MUNCHKIN_ID='<YOUR MUNCHKIN ID>' 
```
5. Save the file and reload the shell profile:
```bash
source ~/.bashrc  # or source ~/.zshrc or source ~/.profile
```
6. Configure YAML Files:
Create and configure the settings.dev.yaml and settings.prod.yaml files according to your requirements.

7. Usage  
Run the script using Python:
```bash
python merge_leads.py
```
## Contact 
You can me at tavangar2017@gmail.com for questions or new ideas. 

