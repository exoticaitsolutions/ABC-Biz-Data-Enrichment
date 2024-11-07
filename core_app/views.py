import pandas as pd
from django.shortcuts import render
from .models import LicenseNumber

def import_license_numbers(request):
    # Define the path to your CSV file
    csv_file_path = r"C:\Users\home\Videos\Files\regauravvatsandmichaelbrewer\regauravvatsandmichaelbrewer\License Numbers Sample - Input for Scraper - Input for Start #1.csv"

    # Read the CSV file using pandas
    df = pd.read_csv(csv_file_path)

    # Check if 'License Numbers' column exists in the CSV
    if 'License Numbers' in df.columns:
        # Loop through each license number in the column and save to the database
        for license_number in df['License Numbers']:
            # Convert to integer, if it's not already
            try:
                license_number = int(license_number)
                # Create a new LicenseNumber entry
                LicenseNumber.objects.create(license_number=license_number)
            except ValueError:
                # Skip any rows where the value can't be converted to an integer
                continue

    return render(request, 'import_success.html')
